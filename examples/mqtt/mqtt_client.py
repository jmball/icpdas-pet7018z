"""MQTT client for PET-7018Z."""

import argparse
import collections
import logging
import pickle
import queue
import pathlib
import threading
import time
import uuid
import sys

import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import pyvisa

from mqtt_tools.queue_publisher import MQTTQueuePublisher

sys.path.insert(1, str(pathlib.Path.cwd().parent.parent.joinpath("src")))
import pet7018z.pet7018z as pet7018z

parser = argparse.ArgumentParser()
parser.add_argument(
    "-mqtthost",
    type=str,
    default="127.0.0.1",
    help="IP address or hostname for MQTT broker.",
)
parser.add_argument(
    "--debug",
    action="store_true",
    default=False,
    help="Log messages at debug log level.",
)

args = parser.parse_args()

# set up logger
logging.captureWarnings(True)
logger = logging.getLogger()
LOG_LEVEL = 10 if args.debug is True else 20
logger.setLevel(LOG_LEVEL)

ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)
ch.setFormatter(logging.Formatter("%(asctime)s|%(name)s|%(levelname)s|%(message)s"))
logger.addHandler(ch)

# give the client a unique name
client_id = f"daq-{uuid.uuid4().hex}"

# start queue publisher
mqttqp = MQTTQueuePublisher()
mqttqp.connect(args.mqtthost)
mqttqp.loop_start()

# hold start/stop condition in thread-safe container
start = collections.deque(maxlen=1)
start.append(False)

# container for config data sent over mqtt
config = {}

# create queue for worker tasks
q = queue.Queue()

# inti the instrument object
daq = pet7018z()


def worker():
    """Work on requests sent over MQTT."""
    while True:
        msg = q.get()

        payload = pickle.loads(msg.payload)

        # handle continuous start/stop
        if msg.topic == "daq/start":
            start.append(True)
            logger.info("Starting continuous mode...")
        elif msg.topic == "daq/stop":
            start.append(False)
            logger.info("Continuous mode stopped.")
        elif msg.topic == "measurement/log":
            if (
                payload["msg"] == "Run complete!"
                or payload["msg"].startswith("RUN ABORTED!")
            ) and start[0] is True:
                # make sure continuous mode stops
                start.append(False)

                # wait for measurement delay + 1s to ensure last measurement
                # finishes
                try:
                    time.sleep(config["daq"]["delay"] + 1)
                except KeyError:
                    time.sleep(1)
                logger.info(payload["msg"])
        elif msg.topic == "daq/single":
            if start[0] is False:
                single()
            else:
                log(
                    "Cannot run single measurement: DAQ running in continuous mode.", 30
                )
        elif msg.topic == "measurement/run":
            if start[0] is False:
                logger.info("Received run message")
                read_config(payload)
                setup()
            else:
                log("Cannot update config/setup: DAQ running in continuous mode.", 30)
        elif msg.topic == "measurement/status":
            if payload in ["Offline", "Ready"] and start[0] is True:
                # make sure continuous mode stops
                start.append(False)

                # wait for measurement delay + 1s to ensure last measurement
                # finishes
                try:
                    time.sleep(config["daq"]["delay"] + 1)
                except KeyError:
                    time.sleep(1)
                logger.info(payload["msg"])

        q.task_done()


def continuous():
    """Measure data in continuous mode.

    This function runs in its own thread.
    """
    while True:
        if start[0] is True:
            single()
            time.sleep(config["daq"]["delay"])
        else:
            time.sleep(1)


def single():
    """Perform single shot measurement."""
    data = [time.time()]
    for channel in config["daq"]["channels"].keys():
        data.extend([daq.measure(channel)])
    handle_data(data)


def handle_data(data):
    """Handle measurement data.

    Parameters
    ----------
    data : array-like
        Measurement data.
    """
    payload = {
        "data": data,
        "pixel": {},
        "sweep": "",
    }
    mqttqp.append_payload("data/raw/daq", pickle.dumps(payload))


def log(msg, level):
    """Publish info for logging.

    Parameters
    ----------
    msg : str
        Log message.
    level : int
        Log level used by logging module:

            * 50 : CRITICAL
            * 40 : ERROR
            * 30 : WARNING
            * 20 : INFO
            * 10 : DEBUG
            * 0 : NOTSET
    """
    payload = {"level": level, "msg": msg}
    mqttqp.append_payload("measurement/log", pickle.dumps(payload))


def read_config(payload):
    """Get config data from payload.

    Parameters
    ----------
    payload : dict
        Request dictionary for measurement server.
    """
    global config
    config = payload["config"]


def attempt_connect():
    """Attempt to connect to the DAQ and log the result."""
    daq.connect(
        config["daq"]["host"], config["daq"]["port"], config["daq"]["timeout"], True,
    )

    daq_id = daq.get_id()
    err = None
    conn_msg = f"Connected to device: '{daq_id}'!"
    logger.info(conn_msg)

    return err, conn_msg


def setup():
    """Set up the instrument for measurements."""
    try:
        # check if daq is already connected by querying its id
        daq_id = daq.get_id()
        err = None
        conn_msg = f"Connected to device: '{daq_id}'!"
        logger.info(conn_msg)
    except AttributeError:
        # the daq object hasn't previously connected to the device so try to connect
        try:
            err, conn_msg = attempt_connect()
        except Exception as e:
            err = e
            conn_msg = "DAQ connection failed!"
            logger.exception(conn_msg)
    except Exception as e:
        err = e
        conn_msg = "DAQ connection failed!"
        logger.exception(conn_msg)

        # the daq has previously been connected but now has an issue
        try:
            # try disconnecting daq object and connecting again
            daq.disconnect()
            err, conn_msg = attempt_connect()
        except Exception as e:
            err = e
            logger.exception("DAQ disconnection failed!")
            # the physical device may have been disconnected causing the disconnect
            # method to fail so try to overwrite the connection with a new one
            try:
                err, conn_msg = attempt_connect()
            except Exception as e:
                err = e
                conn_msg = "DAQ connection failed!"
                logger.exception(conn_msg)

    # report daq connection status
    if err is None:
        log(conn_msg, 20)
    else:
        log(conn_msg + " " + str(err), 40)
        mqttqp.append_payload(
            "daq/init", pickle.dumps({"uuid": client_id, "init_success": False})
        )
        return

    try:
        # disable all analog inputs
        for channel in range(10):
            daq.enable_ai(channel, False)

        # global settings
        daq.set_ai_noise_filter(config["daq"]["plf"])
        daq.enable_cjc(True)

        # setup and enable the analog inputs in use
        for channel, ai_range in config["daq"]["channels"].items():
            daq.enable_ai(channel, True)
            daq.set_ai_range(channel, ai_range)

            # trying to send messages too quickly sometimes causes errors
            time.sleep(0.1)

        mqttqp.append_payload(
            "daq/init", pickle.dumps({"uuid": client_id, "init_success": True})
        )
    except Exception as e:
        setup_msg = "DAQ setup failed!"
        logger.exception(setup_msg)
        log(setup_msg + " " + str(e), 40)
        mqttqp.append_payload(
            "daq/init", pickle.dumps({"uuid": client_id, "init_success": False})
        )


def on_message(mqttc, obj, msg):
    """Act on an MQTT msg."""
    q.put_nowait(msg)


# start thread for managing queue tasks
threading.Thread(target=worker, daemon=True).start()

# start thread for measurement tasks
threading.Thread(target=continuous, daemon=True).start()

# create mqtt client
mqttc = mqtt.Client(client_id)
mqttc.will_set(
    "daq/status", pickle.dumps({"uuid": client_id, "mqtt": "offline"}), 2, retain=True,
)
mqttc.on_message = on_message
mqttc.connect(args.mqtthost)
mqttc.subscribe("measurement/#", qos=2)
mqttc.subscribe("daq/#", qos=2)
publish.single(
    "daq/status",
    pickle.dumps({"uuid": client_id, "mqtt": "ready"}),
    qos=2,
    hostname=args.mqtthost,
)
logger.info(f"{client_id} MQTT client connected!")
mqttc.loop_forever()
