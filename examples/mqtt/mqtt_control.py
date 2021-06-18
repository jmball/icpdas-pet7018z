"""Controller to initiate mqtt_client."""

import pathlib
import pickle
import time

import paho.mqtt.publish as publish
import yaml


# get config
cwd = pathlib.Path.cwd()
config_path = cwd.joinpath("mqtt_config.yaml")
with open(config_path, "r") as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

# args for saver
args = {"run_name": "test", "run_name_suffix": int(time.time())}

# mqtt payload
payload = {"args": args, "config": config}

# send run message to update config and measurement args
publish.single(
    "measurement/run", pickle.dumps(payload), qos=2, hostname="127.0.0.1",
)

# start daq continuous mode
publish.single(
    "daq/start", pickle.dumps(""), qos=2, hostname="127.0.0.1",
)

# wait for a few measurements
time.sleep(10)

# stop daq continuous mode
publish.single(
    "daq/stop", pickle.dumps(""), qos=2, hostname="127.0.0.1",
)

# send run complete message
publish.single(
    "measurement/log",
    pickle.dumps({"msg": "Run complete!"}),
    qos=2,
    hostname="127.0.0.1",
)
