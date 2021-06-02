"""Calibrate an AI channel."""

import argparse
import pathlib
import sys

sys.path.insert(1, str(pathlib.Path.cwd().parent.joinpath("src")))
import pet7018z.pet7018z as pet7018z


parser = argparse.ArgumentParser()
parser.add_argument(
    "--plf", type=float, default=50, help="Power line frequency, e.g. 50 Hz.",
)
parser.add_argument(
    "--ip_address",
    type=str,
    default="192.168.255.1",
    help="Instrument IP address, e.g. 192.168.255.1",
)
parser.add_argument(
    "--port", type=int, default=502, help="Instrument port, e.g. 502",
)
parser.add_argument(
    "--timeout", type=int, default=30, help="Communications timeout in seconds",
)
parser.add_argument(
    "--range",
    type=int,
    default=5,
    choices=[
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
    ],
    help=(
        "AI range setting:"
        + "\n\t0 = +/- 15 mv"
        + "\n\t1 = +/- 50 mV"
        + "\n\t2 = +/- 100 mV"
        + "\n\t3 = +/- 500 mV"
        + "\n\t4 = +/- 1 V"
        + "\n\t5 = +/- 2.5 V"
        + "\n\t6 = +/- 20 mA"
        + "\n\t7 = 4-20 mA"
        + "\n\t14 = Type J"
        + "\n\t15 = Type K"
        + "\n\t16 = Type T"
        + "\n\t17 = Type E"
        + "\n\t18 = Type R"
        + "\n\t19 = Type S"
        + "\n\t20 = Type B"
        + "\n\t21 = Type N"
        + "\n\t22 = Type C"
        + "\n\t23 = Type L"
        + "\n\t24 = Type M"
        + "\n\t25 = Type L DIN43710"
        + "\n\t26 = 0-20 mA"
    ),
)
args = parser.parse_args()

with pet7018z as daq:
    daq.connect(args.ip_address, args.port, args.timeout, True)

    print(f"Connected to '{daq.get_id()}'!\n")

    # setup the analog inputs
    daq.set_ai_noise_filter(args.plf)

    # enable only channel 0 for calibration, disable others
    for ch in range(10):
        if ch == 0:
            daq.enable_ai(ch, True)
        else:
            daq.enable_ai(ch, False)

    # determine appropriate range if calibrating thermalcouple setting
    if args.range in [20, 24]:
        ai_range = 0
    elif args.range in [14, 16, 18, 19, 21, 22, 25]:
        ai_range = 1
    elif args.range in [15, 17, 23]:
        ai_range = 2
    else:
        ai_range = args.range
    daq.set_ai_range(args.channel, ai_range)

    range_max = daq.ai_ranges[ai_range]["max"]
    unit = daq.ai_ranges[ai_range]["unit"]

    input(f"Apply 0 {unit} to channel 0. Press Enter when ready...\n")

    # start calibration mode
    daq.enable_calibration(True)

    # perform zero calibration read
    daq.zero_calibration()

    input(f"Apply +{range_max} {unit} to channel 0. Press Enter when ready...\n")

    # perform span calibration read
    daq.span_calibration()

    # end calibration mode
    daq.enable_calibration(False)

    print("Calibration complete!")
