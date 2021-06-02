"""Update cold junction compensation offset for an AI channel."""

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
    default=15,
    choices=[14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
    help=(
        "AI range setting:"
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
    ),
)
parser.add_argument(
    "--channel", type=int, default=0, choices=list(range(10)), help="AI channel",
)
args = parser.parse_args()

with pet7018z as daq:
    daq.connect(args.ip_address, args.port, args.timeout, True)

    print(f"Connected to '{daq.get_id()}'!\n")

    # setup the analog inputs
    daq.set_ai_noise_filter(args.plf)
    daq.enable_ai(args.channel, True)
    daq.set_ai_range(args.channel, args.range)
    daq.enable_cjc(True)

    unit = daq.ai_ranges[args.range]["unit"]

    input(
        f"Connect a known temperature source to channel {args.channel}. Press Enter "
        + "when ready...\n"
    )

    while True:
        print(f"Measured temperature: {daq.measure(args.channel)} {unit}")
        print(f"Offset value: {daq.get_cjc_offset(args.channel)}\n")

        change = input("Do you want to change the offset value [y/n]?\n")

        if change == "y":
            offset = input(
                "Enter a new offset value in the range -9999 to 9999 then press Enter "
                + "when ready...\n"
            )
            if (offset >= -9999) and (offset <= 9999):
                daq.set_cjc_offset(args.channel, offset)
            else:
                print(
                    f"Invalid CJC offset: {offset}. CJC offset must be in the range "
                    + "-9999 to 9999.\n"
                )
        else:
            break

    print("Cold junction compensation offset update complete!")
