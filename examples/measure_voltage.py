"""Read the voltage."""

import pathlib
import sys

sys.path.insert(1, str(pathlib.Path.cwd().parent.joinpath("src")))
import pet7018z.pet7018z as pet7018z

ip_address = "192.168.255.1"
port = 502
timeout = 30

plf = 50
channel = 0
ai_range = 5

with pet7018z as daq:
    daq.connect(ip_address, port, timeout, True)

    print(f"Connected to '{daq.get_id()}'!\n")

    # setup the analog inputs
    daq.set_ai_noise_filter(plf)
    daq.enable_cjc(True)
    daq.enable_ai(channel, True)
    daq.set_ai_range(channel, ai_range)

    # perform a read
    print(f"Measured value: {daq.measure(channel)} {daq.ai_ranges[ai_range]['unit']}")
