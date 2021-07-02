"""ICP DAS PET-7018Z/ET-7018Z analog input DAQ control library.

The full instrument manual, including the programming guide, can be found at
https://www.icpdas.com/en/download/show.php?num=2217&model=PET-7018Z/S.
"""

import pyModbusTCP.client


class pet7018z:
    """ICP DAS PET-7018Z/ET-7018Z analog input DAQ instrument.

    Communication is via Modbus.
    """

    ai_ranges = {
        0: {
            "min": -15e-3,
            "max": 15e-3,
            "unit": "V",
            "hex_max": 0x7FFF,
            "hex_min": 0x8000,
        },
        1: {
            "min": -50e-3,
            "max": 50e-3,
            "unit": "V",
            "hex_max": 0x7FFF,
            "hex_min": 0x8000,
        },
        2: {
            "min": -100e-3,
            "max": 100e-3,
            "unit": "V",
            "hex_max": 0x7FFF,
            "hex_min": 0x8000,
        },
        3: {
            "min": -500e-3,
            "max": 500e-3,
            "unit": "V",
            "hex_max": 0x7FFF,
            "hex_min": 0x8000,
        },
        4: {"min": -1, "max": 1, "unit": "V", "hex_max": 0x7FFF, "hex_min": 0x8000},
        5: {"min": -2.5, "max": 2.5, "unit": "V", "hex_max": 0x7FFF, "hex_min": 0x8000},
        6: {
            "min": -20e-3,
            "max": 20e-3,
            "unit": "A",
            "hex_max": 0x7FFF,
            "hex_min": 0x8000,
        },
        7: {
            "min": 4e-3,
            "max": 20e-3,
            "unit": "A",
            "hex_max": 0xFFFF,
            "hex_min": 0x0000,
        },
        14: {
            "min": -210,
            "max": 760,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0xDCA2,
        },
        15: {
            "min": -270,
            "max": 1372,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0xE6D0,
        },
        16: {
            "min": -270,
            "max": 400,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0xA99A,
        },
        17: {
            "min": -270,
            "max": 1000,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0xDD71,
        },
        18: {
            "min": 0,
            "max": 1768,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0x0000,
        },
        19: {
            "min": 0,
            "max": 1768,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0x0000,
        },
        20: {
            "min": 0,
            "max": 1820,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0x0000,
        },
        21: {
            "min": -270,
            "max": 1300,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0xE56B,
        },
        22: {
            "min": 0,
            "max": 2320,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0x0000,
        },
        23: {
            "min": -200,
            "max": 800,
            "unit": "degC",
            "hex_max": 0x7FFF,
            "hex_min": 0xE000,
        },
        24: {
            "min": -200,
            "max": 100,
            "unit": "degC",
            "hex_max": 0x4000,
            "hex_min": 0x8000,
        },
        25: {
            "min": -200,
            "max": 900,
            "unit": "degC",
            "hex_max": 0xFFFF,
            "hex_min": 0xE38E,
        },
        26: {"min": 0, "max": 20e-3, "unit": "A", "hex_min": 0xFFFF, "hex_max": 0x0000},
    }

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object.

        Make sure everything gets cleaned up properly.
        """
        self.disconnect()

    def __init__(self):
        """Construct object."""
        self.instr = None

    def connect(self, host, port=502, timeout=30, reset=True):
        """Connect to the instrument.

        Parameters
        ----------
        host : str
            Instrument host.
        port : int
            Instrument port. Default for Modbus is 502.
        timeout : float
            Comms timeout in seconds.
        reset : bool, optional
            Reset the instrument to the built-in default configuration.
        """
        self.instr = pyModbusTCP.client.ModbusClient(host, port, timeout=timeout)
        self.instr.open()

        if reset is True:
            self.reset()

        # measure method assumes hex format
        self.set_ai_data_format("hex")

    def disconnect(self):
        """Disconnect the instrument."""
        self.instr.close()

    def get_id(self):
        """Get instrument identity string.

        Returns
        -------
        id : str
            Identification string formatted as: '[manufacturer], [model], [os version],
            [firmware version], [I/O version]'.
        """
        model = hex(self.instr.read_holding_registers(559, 1)[0])[2:]

        os_version = hex(self.instr.read_input_registers(350, 1)[0])[2:]
        os_version_fmt = ""
        for i, n in enumerate(os_version):
            os_version_fmt += f"{n}"
            if i != len(os_version) - 1:
                os_version_fmt += "."

        fw_version = hex(self.instr.read_input_registers(351, 1)[0])[2:]
        fw_version_fmt = ""
        for i, n in enumerate(fw_version):
            fw_version_fmt += f"{i}"
            if i != len(fw_version) - 1:
                fw_version_fmt += "."

        io_version = hex(self.instr.read_input_registers(353, 1)[0])[2:]
        io_version_fmt = ""
        for i, n in enumerate(io_version):
            io_version_fmt += f"{i}"
            if i != len(io_version) - 1:
                io_version_fmt += "."

        id_str = (
            f"ICP DAS, {model}, {os_version_fmt}, {fw_version_fmt}, {io_version_fmt}"
        )

        return id_str

    def reset(self):
        """Reset the instrument to the factory default configuration.

        This method only affects I/O settings, preserving calibration settings.
        """
        self.instr.write_single_coil(226, True)

    def _adc_to_eng(self, channel, value):
        """Normalise a returned ADC value.

        pyModbusTCP returns the two's complement of the internal ADC value when
        queried, irrespective of whether the instrument is in hex or engineering mode.
        This method converts the returned value to engineering units.

        Paramters
        ---------
        channel : int
            Channel to set, 0-indexed.
        value : int
            Returned integer to normalise.

        Returns
        -------
        eng : float
            Value in engineering units.
        """
        value = self._twos_complement(value)

        # get range setting params
        ai_range_setting = self.ai_ranges[self.get_ai_range(channel)]
        ai_range_min = ai_range_setting["min"]
        ai_range_max = ai_range_setting["max"]
        ai_range = ai_range_max - ai_range_min

        hex_range_min = self._twos_complement(ai_range_setting["hex_min"])
        hex_range_max = self._twos_complement(ai_range_setting["hex_max"])
        hex_range = hex_range_max - hex_range_min

        # eng units per adc count
        units_per_count = ai_range / hex_range

        eng = value * units_per_count

        return eng

    def _twos_complement(self, value):
        """Calculate decimal value from signed 2's complement.

        Parameters
        ----------
        value : int
            Decimal value from instrument.

        Returns
        -------
        value : int
            Signed decimal value derived from hex 2's complement.
        """
        # rescale the value from -32768 to 32767 (16 bit)
        if (value & (1 << (16 - 1))) != 0:
            value -= 1 << 16

        return value

    def _eng_to_adc(self, channel, eng):
        """Convert a number to an ADC value expected by the instrument.

        Paramters
        ---------
        eng : float
            Value in engineering units.

        Returns
        -------
        value : int
            ADC value.
        """
        # get range setting params
        ai_range_setting = self.ai_ranges[self.get_ai_range(channel)]
        ai_range_min = ai_range_setting["min"]
        ai_range_max = ai_range_setting["max"]
        ai_range = ai_range_max - ai_range_min

        hex_range_min = self._twos_complement(ai_range_setting["hex_min"])
        hex_range_max = self._twos_complement(ai_range_setting["hex_max"])
        hex_range = hex_range_max - hex_range_min

        # eng units per adc count
        units_per_count = ai_range / hex_range

        value = eng / units_per_count

        # re-scale from 0 to 65535 using two's complement
        if value < 0:
            value += 1 << 16

        return value

    def set_ai_range(self, channel, ai_range):
        """Set an AI range.

        Paramters
        ---------
        channel : int
            Channel to set, 0-indexed.
        ai_range : int
            Range setting integer:
                0: +/- 15 mv
                1: +/- 50 mV
                2: +/- 100 mV
                3: +/- 500 mV
                4: +/- 1 V
                5: +/- 2.5 V
                6: +/- 20 mA
                7: 4-20 mA
                14: Type J
                15: Type K
                16: Type T
                17: Type E
                18: Type R
                19: Type S
                20: Type B
                21: Type N
                22: Type C
                23: Type L
                24: Type M
                25: Type L DIN43710
                26: 0-20 mA
        """
        self.instr.write_single_register(427 + channel, ai_range)

    def get_ai_range(self, channel):
        """Get an AI range.

        Paramters
        ---------
        channel : int
            Channel to set, 0-indexed.

        Returns
        -------
        ai_range : int
            Range setting integer:
                0: +/- 15 mv
                1: +/- 50 mV
                2: +/- 100 mV
                3: +/- 500 mV
                4: +/- 1 V
                5: +/- 2.5 V
                6: +/- 20 mA
                7: 4-20 mA
                14: Type J
                15: Type K
                16: Type T
                17: Type E
                18: Type R
                19: Type S
                20: Type B
                21: Type N
                22: Type C
                23: Type L
                24: Type M
                25: Type L DIN43710
                26: 0-20 mA
        """
        return self.instr.read_holding_registers(427 + channel, 1)[0]

    def measure(self, channel):
        """Get measurement value for a channel.

        Parameters
        ----------
        channel : int
            Channel to set, 0-indexed.

        Returns
        -------
        eng : float
            Value in engineering units.
        """
        value = self.instr.read_input_registers(channel, 1)[0]

        return self._adc_to_eng(channel, value)

    def enable_cjc(self, enable):
        """Enable or disable cold junction compensation.

        Parameters
        ----------
        enable : bool
            Enable (`True`) or disable (`False`) cold junction compensation.
        """
        self.instr.write_single_coil(627, enable)

    def set_cjc_offset(self, channel, offset):
        """Set the cold junction compensation offset for a channel.

        Parameters
        ----------
        channel : int
            Channel to set, 0-indexed.
        offset : float
            Cold junction compensation offset in ADC counts (-9999 to 9999).
        """
        if (offset > 9999) or (offset < -9999):
            raise ValueError(f"Invalid offset: {offset}. Must be >= -9999 and =< 9999.")

        # re-scale from 0 to 65535 using two's complement
        if offset < 0:
            offset += 1 << 16

        self.instr.write_single_register(491 + channel, offset)

    def get_cjc_offset(self, channel):
        """Get the cold junction compensation offset for a channel.

        Parameters
        ----------
        channel : int
            Channel to set, 0-indexed.

        Returns
        -------
        offset : float
            Cold junction compensation offset in ADC counts (-9999 to 9999).
        """
        offset = self.instr.read_holding_registers(491 + channel, 1)[0]

        # re-scale from -9999 to 9999
        offset = self._twos_complement(offset)

        return offset

    def enable_ai(self, channel, enable):
        """Enable or disable an analog input.

        Parameters
        ----------
        channel : int
            Channel to set, 0-indexed.
        enable : bool
            Enable (`True`) or disable (`False`) cold junction compensation.
        """
        self.instr.write_single_coil(595 + channel, enable)

    def set_ai_noise_filter(self, plf):
        """Set analog input noise filter frequency.

        Parameters
        ----------
        plf : int, {50, 60}
            Power line frequency in Hz. Must be 50 or 60.
        """
        if plf == 50:
            cmd = True
        elif plf == 60:
            cmd = False
        else:
            raise ValueError(f"Invalid power line frequency: {plf}. Must be 50 or 60.")

        self.instr.write_single_coil(629, cmd)

    def set_ai_data_format(self, data_format):
        """Set analog input data format.

        Paramters
        ---------
        data_format : str
            Hexadecimal ("hex") or engineering unit ("eng") format.
        """
        if data_format == "hex":
            cmd = False
        elif data_format == "eng":
            cmd = True
        else:
            raise ValueError(
                f"Invalid AI data format: {data_format}. Must be 'hex' or 'eng'."
            )

        self.instr.write_single_coil(631, cmd)

    def enable_calibration(self, enable):
        """Enable/disable AI calibration mode.

        Parameters
        ----------
        enable : bool
            Enable (`True`) or disable (`False`) AI calibration mode.
        """
        self.instr.write_single_coil(830, enable)

    def zero_calibration(self):
        """Record the 0 V or 0 mA calibration value."""
        self.instr.write_single_coil(831, True)

    def span_calibration(self):
        """Record the span (positive full range) calibration value."""
        self.instr.write_single_coil(831, True)
