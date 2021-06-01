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
        0: {"min": -15e-3, "max": 15e-3, "unit": "V"},
        1: {"min": -50e-3, "max": 50e-3, "unit": "V"},
        2: {"min": -100e-3, "max": 100e-3, "unit": "V"},
        3: {"min": -500e-3, "max": 500e-3, "unit": "V"},
        4: {"min": -1, "max": 1, "unit": "V"},
        5: {"min": -2.5, "max": 2.5, "unit": "V"},
        6: {"min": -20, "max": 20, "unit": "mA"},
        7: {"min": 4, "max": 20, "unit": "mA"},
        14: {"min": -210, "max": 760, "unit": "degC"},
        15: {"min": -270, "max": 1372, "unit": "degC"},
        16: {"min": -270, "max": 400, "unit": "degC"},
        17: {"min": -270, "max": 1000, "unit": "degC"},
        18: {"min": 0, "max": 1768, "unit": "degC"},
        19: {"min": 0, "max": 1768, "unit": "degC"},
        20: {"min": 0, "max": 1820, "unit": "degC"},
        21: {"min": -270, "max": 1300, "unit": "degC"},
        22: {"min": 0, "max": 2320, "unit": "degC"},
        23: {"min": -200, "max": 800, "unit": "degC"},
        24: {"min": -200, "max": 100, "unit": "degC"},
        25: {"min": -200, "max": 900, "unit": "degC"},
        26: {"min": 0, "max": 20, "unit": "mA"},
    }

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object.

        Make sure everything gets cleaned up properly.
        """
        self.disconnect()

    def connect(self, ip_address, port, timeout=30, reset=True):
        """Connect to the instrument.

        Parameters
        ----------
        ip_address : str
            Instrument IP address.
        port : int
            Instrument port.
        timeout : float
            Comms timeout in seconds.
        reset : bool, optional
            Reset the instrument to the built-in default configuration.
        """
        self.instr = pyModbusTCP.client.ModbusClient(ip_address, port, timeout=timeout)
        self.instr.open()

        if reset is True:
            self.reset()

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
        # convert to signed integer using two's complement
        # this rescales the value from -32768 to 32767 (16 bit)
        if (value & (1 << (16 - 1))) != 0:
            value -= 1 << 16

        # re-scale from 0 to 2^16 - 1
        value += 32768

        # get range setting params
        ai_range_setting = self.ai_ranges[self.get_ai_range(channel)]
        ai_range_min = ai_range_setting["min"]
        ai_range_max = ai_range_setting["max"]
        ai_range = ai_range_max - ai_range_min

        # normalise
        eng = (value * ai_range / (2 ** 16 - 1)) + ai_range_min

        return eng

    # TODO: finish this method
    def _eng_to_adc(self, eng):
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
        pass

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
        """Set an AI range.

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
