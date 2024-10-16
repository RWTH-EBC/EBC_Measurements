"""
Module for I-87K Series

Online user manual: http://ftp.icpdas.com/pub/cd/8000cd/napdos/dcon/io_module/87k_modules.htm
"""
from ebcmeasurements.Icpdas import IoBase


class IoUnit(IoBase.EthernetIoUnit):
    # Class attribute: I/O unit specifications
    _specifications = {
        'ET-87P4': {'io_slot': 4},
        'ET-87P8': {'io_slot': 8},
    }

    def __init__(self, host: str, port: int, time_out: float = 0.5):
        super().__init__(host, port, time_out)

    def read_firmware_version(self, address_id: int) -> dict[str, int | str]:
        """$AAF: Read firmware version, valid for CPU and I/O module"""
        cmd = f"${self._to_hex(address_id)}F\r"
        rsp = self.get_response_by_command(cmd)
        return self.decode_response(
            response=rsp,
            parse={'address_id': (1, 2), 'firmware_version': (3, -2)}
        )

    def read_module_name(self, address_id: int) -> dict[str, int | str]:
        """$AAM: Read module name, valid for CPU and I/O module"""
        cmd = f"${self._to_hex(address_id)}M\r"
        rsp = self.get_response_by_command(cmd)
        return self.decode_response(
            response=rsp,
            parse={'address_id': (1, 2), 'module_name': (3, -2)}
        )

    @property
    def specifications(self) -> dict[str, dict[str, int | str]]:
        return self._specifications


class IoModule87013W(IoBase.EthernetIoModule):
    """4-Channel RTD Analog Input Module"""
    def __init__(self, io_unit: IoUnit, address_id: int):
        super().__init__(io_unit, address_id, slot_idx=address_id - 2)
        self._type_code_settings = {
            '20': 'Platinum 100, a = 0.00385, -100 to 100 degC (default)',
            '21': 'Platinum 100, a = 0.00385, 0 to 100 degC',
            '22': 'Platinum 100, a = 0.00385, 0 to 200 degC',
            '23': 'Platinum 100, a = 0.00385, 0 to 600 degC',
            '24': 'Platinum 100, a = 0.003916, -100 to 100 degC',
            '25': 'Platinum 100, a = 0.003916, 0 to 100 degC',
            '26': 'Platinum 100, a = 0.003916, 0 to 200 degC',
            '27': 'Platinum 100, a = 0.003916, 0 to 600 degC',
            '28': 'Nickel 120, -80 to 100 degC',
            '29': 'Nickel 120, 0 to 100 degC',
            '2A': 'Platinum 1000, a = 0.00385, -200 to 600 degC',
            '2E': 'Pt 100, a = 0.00385, -200 to +200 degC',
            '2F': 'Pt 100, a = 0.003916, -200 to +200 degC',
            '80': 'Pt 100, a = 0.00385, -200 to +600 degC',
            '81': 'Pt 100, a = 0.003916, -200 to +600 degC',
        }
        self.io_type = 'AI'
        self.io_channel = 4

    def read_analog_input_all_channels(self) -> dict[str, float | None]:
        dec_rsp = super().read_analog_input_all_channels()  # Get the decoded response
        return self._split_data_string_to_values(dec_rsp.pop('data'), none_value='-0000') if dec_rsp is not None else None


class IoModule87019RW(IoBase.EthernetIoModule):
    """8-channel Universal Analog Input Module with High Overvoltage Protection"""
    def __init__(self, io_unit: IoUnit, address_id: int):
        super().__init__(io_unit, address_id, slot_idx=address_id - 2)
        self._type_code_settings = {
            '00': '-15mV to +15mV',
            '01': '-50mV to +50mV',
            '02': '-100mV to +100mV',
            '03': '-500mV to +500mV',
            '04': '-1V to +1V',
            '05': '-2.5V to +2.5V',
            '06': '-20mA to +20mA with 125 ohms resistor',
            '08': '-10V to +10V',
            '09': '-5V to +5V',
            '0A': '-1V to +1V',
            '0B': '-500mV to +500mV',
            '0C': '-150mV to +150mV',
            '0D': '-20mA to +20mA with 125 ohms resistor',
            '0E': 'J Type',
            '0F': 'K Type',
            '10': 'T Type',
            '11': 'E Type',
            '12': 'R Type',
            '13': 'S Type',
            '14': 'B Type',
            '15': 'N Type',
            '16': 'C Type',
            '17': 'L Type',
            '18': 'M Type',
            '19': 'L Type DIN43710',
        }
        self.io_type = 'AI'
        self.io_channel = 8

    def read_analog_input_all_channels(self) -> dict[str, float | None]:
        dec_rsp = super().read_analog_input_all_channels()  # Get the decoded response
        return self._split_data_string_to_values(dec_rsp.pop('data')) if dec_rsp is not None else None


# I/O module map <name by request>: <class of I/O module>
IO_MODULE_MAP = {
    '87013': {'cls': IoModule87013W},
    '87019R': {'cls': IoModule87019RW},
}
