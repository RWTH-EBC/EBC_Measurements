"""
Module for I-87K Series

Online user manual: http://ftp.icpdas.com/pub/cd/8000cd/napdos/dcon/io_module/87k_modules.htm
"""
from ebcmeasurements.Icpdas import Base


class IoUnit(Base.EthernetIoUnit):
    def __init__(self, host: str, port: int, time_out: float = 0.5):
        super().__init__(host, port, time_out)

    def read_firmware_version(self, address_id: int):
        """$AAF: Read firmware version, valid for CPU and I/O module"""
        cmd = f"${self._to_hex(address_id)}F\r"
        rsp = self.get_response_by_command(cmd)
        return self.decode_response(
            response=rsp,
            parse={'address_id': (1, 2), 'firmware_version': (3, -2)}
        )

    def read_module_name(self, address_id: int):
        """$AAM: Read module name, valid for CPU and I/O module"""
        cmd = f"${self._to_hex(address_id)}M\r"
        rsp = self.get_response_by_command(cmd)
        return self.decode_response(
            response=rsp,
            parse={'address_id': (1, 2), 'module_name': (3, -2)}
        )


class Module_87019R(Base.EthernetIoModule):
    pass


if __name__ == '__main__':
    test_unit = IoUnit('134.130.40.39', 9999)
    for _id in range(1, 10):
        print(test_unit.read_module_name(_id))
