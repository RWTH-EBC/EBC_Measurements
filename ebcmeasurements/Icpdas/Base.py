"""
Base module for ICP DAS

Introduction of I/O modules: https://www.icpdas.com/root/product/solutions/remote_io/rs-485/i-8k_i-87k/i-8k_i-87k_introduction.html
"""
from abc import ABC
import re
import socket
import sys
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class EthernetIoBase(ABC):
    """Base class for I/O units and modules"""
    # Class attribute: baud rate settings
    _baud_rate_settings = {
        '03': 1200,
        '04': 2400,
        '05': 4800,
        '06': 9600,
        '07': 19200,
        '08': 38400,
        '09': 57600,
        '0A': 115200,
    }

    @staticmethod
    def _to_hex(value: int) -> str:
        """Converts an integer (dec) to hex with 2 digits"""
        if isinstance(value, int):
            return format(value, '02X')
        else:
            raise ValueError(f"Input value '{value}' must be an integer")

    @staticmethod
    def decode_response(
            response: str,
            parse: dict[str, tuple[int, int]],
    ) -> dict[str, str | float | int] | None:
        """
        Decode the response by parse
        :param response: Response in utf-8 format
        :param parse: Parse dict, with the format {'key1': (start_index, stop_index), ...}
        :return: Dict of decoded keys and values
        """
        def _decode_general(rsp: str, par: dict[str, tuple[int, int]]):
            """General response decoder"""
            decoded_response = {}
            for key, (index_start, index_stop) in par.items():
                if key == 'address_id':
                    decoded_response[key] = int(rsp[index_start: index_stop + 1], 16)
                elif key == 'baud_rate_code':
                    decoded_response['baud_rate'] = EthernetIoBase._baud_rate_settings.get(rsp[index_start: index_stop + 1])
                else:
                    decoded_response[key] = rsp[index_start: index_stop + 1]
            return decoded_response

        if not response:
            # No response due to time out
            logger.info(f"No data received due to time out or error")
            return None
        elif response.startswith(('!', '>')):
            # Valid response
            return _decode_general(rsp=response, par=parse)
        elif response.startswith('?'):
            # Invalid response
            address_id = int(response[1:3], 16)
            logger.info(f"Invalid response received by address-id '{address_id}': '{response}'")
            return None
        else:
            logger.warning(f"Unexpected response format received: '{response}'")
            return None


class EthernetIoUnit(EthernetIoBase, ABC):
    """Base class for I/O expansion unit"""
    def __init__(self, host: str, port: int, time_out: float):
        self.host = host
        self.port = port
        self.time_out = time_out
        self.socket = None
        # Establish socket connection
        self._establish_socket_connection()

    def _establish_socket_connection(self) -> None:
        """Establish socket connection to I/O"""
        logger.info(f"Establishing socket connection to {self.host}:{self.port} ...")
        if self.socket is None:
            try:
                self.socket = socket.create_connection(
                    address=(self.host, self.port),
                    timeout=self.time_out,
                )
                logger.info(f"Socket connection established: {self.socket}")
            except TimeoutError as e:
                logger.error(f"Socket connection error: {e}")
                sys.exit(1)
        else:
            logger.info(f"Socket connection already established: {self.socket}")

    def get_response_by_command(self, command: str, buffer_size: int = 1024) -> str:
        """Get response by writing a command"""
        # Send the command as request
        try:
            self.socket.sendall(command.encode('utf-8'))
        except (TimeoutError, UnicodeError) as e:
            logger.error(e)
            return ''
        # Receiving data
        try:
            return self.socket.recv(buffer_size).decode('utf-8')
        except (TimeoutError, UnicodeError) as e:
            logger.error(e)
            return ''


class EthernetIoModule(EthernetIoBase, ABC):
    """Base class for I/O module"""
    def __init__(self, io_unit: EthernetIoUnit):
        self.io_unit = io_unit
        self._type_code_settings = None  # Dict for type code settings

    def read_configuration_status(self, address_id: int) -> dict[str, str] | None:
        """$AA2: Read module configuration"""
        cmd = f"${self._to_hex(address_id)}2\r"
        rsp = self.io_unit.get_response_by_command(cmd)
        # Get decoded response
        dec_rsp = self.decode_response(
            response=rsp,
            parse={'address_id': (1, 2), 'type_code': (3, 4), 'baud_rate_code': (5, 6), 'format_code': (7, 8)}
        )
        # Process decoded response
        if dec_rsp is not None:
            dec_rsp['type'] = self._type_code_settings.get(dec_rsp.pop('type_code'))
            return dec_rsp
        else:
            return None

    def read_analog_input_all_channels(self, address_id: int) -> dict[str, str | float | int]:
        """#AA: Read analog/counter inputs of all channels"""
        cmd = f"#{self._to_hex(address_id)}\r"
        rsp = self.io_unit.get_response_by_command(cmd)
        return self.decode_response(
            response=rsp,
            parse={'data': (1, -2)},
        )

    @staticmethod
    def _split_data_string_to_values(data_string: str, none_value: str = None) -> dict[str, float | None]:
        """Split data string to dict of channel numbers and values by using regular expression"""
        # Split the string with lookahead assertion, drop the first empty element
        str_values = re.split(pattern='(?=[+-])', string=data_string)[1:]
        if none_value is None:
            # Convert all values to float
            values = [float(v) for v in str_values]
        else:
            # Convert all values to float except none_value
            values = [float(v) if v != none_value else None for v in str_values]
        # Return a dict with channel keys
        return {f'Ch{ch}': v for ch, v in enumerate(values)}
