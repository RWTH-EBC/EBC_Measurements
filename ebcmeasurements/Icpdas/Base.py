"""
Base module for ICP DAS

Introduction of I/O modules: https://www.icpdas.com/root/product/solutions/remote_io/rs-485/i-8k_i-87k/i-8k_i-87k_introduction.html
"""
from abc import ABC, abstractmethod
import socket
import sys
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class EthernetIoBase(ABC):
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
    ):
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
                else:
                    decoded_response[key] = rsp[index_start: index_stop + 1]
            return decoded_response

        if not response:
            # No response due to time out
            logger.info(f"No data received due to time out or error")
            return None
        elif response.startswith(('!', '>', '<')):  # TODO: check the delimiters
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

    def _establish_socket_connection(self):
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

    def get_response_by_command(self, command: str, buffer_size: int = 1024):
        """Get response by writing a command"""
        # Send the command as request
        try:
            self.socket.sendall(command.encode('utf-8'))
        except TimeoutError as e:
            logger.error(e)
            return ''
        except UnicodeError as e:
            logger.error(e)
            return ''
        # Receiving data
        try:
            return self.socket.recv(buffer_size).decode('utf-8')
        except UnicodeError as e:
            logger.error(e)
            return ''


class EthernetIoModule(EthernetIoBase, ABC):
    """Base class for I/O module"""
    pass
