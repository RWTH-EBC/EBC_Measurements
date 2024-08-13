"""
Module SensoSys: SensoHygBar, SensoTherm, SensoAnemo

Communication format
------
Baud Rate:  1200 to 230400 bps (default 115200)
Parity:     none
Data bits:  8
Stop bit:   1

Command Format
| Leading character | Address | Command | [CHKSUM] | CR |

Response Format
| Leading character | Address | Data | [CHKSUM] | CR |

%, #, $, ~  Leading characters of command
!           Leading characters for a valid response
>           Leading character for first reading data bufor response
<           Leading character for re-reading data bufor response
)           Leading character for first reading data bufor response and low voltage detection
(           Leading character for re-reading data bufor response and low voltage detection
?           Leading character for an invalid response
AA          Address ID of the device to be configured in hexadecimal format (00 to FF)
CHKSUM      A 2-character checksum in the ASCII code as a sum of all the characters in the command/response string
            except for the carriage return character (CR)
CR          End of command character, carriage return (0x0D)
"""

import serial  # Pyserial
import serial.tools.list_ports
import sys
import subprocess
import time
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class SensoSys:
    # Class attribute: baud rate settings (CC)
    _baud_rate_settings = {  # 'code (hex)': baud rate
        '04': 2400,
        '05': 4800,
        '06': 9600,
        '07': 19200,
        '08': 38400,
        '09': 57600,
        '0A': 115200,
        '0B': 230400
    }

    # Class attribute: SensoHygbar 'sensor_config'
    _senso_hygbar_sensor_config = {
        'Only BAR': {
            'TT': '01',
            'data_start_stop_index': (1, 7),
            'params': ['p'],
        },
        'Only HIG': {
            'TT': '02',
            'data_start_stop_index': (1, 5),
            'params': ['rh'],
        },
        'BAR + HIG': {
            'TT': '03',
            'data_start_stop_index': (1, 12),
            'params': ['rh', 'p'],
        },
    }

    def __init__(self, port: str = 'COM1', time_out: float = 0.1):
        """
        Initialize the SensoSys instance
        :param port: COM-port for the device
        :param time_out: If no data is received within time out, the read operation will return an empty byte string
        """
        logger.info(f"Initializing SensoSys ...")
        self.port = port
        self.time_out = time_out
        self.ser = None
        # Establish serial connection
        self._establish_serial_connection()

    def __del__(self):
        """Destructor to ensure the serial connection is closed"""
        self.close_serial_connection()  # Close the serial connection

    def _establish_serial_connection(self):
        """Establish the serial connection with specified settings for the system"""
        logger.info("Establishing serial connection ...")
        if self.ser is None or not self.ser.is_open:
            try:
                self.ser = serial.Serial(
                    port=self.port,
                    baudrate=115200,  # Default baud rate
                    bytesize=serial.EIGHTBITS,
                    parity=serial.PARITY_NONE,
                    stopbits=serial.STOPBITS_ONE,
                    timeout=self.time_out
                )
                logger.info(f"Established serial connection: {self.ser}")
            except serial.SerialException as e:
                logger.error(f"Serial connection error: {e}")
                sys.exit(1)
        else:
            logger.info(f"Serial connection already established: {self.ser}")

    def close_serial_connection(self):
        """Close the serial connection"""
        if self.ser and self.ser.is_open:
            logger.info(f"Closing serial connection to {self.port} ...")
            self.ser.close()
        else:
            logger.info(f"Serial connection to {self.port} already closed")

    def _get_response_by_hex_command(self, hex_command: str) -> str:
        """
        Get response from COM-port by writing a command (hex)
        :param hex_command: Command string (hex)
        :return: Decoded response in utf-8
        """
        # Send the request
        try:
            self.ser.write(hex_command.encode('utf-8'))  # Encode the command to bytes
        except serial.SerialTimeoutException as e:
            logger.error(e)
            return ''  # No response
        except UnicodeError as e:
            logger.error(e)
            return ''  # No response
        # Read a line from the serial port, decode, remove any leading and trailing whitespace
        try:
            return self.ser.readline().decode('utf-8').strip()
        except UnicodeError as e:
            logger.error(e)
            return ''  # No response

    def set_configuration(
            self,
            address_id: int,
            new_address: int,
            new_type_code: str,
            new_baud_rate: int,
            set_chksum: str
    ) -> dict[str: int] | None:
        """
        lp1: Set a configuration

        New parameters of the baud rate and chksum are enabled after resetting of the power supply.

        Syntax: %AANNTTCCFF[CHKSUM](CR)
            % Delimiter character
            AA: Address of the instrument to be read
            NN: New address
            TT: New Type code (*)
            CC: New Baud Rate code
            FF: Used to set the CHKSUM format and read other index (*)
            (*) depending on a type of device (see for detailed description)

        Response:
            Valid Response: !AA[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            !: Delimiter character for a valid response
            ?: Delimiter character for an invalid response
            AA: Address of the responding device (00 to FF)

        :param address_id: Address id
        :param new_address: New address id (dec)
        :param new_type_code: New type code, depending on type device
        :param new_baud_rate: New baud rate, valid values are 2400, 4800, 9600, 19200, 38400, 57600, 115200, 230400
        :param set_chksum: Set the checksum format and read other index, depending on type of device
        :return: Decoded response
        """
        def _get_code_from_baud_rate(baud_rate: int) -> str | None:
            """Get the hex code according to the input baud rate"""
            code = None
            for key, value in self._baud_rate_settings.items():
                if value == baud_rate:
                    return key
            if code is None:
                raise ValueError(f"The input baud rate '{baud_rate}' is not in '{self._baud_rate_settings}'")

        NN = self._to_hex(new_address)
        TT = new_type_code
        CC = _get_code_from_baud_rate(baud_rate=new_baud_rate)
        FF = set_chksum
        command = self._to_hex_command('%', self._to_hex(address_id), NN + TT + CC + FF)
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
            }
        )

    def read_configuration(self, address_id: int) -> dict[str: str | int] | None:
        """
        lp2: Read the configuration

        Syntax: $AA2[CHKSUM](CR)
            2: Command to read the configuration

        Response:
            Valid Response: !AATTCCFF[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            TT: Type code
            CC: Baud Rate
            FF: CHK Format and S

        :param: address_id: Address id
        :return: Decoded response
        """
        command = self._to_hex_command('$', self._to_hex(address_id), '2')
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
                'type_code': (3, 4),
                'baud_rate': (5, 6),
                'chk_format_and_s': (7, 8),
            }
        )

    def read_expired_calibration_date(self, address_id: int) -> dict[str: str | int] | None:
        """
        lp4: Read an expired calibration date

        Syntax: $AAD[CHKSUM](CR)
            D: Command to read the calibration date

        Response:
            Valid Response: !AADD-MM-YY[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            DD: Day
            MM: Month
            YY: Year

        :param address_id: Address id
        :return: Decoded response
        """
        command = self._to_hex_command('$', self._to_hex(address_id), 'D')
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
                'calibration_expired_date': (3, 10),
            }
        )

    def read_serial_number(self, address_id: int) -> dict[str: str | int] | None:
        """
        lp6: Read a serial number

        Syntax: $AAF[CHKSUM](CR)
            F Command to read serial number

        Response:
            Valid Response: !AA(Data)[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            (Data): A string indicating the serial number (16 ASCII characters)

        :param address_id: Address id
        :return: Decoded response
        """
        command = self._to_hex_command('$', self._to_hex(address_id), 'F')
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
                'serial_no': (3, 18),
            }
        )

    def read_instrument_name(self, address_id: int) -> dict[str: str | int] | None:
        """
        lp10: Read a name of an instrument

        Syntax: $AAM[CHKSUM](CR)
            M: Command to read the instrument name

        Response:
            Valid Response: !AA(Name)[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            (Name): A string showing the name of the instrument (16 characters)

        :param address_id: Address id
        :return: Decoded response
        """
        command = self._to_hex_command('$', self._to_hex(address_id), 'M')
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
                'instrument_name': (3, 18),
            }
        )

    def read_battery_state(self, address_id: int) -> dict[str: str | int] | None:
        """
        lp12: Read a state of battery

        Syntax: $AAB[CHKSUM](CR)
            B: Command to read the state of battery

        Response:
            Valid Response: !AAN[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            N = 0: for Vdd < min supply 3.6V
            N = 1: for Vdd > min supply 3.6V

        :param address_id: Address id
        :return: Decoded response
        """
        command = self._to_hex_command('$', self._to_hex(address_id), 'B')
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
                'battery_state': (3, 3),
            }
        )

    def set_sleeping_mode(self, address_id: int, activate: bool) -> dict[str: int] | None:
        """
        lp13: Sleeping mode settings

        If the sleeping mode is available, the response to every command except sleeping mode command is '&AA'
        The transducer is ready for the work after 20s.

        Syntax: ~AARN[CHKSUM](CR)
            R: Command to set the calibration mode
            N = 0: for normal mode
            N = 1: for sleeping mode

        Response:
            Valid Response: !AA[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)

        :param address_id: Address id
        :param activate: Activate the sleeping mode
        :return: Decoded response
        """
        N = '1' if activate else '0'
        command = self._to_hex_command('~', self._to_hex(address_id), 'R' + N)
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
            }
        )

    def senso_hygbar_read_configuration(self, address_id: int) -> dict[str: str | int | bool] | None:
        """
        lp2': SensoHygBar - Read the configuration

        Type Setting (TT) - sensor configuration
        TT | Configuration
        01 | Only BAR
        02 | Only HIG
        03 | BAR + HIG

        Data Format Setting (FF)
        F | F
        CS | not used
        CS: Checksum settings
            0: Disabled
            4: Enabled (default)
        not used: should be 0

        :param address_id: Address id
        :return: Decoded response
        """
        def _get_sensor_config_from_tt(tt: str) -> str:
            """Get the sensor_config according to the input TT"""
            config = None
            for key, value in self._senso_hygbar_sensor_config.items():
                if value['TT'] == tt:
                    return key
            if config is None:
                raise ValueError(f"The input TT '{tt}' is not in '{self._senso_hygbar_sensor_config}'")

        decoded_response = self.read_configuration(address_id=address_id)
        if decoded_response is not None:
            # Check TT
            TT = decoded_response.pop('type_code')
            decoded_response['sensor_config'] = _get_sensor_config_from_tt(TT)
            # Check CS
            CS_S = decoded_response.pop('chk_format_and_s')
            CS, S = CS_S[0], CS_S[1]
            if CS in ['0', '4']:
                decoded_response['chksum_enabled'] = (CS == '4')
            else:
                logger.warning(f"Invalid checksum settings '{CS}'")
            if S == '0':
                logger.debug(f"Confirmed not used S '{S}' is '0'")
            else:
                logger.warning(f"The not used S '{S}' should be '0'")
            return decoded_response
        else:
            return None

    def senso_hygbar_read_measurement_data(
            self,
            address_id: int,
            senso_config: str
    ) -> dict[str: float] | None:
        """
        lp22: SensoHygBar - Read a humidity and barometric pressure

        A data for not connected sensor are presented as over range (+99.9+1500.0) only if the sensors are enabled.
        Disabled sensor is not presented.

        Syntax: #AA0[CHKSUM](CR)
            0: Command to read the data

        Response:
            Valid Response: >(Data)[CHKSUM](CR) or <(Data)[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            (Data): A data indicating a humidity and barometric pressure
            Example: >+56.1+0972.1[CHKSUM](CR)

        :param address_id: Address id
        :param senso_config: Configuration of sensor, supported values 'Only HIG', 'Only BAR', 'BAR + HIG'
        :return: Decoded response
        """
        command = self._to_hex_command('#', self._to_hex(address_id), '0')
        response = self._get_response_by_hex_command(hex_command=command)
        data_start_stop_index = self._senso_hygbar_sensor_config[senso_config]['data_start_stop_index']
        return self._decode_response(
            response=response,
            parse={
                'senso_hygbar_measurement_data': data_start_stop_index,
            },
            response_type='senso_hygbar',
            sensor_config=senso_config
        )

    def senso_therm_read_configuration(self, address_id: int) -> dict[str: str | int | bool] | None:
        """
        lp2': SensoTherm - Read the configuration

        Type Setting (TT) - channel configuration
            TT | Channel4 | Channel 3 | Channel 2 | Channel 1
            01 | 0 | 0 | 0 | 1
            02 | 0 | 0 | 1 | 0
            : : : : :
            0F | 1 | 1 | 1 | 1
            0: disabled
            1: enabled

        CHKSUM format and other index (FF)
            F | F
            CS | not used
            CS: Checksum settings
                0: Disabled
                4: Enabled (default)
            not used: should be 0

        :param address_id: Address id
        :return: Decoded response
        """
        decoded_response = self.read_configuration(address_id=address_id)
        if decoded_response is not None:
            # Check TT
            TT = decoded_response.pop('type_code')
            TT_bits = self._hex_to_bits(TT)  # Convert hex to bits
            decoded_response.update({
                'channel_1_config_enabled': TT_bits[-1] == 1,
                'channel_2_config_enabled': TT_bits[-2] == 1,
                'channel_3_config_enabled': TT_bits[-3] == 1,
                'channel_4_config_enabled': TT_bits[-4] == 1,
            })
            # Check CS
            CS_S = decoded_response.pop('chk_format_and_s')
            CS, S = CS_S[0], CS_S[1]
            if CS in ['0', '4']:
                decoded_response['chksum_enabled'] = (CS == '4')
            else:
                logger.warning(f"Invalid checksum settings '{CS}'")
            if S == '0':
                logger.debug(f"Confirmed not used S '{S}' is '0'")
            else:
                logger.warning(f"The not used S '{S}' should be '0'")
            return decoded_response
        else:
            return None

    def senso_therm_read_indicator(self, address_id: int, channel: int) -> dict[str: str | int] | None:
        """
        lp31: SensoTherm - Read a probe indicator from N-channel

        Syntax: $AASN[CHKSUM](CR)
            S: Command to read probe indicator
            N: The channel to be set (1-4)

        Response:
            Valid Response: !AA(Data)[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            (Data) A string indicating the probe indicator (8 ASCII characters)

        :param address_id: Address id
        :param channel: Channel number (1 to 4)
        :return: Decoded response
        """
        if channel < 1 or channel > 4:
            raise ValueError(f"Invalid channel '{channel}', it should be 0 to 4")
        command = self._to_hex_command('$', self._to_hex(address_id), 'S' + str(channel))
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
                'senso_therm_indicator': (3, 10),
            },
            response_type='senso_therm',
        )

    def senso_therm_read_temperatures_enabled_channels(self, address_id: int) -> dict[str: float] | None:
        """
        lp35: SensoTherm: Read temperatures from enabled channels

        A data for the enabled channel and not connected probe are presented as over range (+600.00).
        Disabled channels are not presented.

        Syntax: #AA0[CHKSUM](CR)
            0: Command to read the data

        Response:
            Valid Response: >(Data)[CHKSUM](CR) or <(Data)[CHKSUM](CR) in case of the repeat reading a same data from
                buffer
            Invalid Response: ?AA[CHKSUM](CR)
            (Data): A data indicating temperatures from enabled channels
            Example: >+600.00-025.76+123.57+000.05

        :param address_id: Address id
        :return: Decoded response
        """
        command = self._to_hex_command('#', self._to_hex(address_id), '0')
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'senso_therm_temperatures': (1, 28),
            },
            response_type='senso_therm',
        )

    def senso_therm_read_temperature_specified_channel(self, address_id: int, channel: int) -> dict[str: float] | None:
        """
        lp36: Read a temperature from specified channel

        Syntax: #AAN[CHKSUM](CR)
            N: The channel to be read 1-4

        Response:
            Valid Response: >(Data)[CHKSUM](CR) or <(Data)[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            (Data): A data indicating a temperature from specified channels
            Example: >+025.46

        :param address_id: Address id
        :param channel: Channel number (1 to 4)
        :return: Decoded response
        """
        if channel < 1 or channel > 4:
            raise ValueError(f"Invalid channel '{channel}', it should be 0 to 4")
        command = self._to_hex_command('#', self._to_hex(address_id), str(channel))
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'senso_therm_temperature': (1, 7),
            },
            response_type='senso_therm',
        )

    def senso_anemo_read_configuration(self, address_id: int) -> dict[str: str | int | bool] | None:
        """
        lp2': SensoAnemo - Read the configuration

        Type Setting (TT)
            TT: not used (should be 0)

        CHKSUM format and other index (FF)
            F | F
            CS | OI
            CS: Checksum settings
                0: Disabled
                4: Enabled (default)
            OI Probe Error Identification (only read)
                0 or 1: Yes
                2 or 3: None

        :param address_id: Address id
        :return: Decoded response
        """
        decoded_response = self.read_configuration(address_id=address_id)
        if decoded_response is not None:
            # Check TT
            TT = decoded_response.pop('type_code')
            if TT == '00':
                logger.debug(f"Confirmed type code '{TT}' is '00'")
            else:
                logger.warning(f"The type code '{TT}' should be '00'")
            # Check CS and OI
            CS_OI = decoded_response.pop('chk_format_and_s')
            CS, OI = CS_OI[0], CS_OI[1]
            if CS in ['0', '4']:
                decoded_response['chksum_enabled'] = (CS == '4')
            else:
                logger.warning(f"Invalid checksum settings '{CS}'")
            if OI in ['0', '1', '2', '3']:
                decoded_response['probe_error_identification'] = True if (OI == '0' or OI == '1') else None
            else:
                logger.warning(f"Invalid probe error identification '{OI}'")
            return decoded_response
        else:
            return None

    def senso_anemo_read_indicator(self, address_id: int) -> dict[str: str | int] | None:
        """
        lp42: SensoAnemo - Read the probe indicator

        Syntax: $AAS[CHKSUM](CR)
            S: Command to read probe indicator

        Response:
            Valid Response: !AA(Data)[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            (Data): A string indicating the probe indicator (8 ASCII characters)

        :param address_id: Address id
        :return: Decoded response
        """
        command = self._to_hex_command('$', self._to_hex(address_id), 'S')
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
                'senso_anemo_indicator': (3, 10),
            },
            response_type='senso_anemo',
        )

    def senso_anemo_set_fast_mode(self, address_id: int, activate: bool) -> dict[str: int] | None:
        """
        lp49: SensoAnemo - Fast mode settings (addressed command)

        The fast mode reads the instantaneous value of velocity and temperature
        The fast mode is disabled after resetting the power supply.

        Syntax: ~AAQN[CHKSUM](CR)
            Q: Command to set the fast mode
            N = 0: for normal mode
            N = 1: for fast mode

        Response:
            Valid Response: !AA[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)

        :param address_id: Address id
        :param activate: Activate the fast mode
        :return: Decoded response
        """
        N = '1' if activate else '0'
        command = self._to_hex_command('~', self._to_hex(address_id), 'Q' + N)
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'address_id': (1, 2),
            },
            response_type='senso_anemo',
        )

    def senso_anemo_common_set_fast_mode(self, activate: bool) -> None:
        """
        lp49a: SensoAnemo - Fast mode settings (common command)

        The fast mode reads the instantaneous value of velocity and temperature
        The fast mode is disabled after resetting the power supply.

        Syntax: ~XXQN[CHKSUM](CR)
            Q: Command to set the fast mode
            N = 0: for normal mode
            N = 1: for fast mode

        Response:
            None

        :param activate: Activate the fast mode
        :return: None
        """
        N = '1' if activate else '0'
        command = self._to_hex_command('~', 'XX', 'Q' + N)
        _ = self._get_response_by_hex_command(hex_command=command)
        return None

    def senso_anemo_read_measurement_data(self, address_id: int) -> dict[str: float] | None:
        """
        lp51: SensoAnemo - Read a measurement data

        Syntax: #AA0[CHKSUM](CR)
            0: Command to read the data

        Response:
            Valid Response: >(Data)[CHKSUM](CR) or <(Data)[CHKSUM](CR)
            Invalid Response: ?AA[CHKSUM](CR)
            (Data): A data indicating a mean temperature (t), mean velocity (v) and square root of mean from squares of
                instantaneous velocity values (v*).
            Example: >+25.60+00.565+00.5564

        :param address_id: Address id
        :return: Decoded response
        """
        command = self._to_hex_command('#', self._to_hex(address_id), '0')
        response = self._get_response_by_hex_command(hex_command=command)
        return self._decode_response(
            response=response,
            parse={
                'senso_anemo_measurement_data': (1, 21),
            },
            response_type='senso_anemo',
        )

    @staticmethod
    def _decode_response(
            response: str,
            parse: dict[str: tuple[int, int]],
            response_type: str = 'general',
            **kwargs
    ) -> dict[str: str | int | float] | None:
        """
        Decode the response of device
        :param response: Response in utf-8
        :param parse: Parse dict, with the format {'key1': (start_index, stop_index), ...}
        :param response_type: Sensor response type, valid are 'general', 'senso_anemo', 'senso_therm', 'senso_hygbar'
        :param kwargs: 'sensor_config' for SensoHygBar
        :return: Dict with decoded parameters or None if invalid response
        """
        def _get_largest_index(parse_dict: dict[str: tuple[int, int]]) -> int:
            """Find the largest number in all tuples in the parse dict"""
            return max(max(tup) for tup in parse_dict.values())

        def _get_baud_rate_from_code(code: str) -> int:
            """Get the baud rate from baud rate settings"""
            return SensoSys._baud_rate_settings.get(code)

        def _decode_general(rsp: str, par: dict[str: tuple[int, int]]) -> dict[str | int]:
            """General response decoder"""
            decoded_response = {}
            for key, (index_start, index_stop) in par.items():
                if key == 'address_id':
                    decoded_response[key] = int(rsp[index_start: index_stop + 1], 16)
                elif key == 'baud_rate':
                    decoded_response[key] = _get_baud_rate_from_code(rsp[index_start: index_stop + 1])
                else:
                    decoded_response[key] = rsp[index_start: index_stop + 1]
            return decoded_response

        def _decode_senso_anemo(rsp: str, par: dict[str: tuple[int, int]]) -> dict[str | int | float]:
            """Response decoder for SensoAnemo"""
            decoded_response = {}
            for key, (index_start, index_stop) in par.items():
                if key == 'address_id':
                    decoded_response[key] = int(rsp[index_start: index_stop + 1], 16)
                elif key == 'senso_anemo_measurement_data':
                    raw_data = rsp[index_start: index_stop + 1]
                    t, v, v_star = raw_data[0:6], raw_data[6:13], raw_data[13:21]
                    decoded_response.update({
                        't_a': round(float(t), 2),  # Air temperature (timely averaged / instantaneous)
                        'v': round(float(v), 3),  # Air speed (timely averaged / instantaneous)
                        'v_star': round(float(v_star), 4)  # Root of 2s averaged value of squared speed
                    })
                else:
                    decoded_response[key] = rsp[index_start: index_stop + 1]
            return decoded_response

        def _decode_senso_therm(rsp: str, par: dict[str: tuple[int, int]]) -> dict[str | int | float]:
            """Response decoder for SensoTherm"""
            decoded_response = {}
            for key, (index_start, index_stop) in par.items():
                if key == 'address_id':
                    decoded_response[key] = int(rsp[index_start: index_stop + 1], 16)
                elif key == 'senso_therm_temperatures':
                    raw_data = rsp[index_start: index_stop + 1]
                    t_a, t_g, t_w, t_s = raw_data[0:7], raw_data[7:14], raw_data[14:21], raw_data[21:28]
                    decoded_response.update({
                        't_a': round(float(t_a), 2),  # Air temperature
                        't_g': round(float(t_g), 2),  # Corrected globe temperature
                        't_w': round(float(t_w), 2),  # Wet bulb temperature
                        't_s': round(float(t_s), 2),  # Supplementary temperature
                    })
                elif key == 'senso_therm_temperature':
                    raw_data = rsp[index_start: index_stop + 1]
                    decoded_response.update({
                        't_ch': round(float(raw_data), 2),  # Temperature of the channel
                    })
                else:
                    decoded_response[key] = rsp[index_start: index_stop + 1]
            return decoded_response

        def _decode_senso_hygbar(
                rsp: str, par: dict[str: tuple[int, int]], sensor_config: str) -> dict[str | int | float]:
            """Response decoder for SensoHigBar"""
            decoded_response = {}
            for key, (index_start, index_stop) in par.items():
                if key == 'address_id':
                    decoded_response[key] = int(rsp[index_start: index_stop + 1], 16)
                elif key == 'senso_hygbar_measurement_data':
                    raw_data = rsp[index_start: index_stop + 1]
                    if sensor_config == 'Only BAR':
                        p = raw_data
                        decoded_response.update({'p': round(float(p), 1)})
                    elif sensor_config == 'Only HIG':
                        rh = raw_data
                        decoded_response.update({'rh': round(float(rh), 1)})
                    elif sensor_config == 'BAR + HIG':
                        rh, p = raw_data[0:5], raw_data[5:12]
                        decoded_response.update({
                            'rh': round(float(rh), 1),
                            'p': round(float(p), 1),
                        })
                    else:
                        raise ValueError(f"Invalid sensor_config {sensor_config}")
                else:
                    decoded_response[key] = rsp[index_start: index_stop + 1]
            return decoded_response

        if not response:
            # No response due to time out
            logger.info(f"No data received due to time out or error")
            return None
        elif response.startswith(('!', '>', '<')):
            # Valid response
            # Recalculate checksum for validation
            chksum_start_index = _get_largest_index(parse_dict=parse) + 1  # Start index of chksum
            chksum = response[chksum_start_index: chksum_start_index + 2].upper()  # Get chksum from response
            expected_checksum = SensoSys._calculate_checksum(response[: chksum_start_index])  # Calculate chksum
            if chksum == expected_checksum:
                # Valid response with matched chksum, decode the response
                if response_type == 'general':
                    return _decode_general(rsp=response, par=parse)
                elif response_type == 'senso_anemo':
                    return _decode_senso_anemo(rsp=response, par=parse)
                elif response_type == 'senso_therm':
                    return _decode_senso_therm(rsp=response, par=parse)
                elif response_type == 'senso_hygbar':
                    _sensor_config = kwargs.pop('sensor_config')
                    if _sensor_config is None:
                        raise AttributeError("Attribute 'sensor_config' should be provided for SensoHygBar")
                    return _decode_senso_hygbar(
                        rsp=response, par=parse, sensor_config=_sensor_config)
                else:
                    raise ValueError(f"Invalid response type '{response_type}'")
            else:
                # Valid response with mismatched chksum
                logger.info(f"Invalid response received due to checksum mismatch: '{response}'")
                return None
        elif response.startswith('?'):
            # Invalid response
            # Format always as '?AA[CHKSUM](CR)'
            address_id = int(response[1: 3], 16)
            logger.info(f"Invalid response received by address-id '{address_id}': '{response}'")
            return None
        else:
            logger.warning(f"Unexpected response format received: '{response}'")
            return None

    @staticmethod
    def _to_hex(value: int) -> str:
        """Converts an integer (dec) to hex with 2 digits"""
        if isinstance(value, int):
            return format(value, '02X')
        else:
            raise ValueError(f"Input value '{value}' must be an integer")

    @staticmethod
    def _calculate_checksum(command_or_response: str) -> str:
        """
        Convert a command or a response to checksum (CHKSUM)

        This system has 2-character checksum in the ASCII code as a sum of all the characters in the command/response
        string (leading character + address (hex) + command/data) except for the carriage return character (CR)

        :param command_or_response: Input of command or response
        :return: Checksum in hex
        """
        checksum = sum(ord(c) for c in command_or_response) % 256
        return format(checksum, '02X')

    @staticmethod
    def _to_hex_command(leading_character: str, address_hex_id: str, command: str) -> str:
        """
        Generate hex command for hex address with leading character and command
        :param leading_character: Leading character
        :param address_hex_id: Address ID (hex) from 00 to FF
        :param command: Command
        :return: Hex commands, incl. leading cha, address, command, CHKSUM, CR
        """
        chksum = SensoSys._calculate_checksum(command_or_response=leading_character + address_hex_id + command)
        return leading_character + address_hex_id + command + chksum + '\r'

    @staticmethod
    def _hex_to_bits(hex_str: str) -> list[int]:
        """Convert a hex string to list of bits (int)"""
        # Convert hex string to binary string and remove the '0b' prefix
        binary_str = bin(int(hex_str, 16))[2:].zfill(4)
        return [int(bit) for bit in binary_str]

    @property
    def baud_rate_settings(self) -> dict[str, int]:
        return self._baud_rate_settings

    @property
    def senso_hygbar_sensor_config(self):
        return self._senso_hygbar_sensor_config


def scan_com_ports() -> list[str] | None:
    """
    Scan all available COM ports of Windows system
    :return: List of COM-ports, None if no ports are available
    """
    logger.info("Scanning available COM-port(s) of system ...")
    ports = serial.tools.list_ports.comports()  # -> list of ports
    if len(ports) == 0:
        logging.debug(f"No COM-ports available")
        return None
    else:
        logging.debug(f"Found {len(ports)} COM-port(s)")
        for port in ports:
            logging.debug(f"Port: '{port.device}', Description: '{port.description}', Hardware ID: '{port.hwid}'")
        return [pt.device for pt in ports]


def pop_system_device_management():
    """Pop the Device Manager 'devmgmt.msc'"""
    subprocess.Popen('devmgmt.msc', shell=True)


def _test(
        port: str,
        scan_available_devices: bool,
        addresses_senso_hygbar: list[int] | None,
        addresses_senso_therm: list[int] | None,
        addresses_senso_anemo: list[int] | None,
        measurement_data_requirement: int = 5,
        measurement_data_requirement_interval: float = 1.0
):
    """
    Test class SensoSys
    All 'set' methods will not be tested: lp1, lp13, lp49, lp49a
    """

    def scan_devices(senso_sys: SensoSys) -> dict[str: str]:
        """Scan all available devices connected"""
        available_devices = {}
        for scan_id in range(0, 256):
            print(f"Scanning id = {scan_id} ...")
            device_serial_number = senso_sys.read_serial_number(scan_id)
            if device_serial_number is not None:
                print(f"Found device with ID = {scan_id}")
                available_devices.update({
                    str(scan_id): {
                        'serial_number': device_serial_number,
                        'instrument_name': senso_sys.read_instrument_name(scan_id),
                        'calibration_expired_date': senso_sys.read_expired_calibration_date(scan_id),
                        'battery_state': senso_sys.read_battery_state(scan_id),
                        'configuration': senso_sys.read_configuration(scan_id),
                    }
                })
        return available_devices

    def general_tests(senso_sys: SensoSys, dev_id: int):
        """Test for general commands"""
        print(f"ID '{dev_id}' - lp2 'read_configuration': {senso_sys.read_configuration(dev_id)}")
        print(f"ID '{dev_id}' - lp4 'read_expired_calibration_date': {senso_sys.read_expired_calibration_date(dev_id)}")
        print(f"ID '{dev_id}' - lp6 'read_serial_number': {senso_sys.read_serial_number(dev_id)}")
        print(f"ID '{dev_id}' - lp10 'read_instrument_name': {senso_sys.read_instrument_name(dev_id)}")
        print(f"ID '{dev_id}' - lp12 'read_battery_state': {senso_sys.read_battery_state(dev_id)}")

    # Init the SensoSys
    print("\nInitializing SensoSys ...")
    test_senso_sys = SensoSys(port=port, time_out=0.05)

    # Scan available devices
    if scan_available_devices:
        print("\nScanning all available devices ...")
        all_available_devices = scan_devices(senso_sys=test_senso_sys)
        print(f"All available devices: \n{all_available_devices}")
        print(f"Number of available devices: {len(all_available_devices)}")

    # Test SensoHygBar
    if addresses_senso_hygbar is not None:
        for _id in addresses_senso_hygbar:
            print(f"\nStart testing SensorHygBar ID = {_id} ...")
            # Run general tests
            general_tests(senso_sys=test_senso_sys, dev_id=_id)
            # Run specific tests
            print(
                f"ID '{_id}' - lp2' 'senso_hygbar_read_configuration': "
                f"{test_senso_sys.senso_hygbar_read_configuration(_id)}")
            _sensor_config = test_senso_sys.senso_hygbar_read_configuration(_id)['sensor_config']
            for req in range(1, measurement_data_requirement + 1):
                print(
                    f"ID '{_id}', Req. {req} - lp22 'senso_hygbar_read_measurement_data': "
                    f"{test_senso_sys.senso_hygbar_read_measurement_data(_id, _sensor_config)}"
                )
                time.sleep(measurement_data_requirement_interval)

    # Test SensoTherm
    if addresses_senso_therm is not None:
        for _id in addresses_senso_therm:
            print(f"\nStart testing SensoTherm ID = {_id} ...")
            # Run general tests
            general_tests(senso_sys=test_senso_sys, dev_id=_id)
            # Run specific tests
            print(
                f"ID '{_id}' - lp2' 'senso_therm_read_configuration': "
                f"{test_senso_sys.senso_therm_read_configuration(_id)}")
            for ch in range(1, 5):
                print(
                    f"ID '{_id}', Ch. '{ch}' - lp31 'senso_therm_read_indicator': "
                    f"{test_senso_sys.senso_therm_read_indicator(_id, ch)}"
                )
            for req in range(1, measurement_data_requirement + 1):
                print(
                    f"ID '{_id}', Req. {req} - lp35 'senso_therm_read_temperatures_enabled_channels': "
                    f"{test_senso_sys.senso_therm_read_temperatures_enabled_channels(_id)}")
                time.sleep(measurement_data_requirement_interval)
            for req in range(1, measurement_data_requirement + 1):
                for ch in range(1, 5):
                    print(
                        f"ID '{_id}', Req. {req}, Ch. '{ch}' - lp36 'senso_therm_read_temperature_specified_channel': "
                        f"{test_senso_sys.senso_therm_read_temperature_specified_channel(_id, ch)}"
                    )
                time.sleep(measurement_data_requirement_interval)

    # Test SensoAnemo
    if addresses_senso_anemo is not None:
        for _id in addresses_senso_anemo:
            print(f"\nStart testing SensoAnemo ID = {_id} ...")
            # Run general tests
            general_tests(senso_sys=test_senso_sys, dev_id=_id)
            # Run special tests
            print(
                f"ID '{_id}' - lp2' 'senso_anemo_read_configuration': "
                f"{test_senso_sys.senso_anemo_read_configuration(_id)}")
            print(
                f"ID '{_id}' - lp42 'senso_anemo_read_indicator': {test_senso_sys.senso_anemo_read_indicator(_id)}")
            for req in range(1, measurement_data_requirement + 1):
                print(
                    f"ID '{_id}', Req. {req} - lp51 'senso_anemo_read_measurement_data': "
                    f"{test_senso_sys.senso_anemo_read_measurement_data(_id)}")
                time.sleep(measurement_data_requirement_interval)

    # Close the port, delete the instance
    print("\nClosing serial connection ...")
    test_senso_sys.close_serial_connection()
    del test_senso_sys
    print("\nTesting SensoSys finished")


if __name__ == '__main__':
    # Scan available COM ports
    available_com_ports = scan_com_ports()
    print(f"Available COM ports: {available_com_ports}")

    # Test SensoSys
    run_test = True
    if run_test:
        _test(
            port='COM6',
            scan_available_devices=True,
            addresses_senso_hygbar=[55, 57, 73],
            addresses_senso_therm=[72],
            addresses_senso_anemo=[12, 36, 37, 71],
            measurement_data_requirement=5,
            measurement_data_requirement_interval=0.5
        )
