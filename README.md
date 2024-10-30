# EBC-Measurements

**All-in-One Toolbox for Measurement Data Acquisition and Data Logging**

## About the project

Data logging is the process of acquiring data over time from various sources, typically using sensors or instruments, 
and storing them in one or multiple outputs, such as files or databases.
This Python package provides easily understandable interfaces for various data sources and outputs, facilitating a quick 
and easy configuration for data logging and data transfer.

Potential use cases include field measurements, test bench monitoring, and Hardware-in-the-Loop (HiL) development. 
With its versatile capabilities, this toolbox aims to enhance the efficiency of data acquisition processes across 
different applications.

## Data logger

As the key component in the data logging process, the data logger in this toolbox ensures high flexibility in the 
logging procedure, featuring the following capabilities:

- Read and write data from and to multiple systems simultaneously
- Rename each variable in data sources for each output individually
- Check variable names and automatically prefix to avoid duplicates in data outputs
- Perform data type conversion for each variable in data sources for each data output individually

The following types of data loggers are available in the toolbox:

- Periodic trigger (time trigger)
- MQTT on-message trigger

## Currently supported systems

The toolbox currently supports the following platforms and protocols, as shown in the table:

|                         System                          | Read from system<br>(data source) | Write to system<br>(data output) | Note                                                                                                                                                                                                                                                                                                                                                             | 
|:-------------------------------------------------------:|:---------------------------------:|:--------------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|        [Beckhoff PLC](https://www.beckhoff.com/)        |                Yes                |               Yes                | -                                                                                                                                                                                                                                                                                                                                                                |
|           [ICP DAS](https://www.icpdas.com/)            |                Yes                |         Yes (not tested)         | Currently, the package only supports the [DCON Based I/O Expansion Unit](https://www.icpdas.com/en/product/guide+Remote__I_O__Module__and__Unit+Ethernet__I_O__Modules+IO__Expansion__Unit) with the I-87K series.                                                                                                                                               |
|           [MQTT protocol](https://mqtt.org/)            |                Yes                |               Yes                | -                                                                                                                                                                                                                                                                                                                                                                |
| [The Things Network](https://www.thethingsnetwork.org/) |                Yes                |         Yes (not tested)         | Communication is via MQTT Server supported by The Things Stack.                                                                                                                                                                                                                                                                                                  |
|    [Sensor Electronic](http://sensor-electronic.pl/)    |                Yes                |                No                | The package supports the Air Distribution Measuring System ([AirDistSys 5000](http://sensor-electronic.pl/pdf/KAT_AirDistSys5000.pdf)) and the Thermal Condition Monitoring System ([ThermCondSys 5500](http://sensor-electronic.pl/pdf/KAT_ThermCondSys5500.pdf)). Device configuration is possible, but it is not directly accessible via the data source API. |

