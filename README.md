# EBC-Measurements

**All-in-One Toolbox for Measurement Data Acquisition and Data Logging**

## About the project

Data logging is the process of acquiring data over time from various sources, typically using sensors or instruments, 
and storing them in one or multiple outputs, such as files or databases.
This Python package provides easy understandable interfaces for various data sources and outputs, facilitating a quick 
and easy configuration for data logging and data transfer.

Potential use cases include field measurements, test bench monitoring, and Hardware-in-the-Loop (HiL) development. 
With its versatile capabilities, this toolbox aims to enhance the efficiency of data acquisition processes across 
different applications.

## Currently supported systems

The toolbox currently supports the following platforms and protocols:

- [Beckhoff PLC](https://www.beckhoff.com/)
- [ICP DAS](https://www.icpdas.com/) (Currently, the package only supports the 
[DCON Based I/O Expansion Unit](https://www.icpdas.com/en/product/guide+Remote__I_O__Module__and__Unit+Ethernet__I_O__Modules+IO__Expansion__Unit) 
with the I-87K series.)
- [MQTT protocol](https://mqtt.org/)
- [Sensor Electronic](http://sensor-electronic.pl/), which includes the air distribution measuring system 
[AirDistSys 5000](http://sensor-electronic.pl/pdf/KAT_AirDistSys5000.pdf), and thermal condition monitoring system 
[ThermCondSys 5500](http://sensor-electronic.pl/pdf/KAT_ThermCondSys5500.pdf)
