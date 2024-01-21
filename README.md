# Airmar-150WX-Sensor-Data

Airmar 150WX Sensor Data

## Requirments

sudo apt install python3-nmea2 python3-serial python3-paho-mqtt

## Installation

1. sudo mkdir /opt/airmar_reader
2. sudo mv /path/to/airmar_reader.py /opt/airmar_reader/
3. sudo chmod +x /opt/airmar_reader/airmar_reader.py
4. sudo nano /etc/systemd/system/airmar_reader.service
5. sudo systemctl daemon-reload
6. sudo systemctl enable airmar_reader.service
7. sudo systemctl start airmar_reader.service
