import io

import pynmea2
import serial
from paho.mqtt import client as mqtt_client

# MQTT configuration
broker = "x.x.x.x"  # Replace with your MQTT broker address
port = 1883  # Replace with your MQTT broker port
topic = "nmea/data"  # Replace with your desired topic
client_id = "weatherstation-buiten"
username = "user"
password = "password"


def format_zda_message(msg):
    return f"Timestamp: {msg.timestamp}, Date: {msg.day}/{msg.month}/{msg.year}"


def format_gga_message(msg):
    return (
        f"Latitude: {msg.lat} {msg.lat_dir}, "
        f"Longitude: {msg.lon} {msg.lon_dir}\n"
        f"GPS Quality: {msg.gps_qual}, Number of Satellites: {msg.num_sats}\n"
        f"Altitude: {msg.altitude}{msg.altitude_units}"
    )


def format_hdt_message(msg):
    return f"Heading: {msg.heading}° True"


def format_mwv_message(msg):
    return f"Wind Angle: {msg.wind_angle}°, Wind Speed: {msg.wind_speed}{msg.wind_speed_units}"


def format_mda_message(msg):
    return (
        f"Barometric Pressure: {msg.b_pressure_bar} bar\n"
        f"Air Temperature: {msg.air_temp}°C, Relative Humidity: {msg.rel_humidity}%, "
        f"Dew Point: {msg.dew_point}°C\n"
        f"Wind Speed: {msg.wind_speed_meters} m/s"
    )


def format_vtg_message(msg):
    return (
        f"True Track: {msg.true_track}°, Magnetic Track: {msg.mag_track}°\n"
        f"Speed over Ground: {msg.spd_over_grnd_kmph} km/h"
    )


ser = serial.Serial("/dev/ttyUSB0", 4800, timeout=5.0)
sio = io.TextIOWrapper(io.BufferedRWPair(ser, ser))


def weatherparsing():
    client = connect_mqtt()
    client.loop_start()
    while True:
        try:
            line = sio.readline()
            msg = pynmea2.parse(line)

            formatted_msg = ""
            if isinstance(msg, pynmea2.types.talker.ZDA):
                print("\n")
                print(format_zda_message(msg))
                formatted_msg = format_zda_message(msg)
            elif isinstance(msg, pynmea2.types.talker.MWV):
                print(format_mwv_message(msg))
                formatted_msg = format_mwv_message(msg)
            elif isinstance(msg, pynmea2.types.talker.MDA):
                print(format_mda_message(msg))
                formatted_msg = format_mda_message(msg)
            elif isinstance(msg, pynmea2.types.talker.VTG):
                print(format_vtg_message(msg))
                formatted_msg = format_vtg_message(msg)
            elif isinstance(msg, pynmea2.types.talker.HDT):
                print(format_hdt_message(msg))
                formatted_msg = format_hdt_message(msg)
            elif isinstance(msg, pynmea2.types.talker.GGA):
                print(format_gga_message(msg))
                formatted_msg = format_gga_message(msg)

            if formatted_msg:
                # print(formatted_msg)
                client.publish(topic, formatted_msg)

        except serial.SerialException as e:
            print(f"Device error: {e}")
            break
        except pynmea2.ParseError as e:
            print(f"Parse error: {e}")
            continue


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def run():
    client = mqtt_client.Client(client_id)
    weatherparsing()
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    run()
