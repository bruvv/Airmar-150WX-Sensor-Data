import io
import json
from decimal import Decimal

import pynmea2
import serial
from paho.mqtt import client as mqtt_client

# MQTT configuration
broker = "XXXX"  # Replace with your MQTT broker address
port = 1883  # Replace with your MQTT broker port
client_id = "namethis"
username = "XXX"
password = "XXXXX"
discovery_prefix = "homeassistant"  # Default discovery prefix for Home Assistant


# Custom JSON Encoder class to handle Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal to float
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def publish_discovery_config(client):
    # ZDA Timestamp
    zda_config = {
        "name": "ZDA Timestamp",
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/zda/state",
        "device_class": "timestamp",
        "unique_id": "zda_timestamp",
        "icon": "mdi:clock",
        "value_template": "{{ value_json.timestamp }}",
        "qos": 1,
        "device": {
            "name": "Airmar",
            "identifiers": ["airmar150wx"],
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/zda/config",
        json.dumps(zda_config, cls=DecimalEncoder),
        retain=True,
    )

    # GGA GPS Data
    gga_config = {
        "name": "GGA GPS Data",
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/gga/state",
        "unique_id": "gga_gps_data",
        "value_template": "{{ value_json.latitude }}, {{ value_json.longitude }}, GPS Quality:{{ value_json.gps_quality }}, Number of satellites: {{ value_json.number_of_satellites }}, Altitude: {{ value_json.Altitude }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/gga/config",
        json.dumps(gga_config, cls=DecimalEncoder),
        retain=True,
    )

    # MWV - Wind Data
    mwv_config = {
        "name": "MWV Wind Data",
        "unique_id": "mwv_wind_data",
        "device_class": "wind_speed",
        "unit_of_measurement": "°, m/s",  # Degrees, Meters per second
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mwv/state",
        "value_template": "{{ value_json.wind_angle }}° {{ value_json.wind_speed_units }} at {{ value_json.wind_speed }} m/s",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mwv/config",
        json.dumps(mwv_config, cls=DecimalEncoder),
        retain=True,
    )
    # VTG - Track Made Good and Ground Speed
    vtg_config = {
        "name": "VTG Track and Speed",
        "unique_id": "vtg_speed",
        "unit_of_measurement": "°, kph",  # Degrees, Kph
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/vtg/state",
        "value_template": "Track: {{ value_json.true_track }}°, Speed: {{ value_json.spd_over_grnd_kmph }} kph",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/vtg/config",
        json.dumps(vtg_config),
        retain=True,
    )

    # HDT - Heading
    hdt_config = {
        "name": "HDT Heading",
        "unique_id": "hdt_heading",
        "unit_of_measurement": "°",  # Degrees
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/hdt/state",
        "value_template": "Heading: {{ value_json.heading }}°",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/hdt/config",
        json.dumps(hdt_config),
        retain=True,
    )
    # MDA - Meteorological Composite Data
    mda_config = {
        "name": "MDA Meteorological Data",
        "device_class": "temperature",
        "unique_id": "mda_meteorological_data1",
        "icon": "mdi:weather-sunny",
        "unit_of_measurement": "bar, °C, %",  # Bar, Celsius, Percent",
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mda/state",
        "value_template": "{{ value_json.barometric_pressure }}",
        # "value_template": "Pressure: {{ value_json.barometric_pressure }} bar, Temp: {{ value_json.air_temperature }}°C, Humidity: {{ value_json.humidity }}%",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mda/config",
        json.dumps(mda_config, cls=DecimalEncoder),
        retain=True,
    )


def weatherparsing(client):
    publish_discovery_config(client)
    ser = serial.Serial("/dev/ttyUSB0", 4800, timeout=5.0)
    sio = io.TextIOWrapper(io.BufferedRWPair(ser, ser))
    while True:
        try:
            line = sio.readline()
            msg = pynmea2.parse(line)
            data = {}
            if isinstance(msg, pynmea2.types.talker.ZDA):
                data = {
                    "timestamp": msg.timestamp,
                    "dag": msg.day,
                    "maand": msg.month,
                    "jaar": msg.year,
                }
                client.publish(
                    f"{discovery_prefix}/sensor/{client_id}/zda/state",
                    json.dumps(data, cls=DecimalEncoder),
                    retain=False,
                )
            elif isinstance(msg, pynmea2.types.talker.MDA):
                data = {
                    "barometric_pressure": msg.b_pressure_bar,
                    # "air_temperature": msg.air_temp,
                    # "humidity": msg.rel_humidity,
                    # "dew_point": msg.dew_point,
                    # "wind_speed_meters": msg.wind_speed_meters,
                }
                # print(msg.b_pressure_bar)
                client.publish(
                    f"{discovery_prefix}/sensor/{client_id}/mda/state",
                    json.dumps(data, cls=DecimalEncoder),
                    retain=False,
                )
                print(json.dumps(data, cls=DecimalEncoder))
            elif isinstance(msg, pynmea2.types.talker.MWV):
                data = {"wind_angle": msg.wind_angle, "wind_speed": msg.wind_speed}
                client.publish(
                    f"{discovery_prefix}/sensor/{client_id}/mwv/state",
                    json.dumps(data, cls=DecimalEncoder),
                )
            elif isinstance(msg, pynmea2.types.talker.VTG):
                data = {
                    "true_track": msg.true_track,
                    "magnetic_track": msg.mag_track,
                    "spd_over_grnd_kmph": msg.spd_over_grnd_kmph,
                }
                client.publish(
                    f"{discovery_prefix}/sensor/{client_id}/vtg/state",
                    json.dumps(data, cls=DecimalEncoder),
                )
            elif isinstance(msg, pynmea2.types.talker.HDT):
                data = {"heading": msg.heading}
                client.publish(
                    f"{discovery_prefix}/sensor/{client_id}/hdt/state",
                    json.dumps(data, cls=DecimalEncoder),
                )
            elif isinstance(msg, pynmea2.types.talker.GGA):
                lat = msg.latitude
                lon = msg.longitude
                data["latitude"], data["longitude"] = lat, lon
                client.publish(
                    f"{discovery_prefix}/sensor/{client_id}/gga/state",
                    json.dumps(data, cls=DecimalEncoder),
                )

        except serial.SerialException as e:
            print(f"Device error: {e}")
            break
        except pynmea2.ParseError as e:
            print(f"Parse error: {e}")
            continue


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("INFO :: Connected to MQTT Broker")
        else:
            print("ERROR :: Connection failed:", rc)
            exit(0)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.connect(broker, port)
    client.on_connect = on_connect
    return client


def run():
    try:
        while True:
            client = connect_mqtt()
            client.loop_start()
            weatherparsing(client)
    except KeyboardInterrupt:
        print("Program interrupted. Exiting...")
        # Optional: Perform any necessary cleanup here
        # For example, disconnect from the MQTT client
        client.loop_stop()
        client.disconnect()
        exit(0)


if __name__ == "__main__":
    run()
