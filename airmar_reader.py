import io
import json
import time

import pynmea2
import serial
from paho.mqtt import client as mqtt_client

# MQTT configuration
broker = "x.x.x.x"  # Replace with your MQTT broker address
port = 1883  # Replace with your MQTT broker port
client_id = "weatherstationairmax"  # the client id you want to use, can be anything
username = "xxxxx"  # your mqtt username
password = "xxxxx"  # your mqtt password
discovery_prefix = "homeassistant"  # Default discovery prefix for Home Assistant do not change this unless you know what you are doing


def publish_discovery_config(client):
    mdab_config = {
        "name": "Barometric Pressure",
        "device_class": "atmospheric_pressure",
        "unique_id": "mda_barometric_pressure",
        "icon": "mdi:weather-sunny",
        "unit_of_measurement": "bar",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mdab/state",
        "value_template": "{{ value_json.barometric_pressure }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mdab/config",
        json.dumps(mdab_config),
        retain=True,
    )
    # MDA - Meteorological Composite Data
    mdat_config = {
        "name": "Temperature",
        "device_class": "temperature",
        "unique_id": "mda_barometric_temperature",
        "icon": "mdi:weather-sunny",
        "unit_of_measurement": "°C",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mdat/state",
        "value_template": "{{ value_json.air_temperature }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mdat/config",
        json.dumps(mdat_config),
        retain=True,
    )
    # MDA - Meteorological Composite Data
    mdah_config = {
        "name": "Relative humidity",
        "device_class": "humidity",
        "unique_id": "mda_barometric_humidity",
        "icon": "mdi:weather-sunny",
        "unit_of_measurement": "%",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mdah/state",
        "value_template": "{{ value_json.rel_humidity }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mdah/config",
        json.dumps(mdah_config),
        retain=True,
    )

    mdadp_config = {
        "name": "Dew point",
        "device_class": "humidity",
        "unique_id": "mda_barometric_dew_point",
        "icon": "mdi:weather-sunny",
        "unit_of_measurement": "°C",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mdadp/state",
        "value_template": "{{ value_json.dew_point }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mdadp/config",
        json.dumps(mdadp_config),
        retain=True,
    )

    hdt_config = {
        "name": "Heading",
        "device_class": "timestamp",
        "unique_id": "hdt_heading",
        "icon": "mdi:weather-sunny",
        "unit_of_measurement": "°",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/hdt/state",
        "value_template": "{{ value_json.heading }}",
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

    wsp_config = {
        "name": "Wind Speed",
        "device_class": "wind_speed",
        "unique_id": "wsp",
        "icon": "mdi:weather-sunny",
        "unit_of_measurement": "m/s",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/wsp/state",
        "value_template": "{{ value_json.wind_speed }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mwv_wsp/config",
        json.dumps(wsp_config),
        retain=True,
    )

    wsa_config = {
        "name": "Wind Angle",
        "device_class": "wind_speed",
        "unique_id": "wsa",
        "icon": "mdi:weather-sunny",
        "unit_of_measurement": "∠",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/wsa/state",
        "value_template": "{{ value_json.wind_angle }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mwv_wsa/config",
        json.dumps(wsa_config),
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
            # print(msg)
            if msg.sentence_type == "MDA":
                if msg.b_pressure_bar is not None:
                    # Pressure
                    pressure = {"barometric_pressure": float(msg.b_pressure_bar)}
                    # print(f"Presure {pressure}")
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/mdab/state",
                        json.dumps(pressure),
                        retain=False,
                    )
                if msg.air_temp is not None:
                    # Temperature
                    temp = {"air_temperature": float(msg.air_temp)}
                    # print(f"Temperature {temp}")
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/mdat/state",
                        json.dumps(temp),
                        retain=False,
                    )
                if msg.rel_humidity is not None:
                    rel_hum = {"rel_humidity": float(msg.rel_humidity)}
                    # print(f"Relative humidity {rel_hum}")
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/mdah/state",
                        json.dumps(rel_hum),
                        retain=False,
                    )
                if msg.dew_point is not None:
                    dew_point = {"dew_point": float(msg.dew_point)}
                    # print(f"Dew point {dew_point}")
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/mdadp/state",
                        json.dumps(dew_point),
                        retain=False,
                    )
            if msg.sentence_type == "HDT":
                if msg.heading is not None:
                    hdt = {"heading": float(msg.heading)}
                    # print(f"Heading {hdt}")
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/hdt/state",
                        json.dumps(hdt),
                        retain=False,
                    )
            if msg.sentence_type == "MWV":
                if msg.wind_speed is not None:
                    wsp = {
                        "wind_speed": float(msg.wind_speed) * 0.514444
                    }  # convert from kn to ms
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/wsp/state",
                        json.dumps(wsp),
                        retain=False,
                    )
                if msg.wind_angle is not None:
                    wsa = {"wind_angle": float(msg.wind_angle)}
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/wsa/state",
                        json.dumps(wsa),
                        retain=False,
                    )
            if msg.sentence_type == "ZDA":
                timestamp = {"timestamp": msg.timestamp}
                day = {"day": msg.day}
                month = {"month": msg.month}
                year = {"year": msg.year}
                local_zone = {"local_zone": msg.local_zone}
                print(timestamp, day, month, year, local_zone)
                # client.publish(
                #     f"{discovery_prefix}/sensor/{client_id}/timestamp/state",
                #     json.dumps(timestamp),
                #     retain=False,
                # )
                # client.publish(
                #     f"{discovery_prefix}/sensor/{client_id}/day/state",
                #     json.dumps(day),
                #     retain=False,
                # )
                # client.publish(
                #     f"{discovery_prefix}/sensor/{client_id}/month/state",
                #     json.dumps(month),
                #     retain=False,
                # )
                # client.publish(
                #     f"{discovery_prefix}/sensor/{client_id}/year/state",
                #     json.dumps(year),
                #     retain=False,
                # )
                # client.publish(
                #     f"{discovery_prefix}/sensor/{client_id}/local_zone/state",
                #     json.dumps(local_zone),
                #     retain=False,
                # )
            if msg.sentence_type == "VTG":
                true_track = {"true_track": msg.true_track}
                mag_track = {"mag_track": msg.mag_track}
                spd_over_grnd_kmph = {"spd_over_grnd_kmph": msg.spd_over_grnd_kmph}
                print(true_track, mag_track, spd_over_grnd_kmph)
                # client.publish(
                #     f"{discovery_prefix}/sensor/{client_id}/timestamp/state",
                #     json.dumps(timestamp),
                #     retain=False,
                # )
            if msg.sentence_type == "GGA":
                lat = {"lat": msg.lat}
                lon = {"lon": msg.lon}
                gps_qual = {"gps_qual": msg.gps_qual}
                num_sats = {"num_sats": msg.num_sats}
                altitude = {"altitude": msg.altitude}
                print(lat, lon, gps_qual, num_sats, altitude)
                # client.publish(
                #     f"{discovery_prefix}/sensor/{client_id}/timestamp/state",
                #     json.dumps(timestamp),
                #     retain=False,
                # )

        except serial.SerialException as e:
            print(f"Device error: {e}")
            break
        except pynmea2.ParseError as e:
            print(f"Parse error: {e}")
            continue
        except UnicodeDecodeError as e:
            print(f"Unicode Decode error: {e}")
            break


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
        client.loop_stop()
        time.sleep(1)
        client.disconnect()
        time.sleep(1)
        exit(0)


if __name__ == "__main__":
    run()
