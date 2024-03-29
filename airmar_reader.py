import io
import json
import time

import pynmea2
import serial
from paho.mqtt import client as mqtt_client

# MQTT configuration
broker = "x.x.x.x"  # Replace with your MQTT broker address
port = 1883  # Replace with your MQTT broker port
client_id = "xxx"  # the client id you want to use, can be anything
username = "xxx"  # your mqtt username
password = "xxx"  # your mqtt password
discovery_prefix = "homeassistant"  # Default discovery prefix for Home Assistant do not change this unless you know what you are doing


def publish_discovery_config(client):
    # Barometric Pressure
    mdab_config = {
        "name": "Barometric Pressure",
        "device_class": "atmospheric_pressure",
        "unique_id": "mda_barometric_pressure",
        "icon": "mdi:weather-tornado",
        "suggested_display_precision": 1,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mdab/state",
        "value_template": "{{ value_json.barometric_pressure }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mdab/config",
        json.dumps(mdab_config),
        retain=True,
    )

    # Temperature
    mdat_config = {
        "name": "Temperature",
        "device_class": "temperature",
        "unique_id": "mda_barometric_temperature",
        "icon": "mdi:temperature-celsius",
        "unit_of_measurement": "°C",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mdat/state",
        "value_template": "{{ value_json.air_temperature }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mdat/config",
        json.dumps(mdat_config),
        retain=True,
    )

    # Relative humidity
    mdah_config = {
        "name": "Relative humidity",
        "device_class": "humidity",
        "unique_id": "mda_barometric_humidity",
        "icon": "mdi:cloud-percent",
        "unit_of_measurement": "%",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mdah/state",
        "value_template": "{{ value_json.rel_humidity }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mdah/config",
        json.dumps(mdah_config),
        retain=True,
    )

    # Dew point
    mdadp_config = {
        "name": "Dew point",
        "device_class": "humidity",
        "unique_id": "mda_barometric_dew_point",
        "icon": "mdi:thermometer-water",
        "unit_of_measurement": "°C",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/mdadp/state",
        "value_template": "{{ value_json.dew_point }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mdadp/config",
        json.dumps(mdadp_config),
        retain=True,
    )

    # Wind Speed
    wsp_config = {
        "name": "Wind Speed",
        "device_class": "wind_speed",
        "unique_id": "wsp",
        "icon": "mdi:windsock",
        "unit_of_measurement": "m/s",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/wsp/state",
        "value_template": "{{ value_json.wind_speed }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mwv_wsp/config",
        json.dumps(wsp_config),
        retain=True,
    )

    # Wind Angle
    wsa_config = {
        "name": "Wind Angle",
        "unique_id": "wsa",
        "icon": "mdi:angle-acute",
        "unit_of_measurement": "∠",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/wsa/state",
        "value_template": "{{ value_json.wind_angle }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mwv_wsa/config",
        json.dumps(wsa_config),
        retain=True,
    )

    # Timestamp
    # timestamp_config = {
    #     "name": "Timestamp",
    #     "device_class": "timestamp",
    #     "unique_id": "timestamp_gps",
    #     "icon": "mdi:weather-sunny",
    #     "suggested_display_precision": 0,
    #     "state_topic": f"{discovery_prefix}/sensor/{client_id}/timestamp/state",
    #     "value_template": "{{ value_json.datetime.time }}",
    #     "device": {
    #         "identifiers": ["airmar150wx"],
    #         "name": "Airmar 150WX Weatherstation",
    #         "model": "150WX",
    #         "manufacturer": "AirMar",
    #     },
    # }
    # client.publish(
    #     f"{discovery_prefix}/sensor/{client_id}/timestamp/config",
    #     json.dumps(timestamp_config),
    #     retain=True,
    # )

    # GPS Day
    gps_day = {
        "name": "GPS Day",
        "unique_id": "gps_day",
        "icon": "mdi:calendar-today",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/day/state",
        "value_template": "{{ value_json.day }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/day/config",
        json.dumps(gps_day),
        retain=True,
    )

    # GPS Month
    gps_month_config = {
        "name": "GPS Month",
        "unique_id": "gps_month",
        "icon": "mdi:calendar-today",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/month/state",
        "value_template": "{{ value_json.month }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/month/config",
        json.dumps(gps_month_config),
        retain=True,
    )

    # GPS Year
    gps_year_config = {
        "name": "GPS Year",
        "unique_id": "gps_year",
        "icon": "mdi:calendar-today",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/year/state",
        "value_template": "{{ value_json.year }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/year/config",
        json.dumps(gps_year_config),
        retain=True,
    )

    # GPS lat
    gps_lat_config = {
        "name": "GPS latitude",
        "unique_id": "gps_lat",
        "icon": "mdi:crosshairs-gps",
        "suggested_display_precision": 4,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/lat/state",
        "value_template": "{{ value_json.lat }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/lat/config",
        json.dumps(gps_lat_config),
        retain=True,
    )

    # GPS lon
    gps_lot_config = {
        "name": "GPS longitude",
        # "device_class": "timestamp",
        "unique_id": "gps_lon",
        "icon": "mdi:crosshairs-gps",
        "suggested_display_precision": 4,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/lon/state",
        "value_template": "{{ value_json.lon }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/lon/config",
        json.dumps(gps_lot_config),
        retain=True,
    )

    # gps_qual
    gps_qual_config = {
        "name": "GPS Quality",
        "unique_id": "gps_qual",
        "icon": "mdi:satellite",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/gps_qual/state",
        "value_template": "{{ value_json.gps_qual }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/gps_qual/config",
        json.dumps(gps_qual_config),
        retain=True,
    )

    # GPS num_sats
    gps_num_sats_config = {
        "name": "GPS Number of Satellites",
        "unique_id": "num_sats",
        "icon": "mdi:satellite-uplink",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/num_sats/state",
        "value_template": "{{ value_json.num_sats }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/num_sats/config",
        json.dumps(gps_num_sats_config),
        retain=True,
    )

    # GPS altitude
    gps_altitude_config = {
        "name": "GPS Altitude",
        "unique_id": "altitude",
        "device_class": "distance",
        "unit_of_measurement": "m",
        "icon": "mdi:altimeter",
        "suggested_display_precision": 1,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/altitude/state",
        "value_template": "{{ value_json.altitude }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/altitude/config",
        json.dumps(gps_altitude_config),
        retain=True,
    )


def weatherparsing(client):
    publish_discovery_config(client)
    ser = serial.Serial("/dev/ttyUSB0", 4800, timeout=5.0)
    sio = io.TextIOWrapper(io.BufferedRWPair(ser, ser))
    print("INFO :: Starting parsing")
    while True:
        try:
            line = sio.readline()
            msg = pynmea2.parse(line)
            # print(msg)
            if msg.sentence_type == "MDA":
                if msg.b_pressure_bar is not None:
                    # Pressure
                    pressure = {"barometric_pressure": float(msg.b_pressure_bar) * 1000}
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
            # if msg.sentence_type == "HDT":
            #     if msg.heading is not None:
            #         hdt = {"heading": float(msg.heading)}
            #         # print(f"Heading {hdt}")
            #         client.publish(
            #             f"{discovery_prefix}/sensor/{client_id}/hdt/state",
            #             json.dumps(hdt),
            #             retain=False,
            #         )
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
                timestamp = {msg.timestamp}
                # print(timestamp, day, month, year)
                day = {"day": msg.day}
                if msg.day is not None:
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/day/state",
                        json.dumps(day),
                        retain=False,
                    )
                month = {"month": msg.month}
                if msg.month is not None:
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/month/state",
                        json.dumps(month),
                        retain=False,
                    )
                year = {"year": msg.year}
                if msg.year is not None:
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/year/state",
                        json.dumps(year),
                        retain=False,
                    )
            # if msg.sentence_type == "VTG":
            #     true_track = {"true_track": msg.true_track}
            #     mag_track = {"mag_track": msg.mag_track}
            #     spd_over_grnd_kmph = {"spd_over_grnd_kmph": msg.spd_over_grnd_kmph}
            #     print(true_track, mag_track, spd_over_grnd_kmph)
            #     client.publish(
            #         f"{discovery_prefix}/sensor/{client_id}/timestamp/state",
            #         json.dumps(timestamp),
            #         retain=False,
            #     )
            if msg.sentence_type == "GGA":
                # print(lat, lon, gps_qual, num_sats, altitude)
                if msg.lat and msg.lat_dir and msg.lon and msg.lon_dir:
                    # Omzetten van breedtegraad naar decimale graden
                    lat_degrees = float(msg.lat[:2])
                    lat_minutes = float(msg.lat[2:])
                    lat_decimal = lat_degrees + (lat_minutes / 60)
                    if msg.lat_dir == "S":  # Zuidelijke breedtegraden zijn negatief
                        lat_decimal = -lat_decimal

                    # Omzetten van lengtegraad naar decimale graden
                    lon_degrees = float(msg.lon[:3])
                    lon_minutes = float(msg.lon[3:])
                    lon_decimal = lon_degrees + (lon_minutes / 60)
                    if msg.lon_dir == "W":  # Westelijke lengtegraden zijn negatief
                        lon_decimal = -lon_decimal

                    # Publiceren van de omgezette waarden
                    lat = {"lat": lat_decimal}
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/lat/state",
                        json.dumps(lat),
                        retain=False,
                    )

                    lon = {"lon": lon_decimal}
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/lon/state",
                        json.dumps(lon),
                        retain=False,
                    )
                gps_qual = {"gps_qual": msg.gps_qual}
                if msg.gps_qual is not None:
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/gps_qual/state",
                        json.dumps(gps_qual),
                        retain=False,
                    )
                num_sats = {"num_sats": msg.num_sats}
                if msg.num_sats is not None:
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/num_sats/state",
                        json.dumps(num_sats),
                        retain=False,
                    )
                altitude = {"altitude": msg.altitude}
                if msg.altitude is not None:
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/altitude/state",
                        json.dumps(altitude),
                        retain=False,
                    )

        except serial.SerialException as e:
            print(f"ERROR :: Device error: {e}")
            break
        except pynmea2.ParseError as e:
            print(f"ERROR :: Parse error: {e}")
            continue
        except UnicodeDecodeError as e:
            print(f"ERROR :: Unicode Decode error: {e}")
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
