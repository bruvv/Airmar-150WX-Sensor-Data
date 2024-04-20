import io
import json
import logging
import subprocess
import time
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

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

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/var/log/airmar_reader.log"),
        RotatingFileHandler(
            "/var/log/airmar_reader.log", maxBytes=1048576, backupCount=3
        ),
        logging.StreamHandler(),
    ],
)


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

    # Wind Speed 5 min gemiddelde
    wspd_config = {
        "name": "Wind Speed 5 min average",
        "device_class": "wind_speed",
        "unique_id": "wspm",
        "icon": "mdi:windsock",
        "unit_of_measurement": "m/s",
        "suggested_display_precision": 2,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/wspmean5min/state",
        "value_template": "{{ value_json.wind_speed_mean_5min }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mwv_wspmean5min/config",
        json.dumps(wspd_config),
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

    # Wind Angle 5 min gemiddelde
    wspaa_config = {
        "name": "Wind Angle 5 min average",
        "unique_id": "wspaa",
        "icon": "mdi:angle-acute",
        "unit_of_measurement": "∠",
        "suggested_display_precision": 0,
        "state_topic": f"{discovery_prefix}/sensor/{client_id}/wspaamean5min/state",
        "value_template": "{{ value_json.wind_angle_mean_5min }}",
        "device": {
            "identifiers": ["airmar150wx"],
            "name": "Airmar 150WX Weatherstation",
            "model": "150WX",
            "manufacturer": "AirMar",
        },
    }
    client.publish(
        f"{discovery_prefix}/sensor/{client_id}/mwv_wspaamean5min/config",
        json.dumps(wspaa_config),
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
    logging.info("Starting parsing")

    wind_speed_sum = 0
    wind_speed_samplecount = 0
    previousWindSpeedTimer = datetime.now()
    wind_angle_sum = 0
    wind_angle_samplecount = 0
    previousWindAngleTimer = datetime.now()

    while True:
        try:
            line = sio.readline()
            msg = pynmea2.parse(line)
            # print(msg)
            if msg.sentence_type == "MDA":
                if msg.b_pressure_bar is not None:
                    # Pressure
                    pressure = {
                        "barometric_pressure": round(
                            float(msg.b_pressure_bar) * 1000, 2
                        )
                    }
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
                        "wind_speed": round(float(msg.wind_speed) * 0.514444, 2)
                    }  # convert from kn to ms
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/wsp/state",
                        json.dumps(wsp),
                        retain=False,
                    )
                    # Convert from knots to meters per second and store the value
                    wind_speed_mps = float(msg.wind_speed) * 0.514444
                    wind_speed_sum += wind_speed_mps
                    wind_speed_samplecount += 1

                    now = datetime.now()

                    if now - previousWindSpeedTimer > timedelta(minutes=5):
                        previousWindSpeedTimer = now

                        avg_windspeed = wind_speed_sum / wind_speed_samplecount
                        logging.debug(
                            f"{datetime.now()} average wind snelheid, {round(avg_windspeed, 2)}"
                        )

                        mean_wsp = {"wind_speed_mean_5min": round(avg_windspeed, 2)}
                        client.publish(
                            f"{discovery_prefix}/sensor/{client_id}/wspmean5min/state",
                            json.dumps(mean_wsp),
                            retain=False,
                        )

                        # reset the timer
                        wind_speed_sum = 0
                        wind_speed_samplecount = 0
                        previousWindSpeedTimer = now

                if msg.wind_angle is not None:
                    wsa = {"wind_angle": float(msg.wind_angle)}
                    client.publish(
                        f"{discovery_prefix}/sensor/{client_id}/wsa/state",
                        json.dumps(wsa),
                        retain=False,
                    )
                    wind_angle_mps = float(msg.wind_angle)
                    wind_angle_sum += wind_angle_mps
                    wind_angle_samplecount += 1

                    now = datetime.now()

                    if now - previousWindAngleTimer > timedelta(minutes=5):
                        previousWindAngleTimer = now

                        avg_windangle = wind_angle_sum / wind_angle_samplecount

                        logging.debug(
                            f"{datetime.now()} average wind angle, {round(avg_windangle, 0)}"
                        )
                        mean_wsaa = {"wind_angle_mean_5min": round(avg_windangle, 0)}
                        client.publish(
                            f"{discovery_prefix}/sensor/{client_id}/wspaamean5min/state",
                            json.dumps(mean_wsaa),
                            retain=False,
                        )

                        # reset the timer
                        wind_angle_sum = 0
                        wind_angle_samplecount = 0
                        previousWindAngleTimer = now

            if msg.sentence_type == "ZDA":
                # timestamp = {msg.timestamp}
                # print(timestamp)
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

                    lat_decimal = round(lat_decimal, 5)

                    # Omzetten van lengtegraad naar decimale graden
                    lon_degrees = float(msg.lon[:3])
                    lon_minutes = float(msg.lon[3:])
                    lon_decimal = lon_degrees + (lon_minutes / 60)
                    if msg.lon_dir == "W":  # Westelijke lengtegraden zijn negatief
                        lon_decimal = -lon_decimal

                    lon_decimal = round(lon_decimal, 5)

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
            logging.error(f"Device error: {e}")
            break
        except pynmea2.ParseError as e:
            logging.error(f"Parse error: {e}")
            continue
        except UnicodeDecodeError as e:
            logging.error(f"Unicode Decode error: {e}")
            break


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker")
        else:
            logging.error("Connection failed:", rc)
            exit(0)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.connect(broker, port)
    client.on_connect = on_connect
    return client


def check_internet(retry_interval=5, total_wait_time=30):
    end_time = time.time() + total_wait_time
    while time.time() < end_time:
        try:
            # Probeert de host te pingen.
            response = subprocess.run(
                ["ping", "-c", "1", broker],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if response.returncode == 0:
                logging.info(f"MQTT broker connection {broker} detected.")
                return True
            else:
                logging.error(
                    f"No MQTT broker connection to {broker} detected. Retrying..."
                )
        except Exception as e:
            logging.error(f"Ping error: {e}")
        time.sleep(retry_interval)
    logging.error("Failed to connect to MQTT broker.")
    return False


def run():
    try:
        client = connect_mqtt()
        client.loop_start()
        while True:
            weatherparsing(client)
    except KeyboardInterrupt:
        logging.info("Program interrupted. Exiting...")
        client.loop_stop()
        time.sleep(1)
        client.disconnect()
        time.sleep(1)
        exit(0)


if __name__ == "__main__":
    if check_internet():
        run()
    else:
        logging.error("No mqtt connection, cannot start program")
