from asyncio import create_task
from collections import defaultdict
import datetime
import logging

from .const import DOMAIN
from .data_formats import DataFormats
from ruuvi_decoders import get_decoder

import voluptuous as vol
import homeassistant.helpers.config_validation as cv
from homeassistant.components.bluetooth import async_register_callback, async_get_scanner, BluetoothServiceInfoBleak, BluetoothScanningMode, BluetoothChange

from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.helpers.entity import Entity
from homeassistant.util import dt
from homeassistant.components import HomeAssistant
from homeassistant.const import (
    CONF_MONITORED_CONDITIONS,
    CONF_NAME, CONF_MAC, CONF_SENSORS, STATE_UNKNOWN,
    TEMP_CELSIUS, PERCENTAGE, PRESSURE_HPA
)

_LOGGER = logging.getLogger(__name__)

MANUFACTURER_ID = 0x0499
MAX_UPDATE_FREQUENCY = 'max_update_frequency'

DEFAULT_FORCE_UPDATE = False
DEFAULT_UPDATE_FREQUENCY = 10
DEFAULT_NAME = 'RuuviTag'

MILI_G = "cm/s2"
MILI_VOLT = "mV"
DBM = "dBm"

# Sensor types are defined like: Name, units
SENSOR_TYPES = {
    'temperature': ['Temperature', TEMP_CELSIUS],
    'humidity': ['Humidity', PERCENTAGE],
    'pressure': ['Pressure', PRESSURE_HPA],
    'acceleration': ['Acceleration', MILI_G],
    'acceleration_x': ['X Acceleration', MILI_G],
    'acceleration_y': ['Y Acceleration', MILI_G],
    'acceleration_z': ['Z Acceleration', MILI_G],
    'battery': ['Battery voltage', MILI_VOLT],
    'movement_counter': ['Movement counter', 'count'],
    'rssi': ['Received Signal Strength Indicator', DBM]
}


PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_SENSORS): vol.All(
                cv.ensure_list,
                [
                    vol.Schema(
                        {
                            vol.Required(CONF_MAC): cv.string,
                            vol.Optional(CONF_NAME): cv.string,
                            vol.Optional(
                                CONF_MONITORED_CONDITIONS,
                                default=list(SENSOR_TYPES)): vol.All(
                                    cv.ensure_list,
                                    [vol.In(SENSOR_TYPES)]),
                        }
                    )
                ],
        ),
        vol.Optional(MAX_UPDATE_FREQUENCY, default=DEFAULT_UPDATE_FREQUENCY): cv.positive_int
    }
)

ruuvi_subscriber = None

async def get_sensor_set(hass: HomeAssistant, config):
    """Get a list of Sensor entities from a config entry."""

    mac_addresses = [resource[CONF_MAC].upper() for resource in config[CONF_SENSORS]]
    if not isinstance(mac_addresses, list):
        mac_addresses = [mac_addresses]

    sensors = []

    for resource in config[CONF_SENSORS]:
        mac_address = resource[CONF_MAC].upper()
        default_name = "Ruuvitag " + mac_address.replace(":","").lower()
        name = resource.get(CONF_NAME, default_name)
        for condition in resource[CONF_MONITORED_CONDITIONS]:
            sensors.append(
              RuuviSensor(
                hass, mac_address, name, condition,
                config.get(MAX_UPDATE_FREQUENCY)
              )
            )
    return sensors

async def async_setup_entry(hass: HomeAssistant, config_entry, async_add_entities, discovery_info=None):
    """Set up ruuvi from a config entry."""
    global ruuvi_subscriber
    sensors = await get_sensor_set(hass, config_entry.data)
    sensors_to_add = ruuvi_subscriber.update_sensors(sensors)
    async_add_entities(sensors_to_add)
    create_task(ruuvi_subscriber.start())


async def async_setup_platform(hass: HomeAssistant, config, async_add_entities, discovery_info=None):
    """Set up ruuvi from a config entry."""
    global ruuvi_subscriber
    sensors = await get_sensor_set(hass, config)
    ruuvi_subscriber = RuuviSubscriber(hass, sensors)
    async_add_entities(sensors)
    ruuvi_subscriber.update_sensors(sensors)
    create_task(ruuvi_subscriber.start())


class RuuviSensor(Entity):
    def __init__(self, hass: HomeAssistant, mac_address, tag_name, sensor_type, max_update_frequency):
        self.hass = hass
        self.mac_address = mac_address
        self.tag_name = tag_name
        self.sensor_type = sensor_type
        self.max_update_frequency = max_update_frequency
        self.update_time = dt.utcnow() - datetime.timedelta(days=360)
        self._state = STATE_UNKNOWN

    @property
    def name(self):
        return f"{self.tag_name} {self.sensor_type}"

    @property
    def should_poll(self):
        return False

    @property
    def state(self):
        return self._state

    @property
    def unit_of_measurement(self):
        return SENSOR_TYPES[self.sensor_type][1]

    @property
    def unique_id(self):
      return f"ruuvi.{self.mac_address}.{self.sensor_type}"

    def set_state(self, state):
        last_updated_seconds_ago = (dt.utcnow() - self.update_time) / datetime.timedelta(seconds=1)

        self._state = state

        if last_updated_seconds_ago < self.max_update_frequency:
          _LOGGER.debug(f"Updated throttled ({last_updated_seconds_ago} elapsed): {self.name}")
          return
        else:
          _LOGGER.debug(f"Updating {self.update_time} {self.name}: {self.state}")
          self.update_time = dt.utcnow()
          self.async_schedule_update_ha_state()


class RuuviSubscriber(object):
    """
    Subscribes to a set of Ruuvi tags and update Hass sensors whenever a
    new value is received.
    """

    def __init__(self, hass: HomeAssistant, sensors: list[RuuviSensor]):
        self.hass = hass
        self.deregister_callback = None
        self.sensors: RuuviSensor = sensors
        self.entities_by_mac_address: dict[str, list[RuuviSensor]] = defaultdict(list)
        self.mac_blocklist = []
        for sensor in self.sensors:
            self.entities_by_mac_address[sensor.mac_address].append(sensor)

    async def start(self):
        self.deregister_callback = async_register_callback(
            self.hass,
            self.handle_callback,
            match_dict=None,
            mode=BluetoothScanningMode.ACTIVE
        )

    def stop(self):
        if self.deregister_callback:
            self.deregister_callback()

    def update_sensors(self, sensors):
      self.sensors = sensors
      self.entities_by_mac_address = defaultdict(list)
      for sensor in self.sensors:
          self.entities_by_mac_address[sensor.mac_address].append(sensor)
      return sensors

    def handle_callback(self, bt_info: BluetoothServiceInfoBleak, bt_change: BluetoothChange):
        if not bt_info.address in self.entities_by_mac_address or bt_info.address in self.mac_blocklist:
            _LOGGER.debug(f"Ignoring {bt_change} from {bt_info.address}")
            return
        if MANUFACTURER_ID not in bt_info.manufacturer_data:
            _LOGGER.warn(f"Ignoring {bt_change} from {bt_info.address}: Did not contain manufacturer data")
            return
        _LOGGER.debug(bt_info)
        (data_format, converted_raw_data) = DataFormats.convert_data(bt_info.manufacturer_data[MANUFACTURER_ID].hex())
        if not converted_raw_data:
            if bt_info.address:
                _LOGGER.warn(f"Did not recognize the data format. Adding {bt_info.address} to blocklist")
                self.mac_blocklist.append(bt_info.address)
            return

        decoded_data = get_decoder(data_format).decode_data(converted_raw_data)
        _LOGGER.debug(decoded_data)
        decoded_data["rssi"] = bt_info.rssi
        for sensor in self.sensors:
            if sensor.sensor_type in decoded_data.keys():
                sensor.set_state(decoded_data[sensor.sensor_type])
