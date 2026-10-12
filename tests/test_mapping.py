import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import bridge  # noqa: E402


def timmerflotte(rel="r1"):
    common = {"model": "TIMMERFLOTTE temp/hmd sensor", "batteryPercentage": 58}
    return [
        {"id": f"{rel}_2", "relationId": rel, "type": "sensor", "deviceType": "environmentSensor",
         "isReachable": True, "attributes": {**common, "customName": "TIMMERFLOTTE temp/hmd sensor",
                                             "currentRH": 56, "windowOpen": False}},
        {"id": f"{rel}_1", "relationId": rel, "type": "sensor", "deviceType": "environmentSensor",
         "isReachable": True, "room": {"name": "Kids Room"}, "lastSeen": "x",
         "attributes": {**common, "customName": "Kids Room Sensor", "currentTemperature": 18.78}},
    ]


def test_lux_matches_zigbee_measured_value():
    assert bridge.lux(1) == 1.0
    assert bridge.lux(10001) == 10.0
    assert bridge.lux(40001) == 10000.0
    assert bridge.lux(0) == 0.0


def test_primary_is_the_temperature_half_whatever_the_order():
    devices = timmerflotte()
    assert sorted(devices, key=bridge.primary_rank)[0]["id"] == "r1_1"
    assert sorted(reversed(devices), key=bridge.primary_rank)[0]["id"] == "r1_1"


def test_split_device_exposes_battery_once_and_no_settings():
    secondary, primary = timmerflotte()
    keys = [k for _, k, _ in bridge.entities(primary, True)] + \
           [k for _, k, _ in bridge.entities(secondary, False)]
    assert sorted(keys) == ["battery", "humidity", "temperature"]


def test_state_payload_excludes_last_seen_and_settings():
    _, primary = timmerflotte()
    assert bridge.state_payload(primary) == {
        "isReachable": True, "currentTemperature": 18.78, "batteryPercentage": 58}


def test_binary_sensors_render_on_off():
    door = {"id": "d_1", "type": "sensor", "deviceType": "openCloseSensor",
            "attributes": {"isOpen": True, "batteryPercentage": 90}}
    (component, key, cfg), _ = list(bridge.entities(door, True))
    assert (component, key) == ("binary_sensor", "contact")
    assert cfg["value_template"] == "{{ 'ON' if value_json.isOpen else 'OFF' }}"


def test_light_discovery_normalises_inverted_kelvin_range():
    light = {"id": "l_1", "type": "light", "deviceType": "light", "isReachable": True,
             "capabilities": {"canReceive": ["customName", "isOn", "lightLevel", "colorTemperature"]},
             "attributes": {"isOn": True, "lightLevel": 100, "colorTemperature": 2702,
                            "colorTemperatureMin": 4000, "colorTemperatureMax": 2202}}
    (component, _, cfg), = list(bridge.entities(light, True))
    assert component == "light"
    assert (cfg["min_kelvin"], cfg["max_kelvin"]) == (2202, 4000)
    assert cfg["supported_color_modes"] == ["color_temp"]
    assert bridge.state_payload(light)["state"] == "ON"


class FakeHub:
    def __init__(self):
        self.calls = []

    def set_attributes(self, device_id, attributes):
        self.calls.append(attributes)


def test_light_command_sends_one_attribute_per_patch_in_order():
    b = bridge.Bridge.__new__(bridge.Bridge)
    b.hub, b.lock = FakeHub(), __import__("threading").RLock()
    b.devices = {"l_1": {"id": "l_1", "type": "light"}}
    b.handle_command("l_1", '{"state": "ON", "brightness": 0, "color_temp": 2700}')
    assert b.hub.calls == [{"isOn": True}, {"lightLevel": 1}, {"colorTemperature": 2700}]
    b.hub.calls.clear()
    b.handle_command("l_1", '{"state": "OFF", "brightness": 50}')
    assert b.hub.calls == [{"isOn": False}]
