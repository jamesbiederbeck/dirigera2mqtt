#!/usr/bin/env python3
"""IKEA DIRIGERA -> MQTT bridge with Home Assistant discovery.

Reads devices over the hub's local REST API, follows changes over its
websocket, and publishes one Home Assistant device per physical device
(sub-devices sharing a relationId, e.g. TIMMERFLOTTE temperature + humidity,
are grouped). Lights and outlets accept commands.

    bridge.py          run the bridge
    bridge.py pair     pair with the hub (press its action button) and save the token
"""
import base64
import copy
import hashlib
import json
import logging
import os
import secrets
import socket
import ssl
import sys
import threading
import time

import paho.mqtt.client as mqtt
import requests
import urllib3
import websocket

urllib3.disable_warnings()  # the hub serves a self-signed certificate
log = logging.getLogger("dirigera2mqtt")

HUB_HOST = os.environ.get("DIRIGERA_HOST", "")
TOKEN_FILE = os.environ.get("DIRIGERA_TOKEN_FILE", "/data/token")
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME") or None
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD") or None
PREFIX = os.environ.get("MQTT_PREFIX", "dirigera")
DISCOVERY = os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant")
RESYNC_SECONDS = int(os.environ.get("RESYNC_SECONDS", "300"))
BRIDGE_STATUS = f"{PREFIX}/bridge/status"


class AuthError(Exception):
    pass


# --------------------------------------------------------------------------- hub

class Hub:
    def __init__(self, host, token):
        self.base = f"https://{host}:8443/v1"
        self.ws_url = f"wss://{host}:8443/v1"
        self.headers = {"Authorization": f"Bearer {token}"}

    def _check(self, r):
        if r.status_code == 401:
            raise AuthError("hub rejected the token (401); re-run `bridge.py pair`")
        r.raise_for_status()
        return r

    def devices(self):
        return self._check(requests.get(f"{self.base}/devices", headers=self.headers,
                                        verify=False, timeout=15)).json()

    def set_attributes(self, device_id, attributes):
        self._check(requests.patch(f"{self.base}/devices/{device_id}", headers=self.headers,
                                   json=[{"attributes": attributes}], verify=False, timeout=15))


def pair(host, token_file):
    """OAuth PKCE pairing, the same flow the IKEA Home smart app uses."""
    # Check before asking for a button press, not after the hub hands over a token.
    token_dir = os.path.dirname(os.path.abspath(token_file))
    os.makedirs(token_dir, exist_ok=True)
    if not os.access(token_dir, os.W_OK):
        sys.exit(f"{token_dir} is not writable by uid {os.getuid()}; "
                 f"on the host run: mkdir -p data && sudo chown {os.getuid()} data")
    base = f"https://{host}:8443/v1"
    verifier = secrets.token_urlsafe(96)[:128]
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    code = requests.get(f"{base}/oauth/authorize", verify=False, timeout=10, params={
        "audience": "homesmart.local", "response_type": "code",
        "code_challenge": challenge, "code_challenge_method": "S256"}).json()["code"]
    print("Press the action button on the bottom of the DIRIGERA hub now...", flush=True)
    for _ in range(60):
        r = requests.post(f"{base}/oauth/token", verify=False, timeout=10, data={
            "code": code, "name": f"dirigera2mqtt-{socket.gethostname()}",
            "grant_type": "authorization_code", "code_verifier": verifier})
        if r.ok:
            os.makedirs(os.path.dirname(token_file) or ".", exist_ok=True)
            with os.fdopen(os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
                f.write(r.json()["access_token"])
            print(f"Paired; token written to {token_file}")
            return
        time.sleep(1)
    sys.exit("No button press within 60s; run pair again")


# ------------------------------------------------------------------- mapping

def lux(raw):
    """Zigbee/Matter MeasuredValue is 10000*log10(lux)+1 (hub reports min 1, max 40001)."""
    return round(10 ** ((raw - 1) / 10000), 1) if raw and raw > 0 else 0.0


def primary_rank(device):
    """Same rule as the dirigera_platform fix: temperature half, then room, then _1."""
    a = device.get("attributes") or {}
    return (a.get("currentTemperature") is None, not device.get("room"),
            not device.get("id", "").endswith("_1"), device.get("id", ""))


# (attribute, component, key, config); only emitted when the attribute is present.
SENSORS = [
    ("currentTemperature", "sensor", "temperature",
     {"name": "Temperature", "device_class": "temperature", "unit_of_measurement": "°C",
      "state_class": "measurement", "suggested_display_precision": 1}),
    ("currentRH", "sensor", "humidity",
     {"name": "Humidity", "device_class": "humidity", "unit_of_measurement": "%",
      "state_class": "measurement"}),
    ("currentCO2", "sensor", "co2",
     {"name": "CO2", "device_class": "carbon_dioxide", "unit_of_measurement": "ppm",
      "state_class": "measurement"}),
    ("currentPM25", "sensor", "pm25",
     {"name": "PM2.5", "device_class": "pm25", "unit_of_measurement": "µg/m³",
      "state_class": "measurement"}),
    ("illuminance", "sensor", "illuminance",
     {"name": "Illuminance", "device_class": "illuminance", "unit_of_measurement": "lx",
      "state_class": "measurement"}),
    ("isOpen", "binary_sensor", "contact",
     {"name": "Contact", "device_class": "opening"}),
    ("currentActivePower", "sensor", "power",
     {"name": "Power", "device_class": "power", "unit_of_measurement": "W",
      "state_class": "measurement"}),
    ("totalEnergyConsumed", "sensor", "energy",
     {"name": "Energy", "device_class": "energy", "unit_of_measurement": "kWh",
      "state_class": "total_increasing"}),
]
# Attributes that are settings the hub accepts, not readings; never exposed.
SETTINGS = {"windowOpen", "sensorConfig", "circadianPresets", "startupOnOff",
            "startUpCurrentLevel", "startupTemperature", "deviceOnBehavior"}


def state_payload(device):
    """Flat JSON published on <prefix>/<id>/state. Excludes lastSeen so it only changes on real changes."""
    a = device.get("attributes") or {}
    p = {"isReachable": bool(device.get("isReachable"))}
    for attr, *_ in SENSORS:
        if attr in a and attr not in SETTINGS:
            p[attr] = lux(a[attr]) if attr == "illuminance" else a[attr]
    if "illuminance" in a:
        p["illuminance_raw"] = a["illuminance"]
    if "batteryPercentage" in a:
        p["batteryPercentage"] = a["batteryPercentage"]
    if device.get("deviceType") in ("motionSensor", "occupancySensor") and "isDetected" in a:
        p["isDetected"] = a["isDetected"]
    if device.get("type") in ("light", "outlet") and "isOn" in a:
        p["state"] = "ON" if a["isOn"] else "OFF"
    if device.get("type") == "light":
        if "lightLevel" in a:
            p["brightness"] = a["lightLevel"]
        if "colorTemperature" in a:
            p["color_temp"] = a["colorTemperature"]
        if "colorHue" in a and "colorSaturation" in a:
            p["color"] = {"h": a["colorHue"], "s": round(a["colorSaturation"] * 100, 1)}
        p["color_mode"] = _light_color_mode(device)
    return p


def _light_modes(device):
    can = set((device.get("capabilities") or {}).get("canReceive") or [])
    modes = []
    if "colorTemperature" in can:
        modes.append("color_temp")
    if "colorHue" in can:
        modes.append("hs")
    if not modes:
        modes.append("brightness" if "lightLevel" in can else "onoff")
    return modes


def _light_color_mode(device):
    modes = _light_modes(device)
    if "hs" in modes and (device.get("attributes") or {}).get("colorMode") == "color":
        return "hs"
    return modes[0]


def entities(device, is_primary):
    """Yield (component, key, config) for one sub-device."""
    a = device.get("attributes") or {}
    dtype, typ = device.get("deviceType"), device.get("type")
    if dtype == "gateway":
        return
    if typ == "light":
        cfg = {"name": None, "schema": "json", "brightness": "lightLevel" in a,
               "brightness_scale": 100, "supported_color_modes": _light_modes(device),
               "command_topic": f"{PREFIX}/{device['id']}/set"}
        if "color_temp" in cfg["supported_color_modes"]:
            lo, hi = sorted((a.get("colorTemperatureMin", 2200), a.get("colorTemperatureMax", 4000)))
            cfg.update({"color_temp_kelvin": True, "min_kelvin": lo, "max_kelvin": hi})
        yield "light", "light", cfg
    elif typ == "outlet":
        yield "switch", "switch", {"name": None, "command_topic": f"{PREFIX}/{device['id']}/set",
                                   "value_template": "{{ value_json.state }}"}
    if dtype in ("motionSensor", "occupancySensor") and "isDetected" in a:
        yield "binary_sensor", dtype, {
            "name": "Motion" if dtype == "motionSensor" else "Occupancy",
            "device_class": "motion" if dtype == "motionSensor" else "occupancy",
            "value_template": "{{ 'ON' if value_json.isDetected else 'OFF' }}"}
    for attr, component, key, cfg in SENSORS:
        if attr in a:
            if component == "binary_sensor":
                template = f"{{{{ 'ON' if value_json.{attr} else 'OFF' }}}}"
            else:
                template = f"{{{{ value_json.{attr} }}}}"
            cfg = dict(cfg, value_template=template)
            if attr == "illuminance":
                cfg["json_attributes_topic"] = f"{PREFIX}/{device['id']}/state"
                cfg["json_attributes_template"] = '{{ {"raw": value_json.illuminance_raw} | tojson }}'
            yield component, key, cfg
    # Every half of a split device reports the same battery; expose it once.
    if is_primary and "batteryPercentage" in a:
        yield "sensor", "battery", {"name": "Battery", "device_class": "battery",
                                    "unit_of_measurement": "%", "state_class": "measurement",
                                    "entity_category": "diagnostic",
                                    "value_template": "{{ value_json.batteryPercentage }}"}


# ------------------------------------------------------------------- bridge

class Bridge:
    def __init__(self, hub):
        self.hub = hub
        self.lock = threading.RLock()
        self.devices = {}          # id -> device dict (REST snapshot + websocket updates)
        self.last_state = {}       # id -> last published state JSON
        self.config_topics = set()
        self.failure = []          # AuthError raised on any thread stops the bridge
        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"dirigera2mqtt-{socket.gethostname()}")
        if MQTT_USERNAME:
            self.mqtt.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.mqtt.will_set(BRIDGE_STATUS, "offline", retain=True)
        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message
        self.mqtt.reconnect_delay_set(1, 60)

    # ---- mqtt
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("MQTT connect failed: %s", reason_code)
            return
        log.info("MQTT connected to %s:%s", MQTT_HOST, MQTT_PORT)
        client.subscribe([(f"{PREFIX}/+/set", 0), (f"{DISCOVERY}/status", 0)])
        client.publish(BRIDGE_STATUS, "online", retain=True)
        if self.devices:
            self.publish_all(force=True)

    def _on_message(self, client, userdata, msg):
        if msg.topic == f"{DISCOVERY}/status":
            if msg.payload == b"online":  # Home Assistant restarted
                self.publish_all(force=True)
            return
        device_id = msg.topic.split("/")[-2]
        try:
            self.handle_command(device_id, msg.payload.decode())
        except AuthError as e:
            self.failure.append(e)
        except Exception:
            log.exception("command for %s failed: %r", device_id, msg.payload)

    def handle_command(self, device_id, payload):
        with self.lock:
            device = self.devices.get(device_id)
        if not device or device.get("type") not in ("light", "outlet"):
            log.warning("ignoring command for unknown/non-controllable device %s", device_id)
            return
        if payload in ("ON", "OFF"):
            cmd = {"state": payload}
        else:
            cmd = json.loads(payload)
        # The hub takes one attribute per PATCH (like the dirigera library); order matters.
        if cmd.get("state") == "OFF":
            self.hub.set_attributes(device_id, {"isOn": False})
            return
        if cmd.get("state") == "ON":
            self.hub.set_attributes(device_id, {"isOn": True})
        if "brightness" in cmd:
            self.hub.set_attributes(device_id, {"lightLevel": max(1, min(100, int(cmd["brightness"])))})
        if "color_temp" in cmd:
            self.hub.set_attributes(device_id, {"colorTemperature": int(cmd["color_temp"])})
        if "color" in cmd and "h" in cmd["color"]:
            self.hub.set_attributes(device_id, {"colorHue": float(cmd["color"]["h"]),
                                                "colorSaturation": float(cmd["color"]["s"]) / 100})

    # ---- publishing
    def _groups(self):
        groups = {}
        for d in self.devices.values():
            groups.setdefault(d.get("relationId") or d["id"], []).append(d)
        return {k: sorted(v, key=primary_rank) for k, v in groups.items()}

    def publish_all(self, force=False):
        with self.lock:
            topics = set()
            for group_id, members in self._groups().items():
                primary = members[0]
                pa = primary.get("attributes") or {}
                ha_device = {"identifiers": [f"dirigera_{group_id}"], "manufacturer": "IKEA",
                             "name": pa.get("customName") or pa.get("model") or group_id,
                             "model": pa.get("model"), "sw_version": pa.get("firmwareVersion")}
                if (primary.get("room") or {}).get("name"):
                    ha_device["suggested_area"] = primary["room"]["name"]
                for d in members:
                    for component, key, cfg in entities(d, d is primary):
                        topic = f"{DISCOVERY}/{component}/dirigera_{d['id']}/{key}/config"
                        cfg = dict(cfg, unique_id=f"dirigera_{d['id']}_{key}",
                                   state_topic=f"{PREFIX}/{d['id']}/state", device=ha_device,
                                   availability_mode="all", availability=[
                                       {"topic": BRIDGE_STATUS},
                                       {"topic": f"{PREFIX}/{d['id']}/state",
                                        "value_template": "{{ 'online' if value_json.isReachable else 'offline' }}"}])
                        self.mqtt.publish(topic, json.dumps(cfg), retain=True)
                        topics.add(topic)
                    self.publish_state(d, force)
            for stale in self.config_topics - topics:  # device removed from the hub
                self.mqtt.publish(stale, "", retain=True)
            self.config_topics = topics

    def publish_state(self, device, force=False):
        payload = json.dumps(state_payload(device), sort_keys=True)
        if force or self.last_state.get(device["id"]) != payload:
            self.mqtt.publish(f"{PREFIX}/{device['id']}/state", payload, retain=True)
            self.last_state[device["id"]] = payload

    # ---- hub sync
    def resync(self):
        devices = self.hub.devices()
        with self.lock:
            self.devices = {d["id"]: d for d in devices}
            self.publish_all()
        log.info("resynced %d devices from hub", len(devices))

    def apply_event(self, event):
        etype, data = event.get("type"), event.get("data") or {}
        if etype in ("deviceAdded", "deviceRemoved", "deviceConfigurationChanged"):
            log.info("%s for %s; resyncing", etype, data.get("id"))
            self.resync()
            return
        if etype != "deviceStateChanged" or "id" not in data:
            return
        with self.lock:
            device = self.devices.get(data["id"])
            if device is None:
                unknown = True
            else:
                unknown = False
                for k, v in data.items():
                    if k == "attributes":
                        device.setdefault("attributes", {}).update(copy.deepcopy(v))
                    elif k != "id":
                        device[k] = copy.deepcopy(v)
                self.publish_state(device)
        if unknown:
            self.resync()

    def websocket_loop(self):
        backoff = 1
        while True:
            try:
                ws = websocket.create_connection(self.hub.ws_url, header=[f"Authorization: {self.hub.headers['Authorization']}"],
                                                 sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=30)
                log.info("hub websocket connected")
                self.resync()  # catch anything missed while disconnected
                backoff = 1
                while True:
                    try:
                        msg = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        ws.ping()
                        continue
                    if msg:
                        self.apply_event(json.loads(msg))
            except AuthError:
                raise
            except websocket.WebSocketBadStatusException as e:
                if e.status_code == 401:
                    raise AuthError("hub rejected the token on the websocket (401); re-run `bridge.py pair`")
                log.warning("hub websocket rejected: %s; retrying in %ss", e, backoff)
            except Exception as e:
                log.warning("hub websocket dropped: %s; retrying in %ss", e, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    def run(self):
        self.mqtt.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.mqtt.loop_start()
        failure = self.failure

        def ws_thread():
            try:
                self.websocket_loop()
            except AuthError as e:
                failure.append(e)

        threading.Thread(target=ws_thread, daemon=True, name="hub-websocket").start()
        last = time.monotonic()
        while not failure:
            time.sleep(1)
            if time.monotonic() - last >= RESYNC_SECONDS:
                last = time.monotonic()
                try:
                    self.resync()
                except AuthError as e:
                    failure.append(e)
                except Exception as e:
                    log.warning("periodic resync failed: %s", e)
        self.mqtt.publish(BRIDGE_STATUS, "offline", retain=True).wait_for_publish(5)
        self.mqtt.loop_stop()
        log.error("%s", failure[0])
        sys.exit(2)


def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not HUB_HOST:
        sys.exit("DIRIGERA_HOST is required")
    if sys.argv[1:] == ["pair"]:
        pair(HUB_HOST, TOKEN_FILE)
        return
    token = os.environ.get("DIRIGERA_TOKEN")
    if not token:
        try:
            token = open(TOKEN_FILE).read().strip()
        except FileNotFoundError:
            sys.exit(f"No token at {TOKEN_FILE}; run `bridge.py pair` first or set DIRIGERA_TOKEN")
    Bridge(Hub(HUB_HOST, token)).run()


if __name__ == "__main__":
    main()
