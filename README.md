# dirigera2mqtt

Bridges an IKEA DIRIGERA hub to MQTT with Home Assistant discovery. It uses the hub's local REST API and websocket, the same ones the IKEA Home smart app uses; no cloud is involved.

- **One Home Assistant device per physical device.** Sub-devices that share a `relationId` are grouped. For example, each TIMMERFLOTTE becomes a single device with Temperature, Humidity and Battery entities, named after the half that has your name and room, whatever order the hub lists them in.
- **Push updates.** The bridge follows the hub's websocket, re-syncs over REST on startup, after every reconnect, and every `RESYNC_SECONDS`, and only publishes when a value actually changes.
- **Availability** is the bridge LWT combined with the hub's per-device `isReachable`, so a device with a dead battery shows as unavailable instead of keeping stale values.

| Hub device | Home Assistant entities |
|---|---|
| environmentSensor (TIMMERFLOTTE, VINDSTYRKA, ALPSTUGA) | temperature, humidity, CO2, PM2.5, battery |
| openCloseSensor (PARASOLL) | `binary_sensor` opening, battery |
| motionSensor (VALLHORN) / occupancySensor (MYGGSPRAY) | `binary_sensor` motion/occupancy, illuminance, battery |
| light | `light` (on/off, brightness, colour temperature, hue/saturation where supported) |
| outlet | `switch`, power, energy |
| lightController (remotes) | battery |

**About illuminance:** the hub reports the raw Zigbee/Matter `MeasuredValue` (`10000·log10(lux)+1`, from 1 to 40001). The bridge converts it to lux and keeps the raw value as the `raw` attribute.

## Setup

```sh
cp .env.example .env               # set DIRIGERA_HOST, MQTT_HOST and your MQTT credentials
mkdir -p data                      # the container runs as uid 1000 and writes the token here
docker compose pull
docker compose run --rm dirigera2mqtt pair    # press the button on the bottom of the hub
docker compose up -d
```

The image is `ghcr.io/jamesbiederbeck/dirigera2mqtt`, built for amd64 and arm64 by GitHub Actions. `latest` tracks `main`, and `v*` tags publish matching version tags. To build from source instead, uncomment `build: .` in `docker-compose.yml` and run `docker compose build`.

`pair` writes the hub token to `./data/token`, readable by the owner only. If you already have a token, either put it in that file or set `DIRIGERA_TOKEN`. If the hub ever rejects the token, the bridge stops with exit code 2 and a message telling you to pair again, rather than retrying forever.

## MQTT topics

- `dirigera/bridge/status`: `online` or `offline` (retained, and used as the LWT)
- `dirigera/<device id>/state`: JSON state for one hub device (retained)
- `dirigera/<device id>/set`: commands. Use `ON`/`OFF` for outlets, or Home Assistant JSON-schema light payloads such as `{"state":"ON","brightness":40,"color_temp":2700}`
- `homeassistant/<component>/dirigera_<device id>/<key>/config`: discovery. When Home Assistant publishes `online` on `homeassistant/status`, the bridge re-sends it.

## Notes

- If the HACS `dirigera_platform` integration or a Matter bridge also exposes these devices, you'll see duplicate entities. Disable one of them.
- Light commands go to the hub one attribute per request (on, then level, then temperature), the same way the `dirigera` Python library sends them.

## Tests

```sh
pip install -r requirements.txt pytest && pytest -q tests
```
