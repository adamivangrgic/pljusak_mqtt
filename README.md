docker run -d \
  --name pljusak \
  --restart unless-stopped \
  -e LOCATION="Zadar" \
  -e RADIUS_KM="5" \
  -e POLL_S="60" \
  -e MAX_AGE_MIN="15" \
  -e GUST_MODE="max" \
  -e DISPLAY_TZ="Europe/Zagreb" \
  -e MQTT_HOST="192.168.1.10" \
  -e MQTT_PORT="1883" \
  -e MQTT_USER="ha" \
  -e MQTT_PASSWORD="your_mqtt_password" \
  -e MQTT_TLS="false" \
  -e MQTT_DISCOVERY_PREFIX="homeassistant" \
  -e MQTT_STATE_PREFIX="pljusak" \
  -e DEVICE_ID="pljusak_zadar" \
  -e DEVICE_NAME="Pljusak Zadar" \
  -e LOG_LEVEL="info" \
  pljusak-mqtt:latest

simple:

docker run -d \
  --name pljusak \
  --restart unless-stopped \
  -e LOCATION="Zadar" \
  -e MQTT_HOST="192.168.1.10" \
  -e MQTT_USER="ha" \
  -e MQTT_PASSWORD="your_mqtt_password" \
  pljusak-mqtt:latest
