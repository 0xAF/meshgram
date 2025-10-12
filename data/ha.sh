#!/bin/bash
# The jq command below processes JSON output from the Home Assistant API:
# 1. '.[]' iterates over each item in the top-level JSON array.
# 2. 'select(.entity_id | test("emax"))' filters items whose 'entity_id' contains the substring "emax".
# 3. For each matching item, it outputs a string:
#    - The entity_id with the prefix "sensor.emax_w6_4_1613_" removed.
#    - The state value of the entity.
#    - Format: "<key>: <value>"
# 4. Example output:
#    battery: 100
#    temperature_2: 5.66666666666667
#    humidity: 48.0
#    wind_speed: 4.4
#    wind_direction: 270.0
#    rain_total: 144.0
#    uv_index: 23.0
#    outside_luminance: 0
#    temperature: 19.5
#    ...
# This output is then read line-by-line and mapped to new keys using key_map.

# cd to the script's directory, so the .env file is found where the script is
cd "$(dirname "$0")"

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

if [ -z "$HA_API_TOKEN" ]; then
  echo "Error: HA_API_TOKEN is not set. Please set it in your environment or .env file." >&2
  exit 1
fi

json_output=$(curl -sS -X GET \
  -H "Authorization: Bearer $HA_API_TOKEN" \
  -H "Content-Type: application/json" \
  http://homeassistant.local:8123/api/states 2>/dev/null)

# echo "$json_output" | jq
# exit

declare -A key_map=(
  [battery]="voltage"
  #[temperature_2]="soil_temperature"
  [humidity]="relative_humidity"
  [wind_speed]="wind_speed"
  [wind_direction]="wind_direction"
  [rain_total]="rainfall_24h"
  [uv_index]="uv_lux"
  [outside_luminance]="lux"
  [temperature]="temperature"
)

while IFS=": " read -r key value; do
  if [[ -n "${key_map[$key]}" ]]; then
    echo "${key_map[$key]}: $value"
  fi
done < <(
  {
    echo "$json_output" |
    jq -r '
      .[]
      | select(.entity_id | test("emax"))
      | (.entity_id | sub("sensor.emax_w6_4_1613_"; "")) as $k
      | if $k == "outside_luminance" then empty
        elif $k == "wind_speed" then "\($k): \((.state | tonumber) / 3.6)"
        else "\($k): \(.state)"
        end
    ';
    echo "$json_output" |
    jq -r '
      .[]
      | select(.entity_id == "sensor.rainsensor_osvetenost")
      | "outside_luminance: \(.state)"
    ';
  }
)

# https://buf.build/meshtastic/protobufs/file/master:meshtastic/telemetry.proto

# available environment metrics that can be sent from a node:
# Temperature measured
# r.environment_metrics.temperature = 56.4
# Relative humidity percent measured
# r.environment_metrics.relative_humidity = 70.0
# Barometric pressure in hPA measured
# r.environment_metrics.barometric_pressure = 955.0
# # Gas resistance in MOhm measured
# r.environment_metrics.gas_resistance = 99
# # Voltage measured (To be depreciated in favor of PowerMetrics in Meshtastic 3.x)
# r.environment_metrics.voltage = 99
# # Current measured (To be depreciated in favor of PowerMetrics in Meshtastic 3.x)
# r.environment_metrics.current = 99
# # uint32 relative scale IAQ value as measured by Bosch BME680 . value 0-500.
# # Belongs to Air Quality but is not particle but VOC measurement. Other VOC values can also be put in here.
# r.environment_metrics.iaq = 99
# # RCWL9620 Doppler Radar Distance Sensor, used for water level detection. Float value in mm.
# r.environment_metrics.distance = 99
# # VEML7700 high accuracy ambient light(Lux) digital 16-bit resolution sensor.
# r.environment_metrics.lux = 99
# # VEML7700 high accuracy white light(irradiance) not calibrated digital 16-bit resolution sensor.
# r.environment_metrics.white_lux = 99
# # Infrared lux
# r.environment_metrics.ir_lux = 99
# # Ultraviolet lux
# r.environment_metrics.uv_lux = 99
# # uint32 Wind direction in degrees
# # 0 degrees = North, 90 = East, etc...
# r.environment_metrics.wind_direction = 180
# # Wind speed in m/s
# r.environment_metrics.wind_speed = 99
# # Weight in KG
# r.environment_metrics.weight = 99
# # Wind gust in m/s
# r.environment_metrics.wind_gust = 11
# # Wind lull in m/s
# r.environment_metrics.wind_lull = 89
# # Radiation in µR/h
# r.environment_metrics.radiation = 100
# # Rainfall in the last hour in mm
# r.environment_metrics.rainfall_1h = 88
# # Rainfall in the last 24 hours in mm
# r.environment_metrics.rainfall_24h = 11
# # uint32 Soil moisture measured (% 1-100)
# r.environment_metrics.soil_moisture = 67
# # Soil temperature measured (*C)
# r.environment_metrics.soil_temperature = 22
