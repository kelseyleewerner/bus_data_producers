import json

# Simple python program for counting the individual records in a JSON file

# Date value must be set manually
# STARTS WITH 0 FOR SINGLE DIGIT DATES
day = '19'

with open(F"breadcrumb_data/2022-05-{day}.json", 'r') as infile:
  sensor_readings = json.loads(infile.read())

  print(F"Sensor Readings Count: {len(sensor_readings)}")

