import json

# Simple python program for counting the individual records in a JSON file

# Date value must be set manually
# STARTS WITH 0 FOR SINGLE DIGIT DATES
day = '18'

with open(F"stop_event_data/json_data/2022-05-{day}.json", 'r') as infile:
  stop_events = json.loads(infile.read())

  print(F"Stop Events Count: {len(stop_events)}")
