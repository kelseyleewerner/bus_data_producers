from confluent_kafka import Producer, KafkaError
import urllib.request
import json
from datetime import date
import sys
import re
from bs4 import BeautifulSoup
import os



# Parse daily HTML into individual JSON stop events
today = date.today()
log_file_path = F"/home/werner/log_files/stop_event_producer_logs-{today.strftime('%Y-%m-%d')}.txt"
json_file_path = F"/home/werner/stop_event_data/json_data/{today.strftime('%Y-%m-%d')}.json"

print('Parsing HTML...')

url = 'http://www.psudataeng.com:8000/getStopEvents/'
html = urllib.request.urlopen(url)
stop_event_soup = BeautifulSoup(html, 'lxml')

# Parse date of stop event batch
page_title = str(stop_event_soup.find_all('h1')[0])
date_regex = re.compile('\d{4}-\d{2}-\d{2}')
stop_event_date = re.search(date_regex, page_title).group(0)

# Parse list of all Trip IDs and Tables from daily batch
raw_table_titles = stop_event_soup.find_all('h3')
table_titles = BeautifulSoup(str(raw_table_titles), 'lxml').get_text()
trip_id_regex = re.compile('\d+')
trip_ids = re.findall(trip_id_regex, str(table_titles))

tables = stop_event_soup.find_all('table')

trip_id_count = len(trip_ids)
table_count = len(tables)

# Verify that there is one Trip ID for each Table element
if trip_id_count != table_count:
  log_message = ['Fatal Error: Mismatch in trip_id and table count']
  log_message.append(F"trip_id count is {trip_id_count} and table_count is {table_count}")
  log_message.append('This must be resolved before continuing, HTML of souce may have changed')

  with open(log_file_path, 'a') as outfile:
    outfile.write('\n'.join(log_message))
    outfile.write('\n')
    outfile.write('\n')

  sys.exit('Fatal Error: Review daily log file for details')

# Parse table rows to extract individual stop event data attributes
stop_event_list = []
for index in range(trip_id_count):
  trip_id = trip_ids[index]
  table = tables[index]

  raw_rows = table.find_all('tr')

  # If table is empty and only has header row, do not include in data
  if len(raw_rows) == 1:
    continue

  header = raw_rows[0]
  rows = raw_rows[1:]
  columns = header.find_all('th')
  column_count = len(columns)

  # Verify that all tables have expected number of columns
  if column_count != 23:
    log_message = ['Fatal Error: Unexpected number of columns in table']
    log_message.append(F"For trip_id {trip_id}, expect 23 columns and instead found {column_count}")
    log_message.append('This must be resolved before continuing, HTML of souce may have changed')

    with open(log_file_path, 'a') as outfile:
      outfile.write('\n'.join(log_message))
      outfile.write('\n')
      outfile.write('\n')

    sys.exit('Fatal Error: Review daily log file for details')

  columns = BeautifulSoup(str(columns), 'lxml').get_text()
  columns = columns.replace('[', '').replace(']', '')
  columns = columns.split(', ')

  # Transform each stop event row into a dictionary that can be sent as a message to Kafka
  for row in rows:
    stop_event = {
      'trip_id': trip_id,
      'event_date': stop_event_date
    }

    cells = row.find_all('td')
    cells = BeautifulSoup(str(cells), 'lxml').get_text()
    cells = cells.replace('[', '').replace(']', '')
    cells = cells.split(', ')

    for item in range(23):
      stop_event[columns[item]] = cells[item]

    stop_event_list.append(stop_event)

print('Completed parsing HTML!')

# Write stop events to JSON file
with open(json_file_path, 'w') as outfile:
    json.dump(stop_event_list, outfile, indent=4)


# Produces messages containing stop event data and sends them to Kafka on Confluence Cloud
# The producer.py file in the code sample provided by Confluence Cloud was used as a template for this work:
#   https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/python.html#avro-and-confluent-cloud-schema-registry


# Instantiate Producer
producer_config = {
  'bootstrap.servers' : os.environ['BOOTSTRAP_SERVERS'],
  'security.protocol' : os.environ['SECURITY_PROTOCOL'],
  'sasl.mechanisms' : os.environ['SASL_MECHANISM'],
  'sasl.username' : os.environ['SASL_USERNAME'],
  'sasl.password' : os.environ['SASL_PASSWORD']
}
producer = Producer(producer_config)
topic = 'stop_events'
delivered_count = 0


# Message delivery handler triggered by poll() or flush() when a message is successfully delivered
#   or when delivery has failed after retries
# This function is copied with few modifications from the Confluent Cloud code example:
#   https://github.com/confluentinc/examples/blob/7.1.0-post/clients/cloud/python/producer.py#L50
def on_delivery_handler(error, message):
  global delivered_count
  if error is not None:
    print(F"FAILED to deliver message:{error}")
  else:
     delivered_count += 1
     print(F"Produced record to topic {message.topic()} partition [{message.partition()}] @ offset {message.offset()}")


# Deliver individual stop events from data source as messages to Kafka
for event in stop_event_list:
  event_value = json.dumps(event)
  print(F"Producing Record: {event_value}")
  producer.produce(topic, value=event_value, on_delivery=on_delivery_handler)
  producer.poll(0)

producer.flush()

print(F"{len(stop_event_list)} stop events from HTML document")
print(F"{delivered_count} messages produced and sent to Kafka")

