from confluent_kafka import Producer, KafkaError
import urllib.request
import json


month = '05'
day = '18'

with open(F"test_data/2022-{month}-{day}.json", 'r') as infile:
  response = infile.read()

# Read daily breadcrumb data from source
# url = 'http://www.psudataeng.com:8000/getBreadCrumbData'
# raw_response = urllib.request.urlopen(url)
# response = raw_response.read().decode('utf-8')

# Write response from source endpoint to JSON file
# file_name = raw_response.getheader('Content-Disposition')
# file_name = file_name.split('=')[1]

# STOP: DO NOT WRITE TO THIS LOCATION IN TEST FILE
# with open(F"/home/werner/breadcrumb_data/{file_name}", 'w') as outfile:
#  outfile.write(response)


# Produces messages containing response data and sends them to Kafka on Confluence Cloud
# The producer.py file in the  code sample provided by Confluence Cloud was used as a template for this work:
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
topic = 'topic1'
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


# Deliver individual sensor readings from response as messages to Kafka
sensor_readings = json.loads(response)
for reading in sensor_readings:
  reading_value = json.dumps(reading)
  print(F"Producing Record: {reading_value}")
  producer.produce(topic, value=reading_value, on_delivery=on_delivery_handler)
  producer.poll(0)

producer.flush()

print(F"{len(sensor_readings)} sensor readings from JSON file")
print(F"{delivered_count} messages produced and sent to Kafka")

