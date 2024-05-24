Please Note: This repo only contains the producers for this pipeline. The consumers can be found [here](https://github.com/kelseyleewerner/bus_data_consumers).

# Guide to Files and Directories in this Repo

**TEST**

Directory contains a sandbox area for testing changes to both producer before they are introduced to gather_data.py or gather_events.py

**breadcrumb_data**

Directory to store the JSON files that are downloaded from http://www.psudataeng.com:8000/getBreadCrumbData

**confluent-env**

Directory to store the Python packages installed to my virtualenv

**stop_event_data**

Contains sub-directories to store stop event data:

- html_data stores HTML files saved from http://www.psudataeng.com:8000/getStopEvents
- json_data stores JSON files that are output by gather_events.py after parsing HTML data

**.gitignore**

This file indicates to Git that the following types of files and directories should be ignored:

- Hidden files that have been auto-generated by the system
- The large JSON files that are stored in breadcrumb_data, TEST/test_data, stop_event_data/json_data, and test_stop_event_data/json_data
- The large HTML files that are stored in stop_event_data/html_data and test_stop_event_data/html_data

**.vimrc**

Configuration file for vim

**count_sensor_readings.py**

A Python utility for counting the number of breadcrumb sensor readings in each daily batch of JSON

**count_stop_events.py**

A Python utility for counting the number of stop events in each daily HTML file after it has been parsed to JSON

**crontab**

This crontab file creates the cron task to run the gather_data.py file followed by the gather_events.py file every morning. In my VM instance this file does not live in my user's home directory (like the rest of the items in this repo) and instead is stored in /var/spool/cron

**gather_data.py**

This file is a Kafka producer that gathers data from http://www.psudataeng.com:8000/getBreadCrumbData and sends it the breadcrumb_readings Kafka topic

**gather_events.py**

This file is a Kafka producer that gathers data from http://www.psudataeng.com:8000/getStopEvents and sends it the stop_events Kafka topic
