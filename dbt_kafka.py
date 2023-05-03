#!/usr/bin/python3

# imports
from kafka import KafkaProducer, KafkaConsumer, TopicPartition 
import numpy as np              
from sys import argv, exit
from time import time, sleep
from datetime import datetime, timedelta

DEVICE_PROFILES = {
    "Soyabean": {'bandi_market': (21.3, 37.7), 'city_market': (47.4, 58.7), 'grocery_shop': (101, 125.5) },
    "Barley": {'bandi_market': (29.5, 39.3), 'city_market': (32.0, 52.9), 'grocery_shop': (70.0, 90.3) },
    "Sorghum": {'bandi_market': (33.9, 61.7), 'city_market': (72.8, 91.8), 'grocery_shop': (96.9, 109.3) },
}

# check for arguments, exit if wrong
if len(argv) != 2 or argv[1] not in DEVICE_PROFILES.keys():
	print("please provide a valid topic name:")
	for key in DEVICE_PROFILES.keys():
		print(f"  * {key}")
	print(f"\nformat: {argv[0]} DEVICE_NAME")
	exit(1)

profile_name = argv[1]
profile = DEVICE_PROFILES[profile_name]

# set up the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# set up the consumer
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.assign([TopicPartition(topic='crop', partition=0)])
consumer.seek_to_end()

# set initial timestamps for windowing
start_time = datetime.now()
end_time = start_time + timedelta(minutes=1)

count = 1

# until ^C
while True:
	# get random values within a normal distribution of the value
	bandi_market = np.random.normal(profile['bandi_market'][0], profile['bandi_market'][1])
	city_market = max(0, min(np.random.normal(profile['city_market'][0], profile['city_market'][1]), 100))
	grocery_shop = np.random.normal(profile['grocery_shop'][0], profile['grocery_shop'][1])
	
	# create CSV structure
	msg = f'{time()},{profile_name},{bandi_market},{city_market},{grocery_shop}'

	# send to Kafka
	producer.send('crop', bytes(msg, encoding='utf8'))
	print(f'sending data to kafka, #{count}')

	count += 1
	
	# check if current time has exceeded the end of the current window
	if datetime.now() > end_time:
		# seek to the start of the window
		consumer.seek(0)
		window_data = []
		# read messages until the end of the window
		for message in consumer:
			if datetime.fromtimestamp(float(message.value.decode().split(',')[0])) > end_time:
				break
			window_data.append(message.value.decode())
		

		if window_data:
			bandi_market_sum = 0
			city_market_sum = 0
			grocery_shop_sum = 0
			for data in window_data:
				bandi_market_sum += float(data.split(',')[2])
				city_market
