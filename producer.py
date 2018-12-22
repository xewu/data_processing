#!/usr/bin/python

from kafka import KafkaProducer
from kafka.errors import KafkaError

import argparse
import logging

logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

symbol = 'APPL'
kafka_broker = 'localhost:9092'
topic = 'analyzer'

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help='the data symbol')
	parser.add_argument('kafka_broker', help='ip address of kafka broker')
	parser.add_argument('topic', help='topic to write into')
	
	arguments = parser.parse_args()
	symbol = arguments.symbol
	kafka_broker = arguments.kafka_broker
	topic = arguments.topic
	
	producer = KafkaProducer(bootstrap_servers=kafka_broker)
	
	producer.send(topic=topic, value='hello from erica')
	