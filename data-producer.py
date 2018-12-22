#!/usr/bin/python

# - connect to any kafka brooker
# - fetch information every second

# - a simple version, write data to kafka.
from kafka import KafkaProducer
from kafka.errors import KafkaError

import argparse
import logging

# - logging configuration
logging.basicConfig()
logger = logging.getLogger('data-producer')
# - debug, info, warning, error
logger.setLevel(logging.DEBUG)

symbol = 'GOOG'
kafka_broker = '192.168.99.102:9092'
topic = 'data_analyzer'

if __name__ == "__main__":
	# - setup commendline argument
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help='the symbol')
	parser.add_argument('kafka_broker', help='ip address of kafka broker')
	parser.add_argument('topic', help='the kafka topic to write to')

	args = parser.parse_args()
	symbol = args.symbol
	kafka_broker = args.kafka_broker
	topic = args.topic

#	logger.debug('the symbol is %s' % symbol)

	# - instantiate a kafka producer
<<<<<<< HEAD
	producer = KafkaProducer(
		bootstrap_servers = kafka_broker
	)
	
	producer.send(topic=topic, value='hello, from erica.')
=======
	producer = KafkaProducer(bootstrap_servers='192.168.99.102:9092')

	producer.send(topic=topic, value='hello from tigers.')

>>>>>>> d751da6fdb424d62b4cd1577c58ca5ac916f0737
