#!/usr/bin/python

# - connect to any kafka brooker
# - fetch information every second

# - a simple version, write data to kafka.
from kafka import KafkaProducer()
from kafak.error import KafkaError

import argparse
import logging

# - logging configuration
logging.basicConfig()
logger = logging.getLogger('data-producer')
# - debug, info, warning, error
logger.setLevel(logging.DEBUG)

symbol = 'AAPL'
kafka_broker = '127.0.0.1:9092'
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
	producer = KafkaProducer(
		bootstrap_servers = kafka_broker
	)
	