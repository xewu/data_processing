#!/usr/bin/python

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from iexfinance.stocks import Stock

# shutdown hook
import atexit
import argparse
import logging
import schedule
import time

logging.basicConfig()
logger = logging.getLogger('data-producer-logger')
logger.setLevel(logging.DEBUG)

symbol = 'AAPL'
kafka_broker = '120.0.0.1:9092'
topic = 'stock-analyzer'

def fetch_price(producer, symbol):
	try:
		stock = Stock(symbol)
		price = stock.get_price()
		volume = stock.get_volume()
		data = "symbol: %s, price: %s, volume: %s" % (symbol, price, volume)
		producer.send(topic=msg_topic, value = data)
		logging.debug('send data to kafka %s' % data)
	except Excep as e:
		logger.warn('Failed to fetch fiannce data for %s' % symbol)

def shutdown_hook(producer):
	logger.info('closing kafka producer')
	producer.flush(10)
	producer.close(10)
	logger.info('kafka producer closed')

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help='the stock symbol')
	parser.add_argument('kafka_broker', help='ip of broker')
	parser.add_argument('topic', help='kafka topic to write to')
	
	args = parser.parse_args()
	symbol = args.symbol
	msg_topic = args.topic
	kafka_broker = args.kafka_broker
	
	# logger.debug('stock symbol is %s' % symbol)
	producer = KafkaProducer(bootstrap_servers=kafka_broker)
	schedule.every(10).seconds.do(fetch_price, producer, symbol)
	
	atexit.register(shutdown_hook, producer)
	
	while True:
		schedule.run_pending()
		time.sleep(1)
		
#	consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id='1011')

