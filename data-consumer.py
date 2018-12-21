#!/usr/bin/python

from kafka import KafkaConsumer

if __name__ == "__main__":
	consumer = KafkaConsumer('data_analyzer', bootstrap_servers='192.168.99.100:9092')
	for msg in consumer:
		print(msg)