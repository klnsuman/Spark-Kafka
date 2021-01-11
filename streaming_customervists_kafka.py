# -*- coding: utf-8 -*-
import sys
import os
#os.system("pip install kafka-python")
#os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/lib/python3.7/site-packages (2.0.2)'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark.sql.context import SQLContext
from kafka import KafkaProducer
from kafka import SimpleProducer, KafkaClient

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def handler(message):
	records = message.collect()
	for record in records:
		producer.send('CustomersVisiting', str(record))
		producer.flush()

def main():
	sc = SparkContext(appName="PythonStreamingDirectKafkacustomervisiting")
	ssc = StreamingContext(sc, 2)

	kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'CustomersVisiting':1})

	lines = kvs.map(lambda v: json.loads(v[1]))

	#------------------------------------------------
	# Extract the Customer Data From Kafka Topic
	#------------------------------------------------
	age = lines.map(lambda x:x['age'])
	age.map(lambda x:'Age Of Customer is: %s' % x).pprint()

	cust_name = lines.map(lambda x:x['firstname']) 
	cust_name.map(lambda x:'Name Of Customer is: %s' % x).pprint()

	income = lines.map(lambda x:x['income']) 
	income.map(lambda x:'Income Of Customer is: %s' % x).pprint()
	
	gender = lines.map(lambda x:x['gender']) 
	gender.map(lambda x:'Gender Of Customer is: %s' % x).pprint()

	#----------------------------------------------------
	# End of Fetching Customer Details from Kafka Topic
	#----------------------------------------------------

	#----------------------------------------------------
	# Send to Kafka Topic
	#----------------------------------------------------
	kvs.foreachRDD(handler)

	#----------------------------------------------------
	# End of Send to Kafka Topic
	#----------------------------------------------------
	print("type is ",type(lines))
	t = type(lines)
	lines.pprint()

	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
    main()
