from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4
import sys, random

brokers = "kafka1:9092"


pconf = {
    'bootstrap.servers': brokers,
    'partitioner': 'murmur2_random',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer':  StringSerializer('utf_8')
}

p = SerializingProducer(pconf)
stocks = "stocks.csv"
from datetime import datetime  
  #2023-10-13T08:16:13Z
def construct_stock(row):
    time_stamp = time.time()
    date_time = datetime.fromtimestamp(time_stamp)
    str_date_time = date_time.strftime("%Y-%m-%dT%H:%M:%SZ") #"%d-%m-%Y, %H:%M:%S"
    stock = {"name": row[6],
             "price": float(row[2]),
             "timestamp":str_date_time
             }
    return stock


import csv, json
import time
n = 0
with open(stocks) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    try:
        for row in csv_reader:
            #if n == 100:
             #  break
            stock = construct_stock(row)
            print(stock)
            p.produce('stock', value=json.dumps(stock))
            p.poll(0)
            p.flush()
            time.sleep(0.5)
            #n = n + 1
    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))

    
# !!! Use only if you need to purge all the messages in the queue !!!
# brokers = "kafka1:9092,kafka2:9093"

# admin_client = AdminClient({"bootstrap.servers":brokers})
# admin_client.delete_topics(topics=['stock'])