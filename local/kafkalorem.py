from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4
import sys, lorem, random
import time

brokers = "kafka1:9092"

pconf = {
    'bootstrap.servers': brokers,
    'partitioner': 'murmur2_random',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': StringSerializer('utf_8')
}


p = SerializingProducer(pconf)


for n in range(1,1000):
    try:
        # Produce line (without newline)
        line = lorem.sentence()
        print(line)
        p.produce("words", key=str(uuid4()), value=line)
        p.poll(0)
        p.flush()
        print(line)
    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))