from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4
import sys, lorem, random

brokers = "kafka1:9092"

pconf = {
    'bootstrap.servers': brokers,
    'partitioner': 'murmur2_random',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': StringSerializer('utf_8')
}


p = SerializingProducer(pconf)

user_types = {True: 'bot', False: 'human'}

namespace_dict = {-2: 'Media', 
                  -1: 'Special', 
                  0: 'main namespace', 
                  1: 'Talk', 
                  2: 'User', 3: 'User Talk',
                  4: 'Wikipedia', 5: 'Wikipedia Talk', 
                  6: 'File', 7: 'File Talk',
                  8: 'MediaWiki', 9: 'MediaWiki Talk', 
                  10: 'Templatxe', 11: 'Template Talk', 
                  12: 'Help', 13: 'Help Talk', 
                  14: 'Category', 15: 'Category Talk', 
                  100: 'Portal', 101: 'Portal Talk',
                  108: 'Book', 109: 'Book Talk', 
                  118: 'Draft', 119: 'Draft Talk', 
                  446: 'Education Program', 447: 'Education Program Talk', 
                  710: 'TimedText', 711: 'TimedText Talk', 
                  828: 'Module', 829: 'Module Talk', 
                  2300: 'Gadget', 2301: 'Gadget Talk', 
                  2302: 'Gadget definition', 2303: 'Gadget definition Talk'}




def construct_event(event_data, user_types):
    # use dictionary to change assign namespace value and catch any unknown namespaces (like ns 104)
    try:
        event_data['namespace'] = namespace_dict[event_data['namespace']]
    except KeyError:
        event_data['namespace'] = 'unknown'

    # assign user type value to either bot or human
    user_type = user_types[event_data['bot']]

    # define the structure of the json event that will be published to kafka topic
    event = {"id": event_data['id'],
             "domain": event_data['meta']['domain'],
             "uri": event_data['meta']['uri'],
             "namespace": event_data['namespace'],
             "title": event_data['title'],
             #"comment": event_data['comment'],
             "timestamp": event_data['meta']['dt'],#event_data['timestamp'],
             "user_name": event_data['user'],
             "user_type": user_type
             #"minor": event_datab['minor'],
             }
    
    if event_data["type"] == "edit":
        event["old_length"] = event_data['length']['old']
        event["new_length"] = event_data['length']['new']
    
    return event




import json
import requests
from sseclient import SSEClient  # pip install sseclient-py

URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Wikimedia requires a descriptive User-Agent with a contact
session = requests.Session()
session.headers.update({
    "User-Agent": "MyWikiStreamBot/0.1 (andrea.mauri@univ-lyon1.fr)",  # <-- put your contact
    "Accept": "text/event-stream",
})

# Connect directly via SSEClient using the session
client = SSEClient(URL, session=session)

for event in client:
    if event.event != "message" or not event.data:
        continue
    try:
        data = json.loads(event.data)
    except ValueError:
        continue
    if data.get("type") == "edit":
        # build your event; placeholders shown
        event_to_send = construct_event(data, user_types)
        print(event_to_send)
        p.produce('edit', value=json.dumps(event_to_send))
        p.poll(0)
        p.flush()
        