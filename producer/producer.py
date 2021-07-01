#vital
from pykafka import KafkaClient
import json
import time

client = KafkaClient(hosts="kafka:9092")
topic = client.topics['input_topic']
producer = topic.get_sync_producer()

ip = open('./test_data.json')
arr = json.load(ip)

for x in arr:
    msg = json.dumps(arr[x])
    producer.produce(msg.encode('ascii'))
    time.sleep(2)
