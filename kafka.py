import os
from confluent_kafka import Producer
import json
from uuid import uuid4
import time 

def delivery_report(err, msg):
    if err is not None:
        print(f"메시지 전송 실패: {err}")
    else:
        print(f"메시지 전송 성공: {msg.topic()}[{msg.partition()}]")

def send_stream(topic, data,wait_for_seconds=60 ):
    producer = Producer({
         "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    })
    for row in data:
        producer.poll(0)
        producer.produce(topic=topic,
                         key=str(uuid4()),
                         value=json.dumps(row),
                         callback=delivery_report
                         )
        producer.flush()
        time.sleep(wait_for_seconds)
