from kafka import KafkaConsumer
from json import load, loads
import json

def run_consume_server():
    consumer = KafkaConsumer('sf.crime.policecalls',
                            group_id="consumer_test",
                            bootstrap_servers=['localhost:9092'],
                            auto_offset_reset='earliest',
                            #consumer_timeout_ms=10000#,
                            value_deserializer = lambda x: loads(x.decode('utf-8'))
                        )
    
    for message in consumer:
        print(message.value)


if __name__ == "__main__":
    run_consume_server()