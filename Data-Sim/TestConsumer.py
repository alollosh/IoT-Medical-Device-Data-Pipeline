from kafka import KafkaConsumer
import json

def kafkaConsumerFx():
    consumer = KafkaConsumer(
        'health_records',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

consumer = kafkaConsumerFx()

for message in consumer:
    print(f"Received message: {message.value}")