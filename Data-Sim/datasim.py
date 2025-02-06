import random
import time
from kafka import KafkaProducer
import json

def PatientDataFx():

   while True:
        PatientID = "PT" + str(random.randint(1111,9999))
        Time = time.time()
        HR = random.randint(60,100)

        #now to simulate an irregular occurnece of HR
        if random.random() < 0.05:
            HR = random.choice([40,150])
        
        PatientData = {
            "PatiendID" : PatientID,
            "HR"        : HR       ,
            "Time"      : Time
        }

        print(PatientData)
        yield PatientData
        time.sleep (1)

        
#for record in PatientDataFx():
#   print (record)


def kafkaProducerFx():

    kafka_config = {
        'bootstrap_servers': 'localhost:9092',
        'value_serializer': lambda v : json.dumps(v).encode('UTF-8')

    }
    return KafkaProducer(**kafka_config)
    

#create prod
producer = kafkaProducerFx()
topic    = "health_records"

# now send the patient records

for record in PatientDataFx():
    producer.send(topic, value=record)  
    producer.flush()  
    print(f"Sent to Kafka: {record}")  


