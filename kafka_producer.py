from kafka import KafkaProducer
import json
import time
import random


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


TOPIC = 'test_topic'

try:
    while True:
        # sample message
        message = {
            'id': random.randint(1, 1000),
            'timestamp': time.time(),
            'value': random.uniform(0, 100),
            'category': random.choice(['A', 'B', 'C'])
        }
        
        
        producer.send(TOPIC, value=message)
        print(f"Sent: {message}")
        
    
        time.sleep(1)

except KeyboardInterrupt:
    producer.close()
    print("\nStopped sending messages")