import time 
import json 
import random 
from datetime import datetime
from kafka import KafkaProducer
import string 

def generate_message(id):
    movies = [
        'The Shawshank Redemption (1994)',
        'The Godfather (1972)',
        'The Dark Knight (2008)',
        'The Lord of the Rings (2003)',
        'Pulp Fiction (1994)',
        'Forrest Gump (1994)',
        'Fight Club (1999)',
        'Inception (2010)',
        'Star Wars (1980)',
        'The Matrix (1999)',
    ]

    # Generate a random message
    message = ''.join(random.choice(string.ascii_letters) for i in range(32))
    
    # generate random scoring
    random_score = int(random.gauss(6,2))
    
    return {
        'user_id': id,
        'movie': random.choice(movies),
        'score': random_score,
        'message': message
    }

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['host.docker.internal:9094'],
    value_serializer= lambda v: json.dumps(v).encode('utf-8')
)

id = 1

# Infinite loop - runs until you kill the program
while True:

    # Generate a message
    dummy_message = generate_message(id)
    
    # printing the message generated
    print(f'Producing message @ {datetime.now()} | Message = {str(dummy_message)}')

    # Send it to our 'messages' topic
    producer.send('messages', dummy_message)
    
    # sleep before sending messages again
    time_to_sleep = 3
    time.sleep(time_to_sleep)
    id += 1