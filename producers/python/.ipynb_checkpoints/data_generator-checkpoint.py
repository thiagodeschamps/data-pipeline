import random 
import string 

user_ids = list(range(1, 101))
recipient_ids = list(range(1, 101))
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

def generate_message() -> dict:
    
    random_user_id = random.choice(user_ids)
    
    # Copy the recipients array
    recipient_ids_copy = recipient_ids.copy()
    
    # User can't send message to himself
    recipient_ids_copy.remove(random_user_id)
    random_recipient_id = random.choice(recipient_ids_copy)

    # Generate a random message
    message = ''.join(random.choice(string.ascii_letters) for i in range(32))
    
    # generate random scoring
    random_score = int(random.gauss(6,2))
    
    return {
        'user_id': random_user_id,
        'recipient_id': random_recipient_id,
        'movie': random.choice(movies),
        'score': random_score,
        'message': message
    }