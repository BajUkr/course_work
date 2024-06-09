import csv
from faker import Faker
import random
from datetime import datetime

# Initialize the Faker generator
fake = Faker()

# Number of records per table
num_users = 100
num_artists = 25
num_albums = num_artists*5
num_tracks = num_albums*7
num_playlists = 10
num_likes = 20
num_premium_features = 3
num_subscription_plans = 2
num_payments = 10

# Generate data for each table
def generate_users():
    return [{'UserID': i, 'Username': fake.user_name(), 'Email': fake.email(), 'Password': fake.password()} for i in range(1, num_users + 1)]

def generate_artists():
    return [{'ArtistID': i, 'Name': fake.name(), 'Genre': fake.word()} for i in range(1, num_artists + 1)]

def generate_albums():
    return [{'AlbumID': i, 'Title': fake.sentence(nb_words=3), 'ArtistID': random.randint(1, num_artists), 'Genre': fake.word(), 'ReleaseDate': fake.date()} for i in range(1, num_albums + 1)]

def generate_tracks():
    return [{'TrackID': i, 'Title': fake.sentence(nb_words=2), 'ArtistID': random.randint(1, num_artists), 'AlbumID': random.randint(1, num_albums), 'PlayCount' : random.randint(1, 1000000000), 'Duration': str(datetime.strptime(fake.time(), '%H:%M:%S')), 'ReleaseDate': fake.date()} for i in range(1, num_tracks + 1)]

def generate_playlists():
    return [{'PlaylistID': i, 'UserID': random.randint(1, num_users), 'Title': fake.sentence(nb_words=2), 'CreationDate': fake.date()} for i in range(1, num_playlists + 1)]

def generate_likes():
    return [{'LikeID': i, 'UserID': random.randint(1, num_users), 'TrackID': random.randint(1, num_tracks)} for i in range(1, num_likes + 1)]

def generate_premium_features():
    return [{'Premium_Feature_ID': i, 'Name': fake.word()} for i in range(1, num_premium_features + 1)]

def generate_subscription_plans():
    return [{'Subscription_Plan_ID': i, 'Name': fake.word(), 'Price': round(random.uniform(10.0, 100.0), 2), 'Description': fake.text()} for i in range(1, num_subscription_plans + 1)]

def generate_payments():
    return [{'Payment_ID': i, 'User_ID': random.randint(1, num_users), 'Amount': round(random.uniform(5.0, 500.0), 2), 'Date': fake.date(), 'Method': fake.credit_card_provider()} for i in range(1, num_payments + 1)]

# Write data to CSV files
def data_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

# Generate and write data
data_to_csv(generate_users(), 'Users.csv')
data_to_csv(generate_artists(), 'Artists.csv')
data_to_csv(generate_albums(), 'Albums.csv')
data_to_csv(generate_tracks(), 'Tracks.csv')
data_to_csv(generate_playlists(), 'Playlists.csv')
data_to_csv(generate_likes(), 'Likes.csv')
data_to_csv(generate_premium_features(), 'PremiumFeatures.csv')
data_to_csv(generate_subscription_plans(), 'SubscriptionPlans.csv')
data_to_csv(generate_payments(), 'Payments.csv')