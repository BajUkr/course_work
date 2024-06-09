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
num_subscription_plans = 4
num_payments = num_users*num_subscription_plans

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
    plan_names = ['Free', 'Student', 'Premium', 'Family']
    return [{'Subscription_Plan_ID': i+1, 'Name': plan_names[i], 'Price': i*5, 'Description': fake.text()} for i in range(len(plan_names))]
def generate_payments():
    return [{'Payment_ID': i, 'User_ID': random.randint(1, num_users), 'Amount': random.randint(1,77)*25, 'Date': fake.date(), 'Subscription_Plan_ID': random.randint(2, num_subscription_plans),'Method': fake.credit_card_provider()} for i in range(1, num_payments + 1)]

# Write data to CSV files
def data_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

# Generate and write data
data_to_csv(generate_users(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Users.csv')
data_to_csv(generate_artists(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Artists.csv')
data_to_csv(generate_albums(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Albums.csv')
data_to_csv(generate_tracks(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Tracks.csv')
data_to_csv(generate_playlists(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Playlists.csv')
data_to_csv(generate_likes(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Likes.csv')
data_to_csv(generate_premium_features(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\PremiumFeatures.csv')
data_to_csv(generate_subscription_plans(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\SubscriptionPlans.csv')
data_to_csv(generate_payments(), 'C:\\Program Files\\PostgreSQL\\16\\data\\course_work_data\\Payments.csv')