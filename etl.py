import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import numpy as np
from dateutil.relativedelta import relativedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create engine connections
oltp_engine = create_engine("postgresql://postgres:frog2003@localhost:5432/oltp")
olap_engine = create_engine("postgresql://postgres:frog2003@localhost:5432/olap")

def extract_table(table_name, engine):
    """Extract data from a given table in the database."""
    logging.info(f"Extracting table: {table_name}")
    try:
        data = pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
        logging.info(f"Successfully extracted {len(data)} records from {table_name}")
        return data
    except Exception as e:
        logging.error(f"Error extracting table {table_name}: {e}")
        return pd.DataFrame()

def load_existing_data(table_name, engine):
    """Load existing data from OLAP to check for duplicates."""
    logging.info(f"Loading existing data from table: {table_name}")
    try:
        data = pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
        logging.info(f"Successfully loaded {len(data)} records from {table_name}")
        return data
    except Exception as e:
        logging.error(f"Error loading existing data from table {table_name}: {e}")
        return pd.DataFrame()

def transform_and_load(df, table_name, engine, key_columns):
    """Transform and load data, ensuring no duplicates."""
    logging.info(f"Transforming and loading data into table: {table_name}")
    existing_data = load_existing_data(table_name, engine)
    if not existing_data.empty:
        merged_df = df.merge(existing_data, on=key_columns, how='left', indicator=True)
        new_data = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    else:
        new_data = df

    if not new_data.empty:
        try:
            new_data.to_sql(table_name, engine, if_exists='append', index=False)
            logging.info(f"Successfully loaded {len(new_data)} new records into {table_name}")
        except Exception as e:
            logging.error(f"Error loading data into table {table_name}: {e}")
    else:
        logging.info(f"No new data to load into {table_name}")

def prepare_dim_user(users, payments, subscription_plans):
    """Prepare the user dimension table."""
    logging.info("Preparing dim_user table")
    # Group payments by user_id and get the earliest payment date for each user
    first_payment_dates = payments.groupby('user_id')['date'].min().reset_index()
    first_payment_dates.rename(columns={'date': 'startdate', 'user_id': 'userid'}, inplace=True)

      # Group payments by user_id and get the latest payment date and amount for each user
    latest_payments = payments.groupby('user_id').agg({
        'date': 'max', 
        'amount': 'last',
        'subscription_plan_id': 'last',
    }).reset_index()

    # Merge payments with subscription_plans to get the subscription plan price
    # print(latest_payments)
    # print(subscription_plans.columns)
    latest_payments = latest_payments.merge(
        subscription_plans[['subscription_plan_id', 'price']], 
        on='subscription_plan_id', 
        how='left'
    )
    # print(latest_payments.columns)


    # Calculate the enddate by adding the payment amount divided by the subscription plan price (in months) to the latest payment date
    latest_payments['months'] = latest_payments['amount'] / latest_payments['price']
    latest_payments['enddate'] = pd.to_datetime(latest_payments['date']) + \
                                 pd.to_timedelta((latest_payments['months'] * 30).astype(int), unit='D')

    # Rename columns to match the users dataframe
    latest_payments.rename(columns={'date': 'startdate', 'user_id': 'userid'}, inplace=True)

    # Merge the latest payment data with the users dataframe
    users = users.merge(latest_payments[['userid', 'startdate', 'enddate']], on='userid', how='left')

    # Determine if the current date is before the enddate to set iscurrent
    users['iscurrent'] = pd.to_datetime('now').normalize() <= users['enddate']
    
    # print(users.columns)
    return users[['userid', 'username', 'email', 'startdate', 'enddate', 'iscurrent']]
def prepare_dim_artist(artists):
    """Prepare the artist dimension table."""
    logging.info("Preparing dim_artist table")
    return artists[['artistid', 'name', 'genre']]

def prepare_dim_album(albums):
    """Prepare the album dimension table."""
    logging.info("Preparing dim_album table")
    return albums[['albumid', 'title', 'releasedate']]

def prepare_dim_track(tracks):
    """Prepare the track dimension table."""
    logging.info("Preparing dim_track table")
    return tracks[['trackid', 'title', 'playcount', 'duration']]

def prepare_dim_time(min_date, max_date):
    """Prepare the time dimension table."""
    logging.info("Preparing dim_time table")
    dates = pd.date_range(start=min_date, end=max_date)
    dim_time = pd.DataFrame({
        'dateid': range(1, len(dates) + 1),
        'date': dates,
        'month': dates.month,
        'quarter': dates.quarter,
        'year': dates.year,
        'day': dates.day,
 
    })
    return dim_time


def prepare_fact_streaming(track_data):
    """Prepare the streaming fact table using track data."""
    logging.info("Preparing fact_streaming table")

    # Convert duration from datetime.time to string "HH:MM:SS"
    track_data['duration_str'] = track_data['duration'].apply(lambda x: x.strftime('%H:%M:%S'))
    track_data['listeningtime'] = track_data['playcount'] * pd.to_timedelta(track_data['duration_str']).dt.total_seconds()
    track_data['startdate'] = datetime.now().date()
    track_data['releasedate'] = pd.to_datetime(track_data['releasedate'])    
    return track_data[['trackid', 'playcount', 'listeningtime']]

def prepare_fact_subscription(subscription_data, payment_data, dim_time, dim_user_data):
    """Prepare the subscription fact table using subscription plan data."""
    logging.info("Preparing fact_subscription table")
    subscription_data['userid'] = payment_data['user_id']
    subscription_data['startdate'] = payment_data['date'].min()
    subscription_data['monthlyfee'] = subscription_data['price']
    subscription_data['dateid'] = dim_time['dateid']
    subscription_data["enddate"] = subscription_data["startdate"].apply(
        lambda d: d + relativedelta(months=1))
    subscription_data['iscurrent'] = subscription_data['enddate'] > datetime.now().date()
    subscription_data["subscriptionplanid"] = subscription_data["subscription_plan_id"]

    # Match the fact_subscription data with dim_user data
    subscription_data = subscription_data.merge(
        dim_user_data[['userid', 'startdate', 'enddate', 'iscurrent']],
        on=['userid', 'startdate', 'enddate', 'iscurrent'],
        how='inner'
    )

    return subscription_data[['userid', 'startdate', 'enddate', "iscurrent", "dateid", 'subscriptionplanid', 'monthlyfee']]
def main():
    # Extract data from OLTP
    users = extract_table("user", oltp_engine)
    artists = extract_table("artist", oltp_engine)
    albums = extract_table("album", oltp_engine)
    tracks = extract_table("track", oltp_engine)
    sub_plans = extract_table("subscriptionplan", oltp_engine)
    payments = extract_table("payment", oltp_engine)

    

    # Prepare and load dimension tables into OLAP
    transform_and_load(prepare_dim_user(users, payments, sub_plans), "dim_user", olap_engine, ['userid', 'startdate'])
    transform_and_load(prepare_dim_artist(artists), "dim_artist", olap_engine, ['artistid'])
    transform_and_load(prepare_dim_album(albums), "dim_album", olap_engine, ['albumid'])
    transform_and_load(prepare_dim_track(tracks), "dim_track", olap_engine, ['trackid'])

    dim_user_data = extract_table("dim_user", olap_engine)
    # Prepare and load dim_time
    min_date = tracks['releasedate'].min()
    max_date = tracks['releasedate'].max()
    dim_time = prepare_dim_time(min_date, max_date)
    transform_and_load(dim_time, "dim_time", olap_engine, ['dateid'])


    # Prepare and load fact_streaming
    fact_streaming = prepare_fact_streaming(tracks)
    transform_and_load(fact_streaming, "fact_streaming", olap_engine, ['trackid'])

    # Prepare and load fact_subscription
    # fact_subscription = prepare_fact_subscription(sub_plans, payments, dim_time, dim_user_data)
    # transform_and_load(fact_subscription, "fact_subscription", olap_engine, ['userid', 'startdate', 'enddate', "iscurrent", 'dateid'])


    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    main()