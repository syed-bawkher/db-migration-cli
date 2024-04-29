import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, Date, Text
from dotenv import load_dotenv
import os

load_dotenv() # Load environment variables

# Database credentials
username = os.getenv('SB_DB_USERNAME')
password = os.getenv('SB_DB_PASSWORD')
database = os.getenv('SB_DB_DATABASE')
host = os.getenv('SB_DB_HOST')
port = os.getenv('SB_DB_PORT')

# Establish a connection to the database
engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}')

# Define the metadata
metadata = MetaData()

# Define the table structure
customers = Table('Customer', metadata,
                  Column('customer_id', Integer, primary_key=True),
                  Column('first_name', String(255)),
                  Column('middle_name', String(255)),
                  Column('last_name', String(255)),
                  Column('add1', String(255)),
                  Column('add2', String(255)),
                  Column('add3', String(255)),
                  Column('add4', String(255)),
                  Column('email', String(255)),
                  Column('mobile', String(255)),
                  Column('office_phone', String(255)),
                  Column('residential_phone', String(255)),
                  Column('last_ordered_date', Date))

orders = Table('Orders', metadata,
               Column('orderNo', String(255), primary_key=True),
               Column('customer_id', None, ForeignKey('Customer.customer_id')),
               Column('date', Date),
               Column('onote', Text))

jacket_measurements = Table('JacketMeasurement', metadata,
                            Column('measurement_id', String(255), primary_key=True),
                            Column('customer_id', None, ForeignKey('Customer.customer_id')),
                            Column('date', Date),
                            Column('jacket_length', String(255)),
                            Column('natural_length', String(255)),
                            Column('back_length', String(255)),
                            Column('x_back', String(255)),
                            Column('half_shoulder', String(255)),
                            Column('to_sleeve', String(255)),
                            Column('chest', String(255)),
                            Column('waist', String(255)),
                            Column('collar', String(255)),
                            Column('other_notes', Text))

shirt_measurements = Table('ShirtMeasurement', metadata,
                           Column('measurement_id', String(255), primary_key=True),
                           Column('customer_id', None, ForeignKey('Customer.customer_id')),
                           Column('date', Date),
                           Column('length', String(255)),
                           Column('half_shoulder', String(255)),
                           Column('to_sleeve', String(255)),
                           Column('chest', String(255)),
                           Column('waist', String(255)),
                           Column('collar', String(255)),
                           Column('waist_coat_length', String(255)),
                           Column('sherwani_length', String(255)),
                           Column('other_notes', Text))

pant_measurements = Table('PantMeasurement', metadata,
                          Column('measurement_id', String(255), primary_key=True),
                          Column('customer_id', None, ForeignKey('Customer.customer_id')),
                          Column('date', Date),
                          Column('length', String(255)),
                          Column('inseem', String(255)),
                          Column('waist', String(255)),
                          Column('hips', String(255)),
                          Column('bottom', String(255)),
                          Column('knee', String(255)),
                          Column('other_notes', Text))

# Create the tables in the database
metadata.create_all(engine)
# Load data from CSV files and rename columns to match the SQL table structure
df_customers = pd.read_csv('./consolidated-data/ConsolidatedCustomers.csv')
df_customers.rename(columns={
    'name': 'first_name',
    'mname': 'middle_name',
    'lname': 'last_name',
    'phoff': 'office_phone',
    'phres': 'residential_phone',
    'date': 'last_ordered_date'  # Ensure this matches the CSV structure; if 'date' isn't present, adjust accordingly
}, inplace=True)

df_jackets = pd.read_csv('./consolidated-data/JacketMeasurement.csv')
df_jackets.rename(columns={
    'jl': 'jacket_length',
    'jnl': 'natural_length',
    'jbl': 'back_length',
    'jxback': 'x_back',
    'jtsleeve': 'to_sleeve',
    'jchest': 'chest',
    'jhs': 'half_shoulder',
    'jwaist': 'waist',
    'scollar': 'collar',
    'jothers': 'other_notes',
    'measurement_id': 'measurement_id',  # Assuming this already matches
    'date': 'date'
}, inplace=True)

df_pants = pd.read_csv('./consolidated-data/PantMeasurement.csv')
df_pants.rename(columns={
    'plength': 'length',
    'pinseem': 'inseem',
    'pwaist': 'waist',
    'phips': 'hips',
    'pbottom': 'bottom',
    'pknee': 'knee',
    'pothers': 'other_notes',
    'measurement_id': 'measurement_id',
    'date': 'date'
}, inplace=True)

df_shirts = pd.read_csv('./consolidated-data/ShirtMeasurement.csv')
df_shirts.rename(columns={
    'slength': 'length',
    'sshool': 'half_shoulder',
    'stosleeve': 'to_sleeve',
    'schest': 'chest',
    'swaist': 'waist',
    'scollar': 'collar',
    'sothers': 'other_notes',
    'vcoatlen': 'waist_coat_length',
    'sherlen': 'sherwani_length',
    'measurement_id': 'measurement_id',
    'date': 'date'
}, inplace=True)

df_orders = pd.read_csv('./consolidated-data/UpdatedOrders.csv')
df_orders.rename(columns={
    'orderNo': 'orderNo',  # Assuming this already matches
    'customer_id': 'customer_id',
    'date': 'date',
    'onote': 'onote'
}, inplace=True)

# Function to load data and insert into database
def load_and_insert(df, table):
    df.to_sql(table.name, con=engine, if_exists='append', index=False)

load_and_insert(df_customers, customers)
load_and_insert(df_orders, orders)
load_and_insert(df_jackets, jacket_measurements)
load_and_insert(df_shirts, shirt_measurements)
load_and_insert(df_pants, pant_measurements)
