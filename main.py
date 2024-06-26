import pandas as pd
import re
from uuid import uuid4

# Load the dataset
df = pd.read_csv('Database.csv', low_memory=False)

# Clean 'orderno'
df['orderNo'] = df['orderno'].astype(str).str.strip()

# Function to clean phone numbers by removing non-digit characters for consolidation
def clean_phone(phone):
    return re.sub(r"[^\d]", "", str(phone)) if pd.notna(phone) else ""

# Create new columns for cleaned phone numbers
df['clean_mobile'] = df['mobile'].apply(clean_phone)
df['clean_phoff'] = df['phoff'].apply(clean_phone)
df['clean_phres'] = df['phres'].apply(clean_phone)

# Combine cleaned phone numbers into one field, prioritizing mobile, then office, then residential
df['combined_phone'] = df[['clean_mobile', 'clean_phoff', 'clean_phres']].bfill(axis=1).iloc[:, 0].fillna('')

# Create a unique fallback identifier based on other customer details if combined_phone is empty
df['fallback_id'] = df.apply(lambda x: f"{x['name']}-{x['mname']}-{x['lname']}-{x['email']}-{x.index}" if x['combined_phone'] == '' else x['combined_phone'], axis=1)

# Use either combined phone or fallback ID as the unique identifier
df['unique_id'] = df['fallback_id']

# Convert date to datetime format for consistency
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')

# Sort by unique_id and date in descending order
df.sort_values(by=['unique_id', 'date'], ascending=[True, False], inplace=True)

# Assign unique customer IDs based on unique_id
df['customer_id'] = pd.factorize(df['unique_id'])[0] + 1

# Group by unique_id and aggregate the first non-null value in each column
aggregated_data = df.groupby('unique_id').first().reset_index()

# Prepare customer DataFrame
customer_columns = [
    'customer_id', 'name', 'mname', 'lname', 'add1', 'add2', 'add3', 'add4', 'email', 'mobile', 'phoff', 'phres', 'date'
]
customers_final = aggregated_data[customer_columns]

# Ensure 'onote' field is clean
df['onote'] = df['onote'].astype(str).replace('\n', ' ', regex=True)

# Prepare the order DataFrame
order_df = df[['orderNo', 'customer_id', 'date', 'onote']].drop_duplicates(subset='orderNo', keep='first')

#Jacket Measurement
jacketMeasurement = df[['customer_id','date','orderNo' , 'jl', 'jnl', 'jbl', 'jxback', 'jtsleeve', 'jhs', 'jchest', 'jwaist', 'scollar','vcoatlen', 'sherlen', 'jothers']].dropna(subset=['customer_id'])
# Drop rows with missing values in measurement columns
jacketMeasurement = jacketMeasurement.dropna(subset=['jl', 'jnl', 'jbl', 'jxback', 'jtsleeve', 'jhs', 'jchest', 'jwaist', 'scollar','vcoatlen', 'sherlen', 'jothers'], how='all')
jacketMeasurement['measurement_id'] = [f"{uuid4()}" for _ in range(len(jacketMeasurement))]  # Generating unique IDs

#Shirt Measurement
shirtMeasurement = df[['customer_id','date','orderNo' , 'slength', 'sshool', 'stosleeve', 'schest', 'swaist', 'scollar', 'sothers']].dropna(subset=['customer_id'])
# Drop rows with missing values in measurement columns
shirtMeasurement = shirtMeasurement.dropna(subset=['slength', 'sshool', 'stosleeve', 'schest', 'swaist', 'scollar', 'sothers'], how='all')
shirtMeasurement['measurement_id'] = [f"{uuid4()}" for _ in range(len(shirtMeasurement))]  # Generating unique IDs

#Pant Measurement
pantMeasurement = df[['customer_id','date','orderNo' , 'plength', 'pinseem', 'pwaist', 'phips', 'pbottom', 'pknee', 'pothers']].dropna(subset=['customer_id'])
# Drop rows with missing values in measurement columns
pantMeasurement = pantMeasurement.dropna(subset=['plength', 'pinseem', 'pwaist', 'phips', 'pbottom', 'pknee', 'pothers'], how='all')
pantMeasurement['measurement_id'] = [f"{uuid4()}" for _ in range(len(pantMeasurement))]  # Generating unique IDs

jacketMeasurement.to_csv(f'./consolidated-data/JacketMeasurement.csv', index=False)
shirtMeasurement.to_csv(f'./consolidated-data/ShirtMeasurement.csv', index=False)
pantMeasurement.to_csv(f'./consolidated-data/PantMeasurement.csv', index=False)

# Save DataFrames
customers_final.to_csv('./consolidated-data/ConsolidatedCustomers.csv', index=False)
order_df.to_csv('./consolidated-data/UpdatedOrders.csv', index=False)
