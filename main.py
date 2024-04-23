import pandas as pd
import re

# Load the dataset
df = pd.read_csv('Database.csv', low_memory=False)

# Clean 'orderno'
df['orderNo'] = df['orderno'].astype(str).str.strip()

# Function to clean phone numbers by removing non-digit characters
def clean_phone(phone):
    return re.sub(r"[^\d]", "", str(phone)) if pd.notna(phone) else ""

# Apply phone number cleaning
df['mobile'] = df['mobile'].apply(clean_phone)
df['phoff'] = df['phoff'].apply(clean_phone)
df['phres'] = df['phres'].apply(clean_phone)

# Combine phone numbers into one field, prioritizing mobile, then office, then residential
df['phone'] = df[['mobile', 'phoff', 'phres']].bfill(axis=1).iloc[:, 0].fillna('')

# Create a unique fallback identifier based on other customer details if phone is empty
df['fallback_id'] = df.apply(lambda x: f"{x['name']}-{x['mname']}-{x['lname']}-{x['email']}-{x.index}" if x['phone'] == '' else x['phone'], axis=1)

# Use either phone or fallback ID as the unique identifier
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
    'customer_id', 'name', 'mname', 'lname', 'add1', 'add2', 'add3', 'add4', 'email', 'phone', 'date'
]
customers_final = aggregated_data[customer_columns]

# Ensure 'onote' field is clean
df['onote'] = df['onote'].astype(str).replace('\n', ' ', regex=True)

# Prepare the order DataFrame
order_df = df[['orderNo', 'customer_id', 'date', 'onote']].drop_duplicates(subset='orderNo', keep='first')

# Define measurement columns and prepare measurement tables
measure_columns = {
    'JacketMeasurement.csv': ['customer_id', 'jl', 'jnl', 'jbl', 'jxback', 'jtsleeve', 'jhs', 'jchest', 'jwaist', 'scollar', 'jothers'],
    'ShirtMeasurement.csv': ['customer_id', 'slength', 'sshool', 'stosleeve', 'schest', 'swaist', 'scollar', 'vcoatlen', 'sherlen', 'sothers'],
    'PantMeasurement.csv': ['customer_id', 'plength', 'pinseem', 'pwaist', 'phips', 'pbottom', 'pknee', 'pothers']
}

for file, columns in measure_columns.items():
    measurements = df[columns].dropna(subset=['customer_id'])  # Assume measurements are only taken when customer_id is available
    measurements.to_csv(f'./consolidated-data/{file}', index=False)

# Save DataFrames
customers_final.to_csv('./consolidated-data/ConsolidatedCustomers.csv', index=False)
order_df.to_csv('./consolidated-data/UpdatedOrders.csv', index=False)
