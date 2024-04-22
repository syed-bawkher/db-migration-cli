import pandas as pd
import numpy as np

# Load the dataset
df = pd.read_csv('Database.csv', low_memory=False)

# Clean 'orderno' if necessary
df['orderNo'] = df['orderno'].astype(str).str.strip()

# Generate unique initial customer IDs
df['customer_id'] = range(1, len(df) + 1)

# Combine phone numbers into one field, prioritizing mobile, then office, then residential
conditions = [
    (df['mobile'].notna()) & (df['mobile'].str.strip() != ''),
    (df['phoff'].notna()) & (df['phoff'].str.strip() != ''),
    (df['phres'].notna()) & (df['phres'].str.strip() != '')
]
choices = [df['mobile'], df['phoff'], df['phres']]
df['phone'] = np.select(conditions, choices, default=np.nan)

# Convert date to datetime format, specifying format for consistency
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')

# Sort by customer_id and date in descending order to make the latest entries come first
df.sort_values(by=['customer_id', 'date'], ascending=[True, False], inplace=True)

# Define the aggregation function to take the first non-null value in each column
def aggregate_rows(x):
    return x.bfill().iloc[0]


# Group by 'phone' and apply the custom aggregation function
deduped_df = df.groupby('phone').apply(aggregate_rows).reset_index(drop=True)
deduped_df['new_customer_id'] = range(1, len(deduped_df) + 1)

# Map old customer IDs to new ones in the main DataFrame
df['new_customer_id'] = df['phone'].map(deduped_df.set_index('phone')['new_customer_id'])

# Ensure 'customer_id' uses the new ID
df['customer_id'] = df['new_customer_id'].astype('Int64')

# Prepare the final customer DataFrame
customer_columns = [
    'new_customer_id', 'name', 'mname', 'lname', 'add1', 'add2', 'add3', 'add4', 'email', 'phone', 'date'
]
customer_df = deduped_df[customer_columns]

# Ensure 'onote' field is clean
df['onote'] = df['onote'].astype(str).replace('\n', ' ', regex=True)

# Group by 'orderNo' and apply the custom aggregation function
grouped_orders = df.groupby('orderNo').apply(aggregate_rows).reset_index(drop=True)

# Ensure 'customer_id' is unique within each group
grouped_orders['customer_id'] = grouped_orders['customer_id'].astype('Int64')

# Sort by 'orderNo' and 'date' in descending order to make the latest entries come first
grouped_orders.sort_values(by=['orderNo', 'date'], ascending=[True, False], inplace=True)

# Deduplicate by keeping only the first occurrence of each 'orderNo'
grouped_orders.drop_duplicates(subset='orderNo', keep='first', inplace=True)

# Prepare the order DataFrame
order_df = grouped_orders[['orderNo', 'customer_id', 'date', 'onote']]



# Define measurement columns
jacket_measure_columns = ['jl', 'jnl', 'jbl', 'jxback', 'jtsleeve', 'jhs', 'jchest', 'jwaist', 'scollar', 'jothers']
shirt_measure_columns = ['slength', 'sshool', 'stosleeve', 'schest', 'swaist', 'scollar', 'sothers', 'vcoatlen', 'sherlen']
pants_measure_columns = ['plength', 'pinseem', 'pwaist', 'phips', 'pbottom', 'pknee', 'pothers']

# Ensure all columns are present and forward fill them
all_measurement_columns = jacket_measure_columns + shirt_measure_columns + pants_measure_columns
for column in all_measurement_columns:
    if column in df.columns:
        df[column] = df[column].ffill()

# Now proceed to extract the latest measurements for each customer
latest_measurements = df.groupby('customer_id').first().reset_index()

# Now you can safely create your measurement-specific CSV files
latest_jackets = latest_measurements[['customer_id'] + jacket_measure_columns]
latest_shirts = latest_measurements[['customer_id'] + shirt_measure_columns]
latest_pants = latest_measurements[['customer_id'] + pants_measure_columns]

latest_jackets.to_csv('./consolidated-data/JacketMeasurement.csv', index=False)
latest_shirts.to_csv('./consolidated-data/ShirtMeasurement.csv', index=False)
latest_pants.to_csv('./consolidated-data/PantMeasurement.csv', index=False)

# Save the cleaned and updated DataFrames
customer_df.to_csv('./consolidated-data/ConsolidatedCustomers.csv', index=False)
order_df.to_csv('./consolidated-data/UpdatedOrders.csv', index=False)