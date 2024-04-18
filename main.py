import pandas as pd

# Load the dataset, specify dtype for problematic columns or set low_memory=False
df = pd.read_csv('Database.csv', low_memory=False)

# Generate unique IDs for customers, jackets, shirts, and pants
df['customer_id'] = range(1, len(df) + 1)

# Correct the column names and initialize missing columns
df['jacket_m_id'] = df['jacket'].notna().cumsum()
df['orderNo'] = df['orderno']

# Create the Customer table
customer_columns = ['customer_id', 'cusno', 'name', 'mname', 'lname', 'add1', 'add2', 'add3', 'add4', 'email', 'mobile', 'phoff', 'phres', 'date']
customer_df = df[customer_columns].copy()

# Create the Order table
order_df = df[['orderNo', 'customer_id', 'date']].copy()

# Creating the Jacket table, assuming measurement details need to be copied correctly
if 'jacket' in df.columns:
    jacket_columns = ['customer_id', 'date', 'jacket_m_id']
    jacket_df = df[df['jacket'].notna()][jacket_columns].copy()
    jacket_df['jacket_id'] = df['jacket_m_id']
    
    jacket_measure_columns = ['jacket_m_id', 'date', 'jname', 'jl', 'jnl', 'jbl', 'jxback', 'jtosloove', 'jhs', 'jchest', 'jwaist', 'scollar', 'jothers']
    jacket_measure_df = df[df['jacket'].notna()][jacket_measure_columns].copy()

# Save the dataframes to CSVs
customer_df.to_csv('Customer.csv', index=False)
order_df.to_csv('Order.csv', index=False)
if 'jacket' in df.columns:
    jacket_df.to_csv('Jacket.csv', index=False)
    jacket_measure_df.to_csv('JacketMeasure.csv', index=False)

# Add similar sections for shirts and pants
