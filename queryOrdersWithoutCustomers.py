import pandas as pd

# Read the CSV file
df = pd.read_csv('./consolidated-data/UpdatedOrders.csv', low_memory=False)

# Filter out rows where customer_id is null
orders_without_customer_id = df[df['customer_id'].isnull()]

# Print the orders without customer_id
print("Orders without customer_id:")
print(orders_without_customer_id)