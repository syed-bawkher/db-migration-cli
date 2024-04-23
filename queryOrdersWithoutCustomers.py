import pandas as pd

# Read the CSV file
df = pd.read_csv('./consolidated-data/UpdatedOrders.csv', low_memory=False)

# Filter out rows where customer_id is null
orders_without_customer_id = df[df['customer_id'].isnull()]

#only show orders after 2008
#orders_without_customer_id = orders_without_customer_id[orders_without_customer_id['date'] > '2007-01-01']


# Print the orders without customer_id
print("Orders without customer_id:")
print(orders_without_customer_id)