import pandas as pd

# Load the dataset
df = pd.read_csv('./consolidated-data/UpdatedOrders.csv', low_memory=False)

# Group by 'customer_id', count the number of orders, and sort by count in descending order
order_counts = df.groupby('customer_id').size().reset_index(name='order_count')
order_counts = order_counts.sort_values(by='order_count', ascending=False)

# Save the sorted results to a new CSV file
order_counts.to_csv('./queries/customer_order_counts.csv', index=False)
print(f"Order counts have been saved to {'./queries/customer_order_counts.csv'}.")
