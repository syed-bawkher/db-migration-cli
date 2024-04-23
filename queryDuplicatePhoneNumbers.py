import pandas as pd
import re

# Load the dataset
df = pd.read_csv('./consolidated-data/ConsolidatedCustomers.csv')

# Function to clean phone numbers
def clean_phone(phone):
    if pd.notna(phone):
        # Remove all characters except digits
        phone = re.sub(r"[^\d]", "", phone)
    return phone

# Apply phone number cleaning
df['phone'] = df['phone'].apply(clean_phone)

# Clean emails by trimming whitespace and converting to lowercase
df['email'] = df['email'].str.strip().str.lower()

# Group by 'phone' and 'email' separately and count occurrences
phone_grouped = df.groupby('phone').size()
email_grouped = df.groupby('email').size()

# Filter groups that have more than one occurrence (duplicates)
duplicate_phones = phone_grouped[phone_grouped > 1].index
duplicate_emails = email_grouped[email_grouped > 1].index

# Select rows where the cleaned phone number or email is in the list of duplicates
duplicate_customers_phone = df[df['phone'].isin(duplicate_phones)]
duplicate_customers_email = df[df['email'].isin(duplicate_emails)]

# Sort by phone and date to better visualize duplicates
duplicate_customers_phone.sort_values(by=['phone', 'date'], inplace=True)
duplicate_customers_email.sort_values(by=['email', 'date'], inplace=True)

# Display the customers with duplicate phone numbers
print("Customers with Duplicate Phone Numbers:")
print(duplicate_customers_phone)
print("\nCustomers with Duplicate Emails:")
print(duplicate_customers_email)
