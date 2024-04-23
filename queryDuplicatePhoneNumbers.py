import pandas as pd
import re

# Load the dataset
df = pd.read_csv('./consolidated-data/ConsolidatedCustomers.csv')

# Function to clean phone numbers by removing non-digit characters
def clean_phone(phone):
    if pd.isna(phone):
        return None  # Directly return None if phone is NaN
    phone = re.sub(r"[^\d]", "", str(phone))
    return phone if phone.strip() else None  # Return None if phone is empty after cleaning

# Apply phone number cleaning
df['mobile'] = df['mobile'].apply(clean_phone)
df['phoff'] = df['phoff'].apply(clean_phone)
df['phres'] = df['phres'].apply(clean_phone)

# Combine phone numbers into one field, prioritizing mobile, then office, then residential
df['combined_phone'] = df[['mobile', 'phoff', 'phres']].fillna('').agg(' '.join, axis=1).str.strip()
df['combined_phone'] = df['combined_phone'].apply(lambda x: None if x == '' else x)

# Clean emails by trimming whitespace and converting to lowercase
df['email'] = df['email'].str.strip().str.lower()

# Filter out rows where both combined phone and email are empty
df = df.dropna(subset=['combined_phone', 'email'], how='all')

# Group by 'combined_phone' and 'email' separately and count occurrences
phone_grouped = df.groupby('combined_phone').size()
email_grouped = df.groupby('email').size()

# Filter groups that have more than one occurrence (duplicates)
duplicate_phones = phone_grouped[phone_grouped > 1].index
duplicate_emails = email_grouped[email_grouped > 1].index

# Select rows where the combined phone number or email is in the list of duplicates
duplicate_customers_phone = df[df['combined_phone'].isin(duplicate_phones)]
duplicate_customers_email = df[df['email'].isin(duplicate_emails)]

# Sort by combined_phone and email to better visualize duplicates
duplicate_customers_phone.sort_values(by=['combined_phone', 'date'], inplace=True)
duplicate_customers_email.sort_values(by=['email', 'date'], inplace=True)

# Display the customers with duplicate phone numbers and emails
print("Customers with Duplicate Phone Numbers:")
print(duplicate_customers_phone[['customer_id', 'name', 'mname', 'lname', 'email', 'mobile', 'phoff', 'phres', 'combined_phone', 'date']])
print("\nCustomers with Duplicate Emails:")
print(duplicate_customers_email[['customer_id', 'name', 'mname', 'lname', 'email', 'mobile', 'phoff', 'phres', 'combined_phone', 'date']])
