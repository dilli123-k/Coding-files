# Data extraction:
import pandas as pd

def extract_data(file_path, password):
    # Use pandas to read the CSV file and handle password protection
    # Assuming the files are accessible directly via URL
    data = pd.read_csv(file_path, password=password)
    return data

data_region_a = extract_data("https://sharepoint.com/order_region_a.csv", "order_region_a")
data_region_b = extract_data("https://sharepoint.com/order_region_b.csv", "order_region_b")

# Transform Data
def transform_data(data_a, data_b):
     
    data_a['region'] = 'A'
    data_b['region'] = 'B'
    
    # Concatenate data from both regions
    data = pd.concat([data_a, data_b])
    
    # Calculate total_sales
    data['total_sales'] = data['QuantityOrdered'] * data['ItemPrice']
    data['net_sale'] = data['total_sales'] - data['PromotionDiscount']
    
     
    data = data[data['net_sale'] > 0]
    
    # Remove duplicates based on OrderId
    data = data.drop_duplicates(subset='OrderId')
    
    return data

transformed_data = transform_data(data_region_a, data_region_b)

# Load data
import sqlite3

def load_data_to_db(data, db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create sales_data table
    cursor.execute('''CREATE TABLE IF NOT EXISTS sales_data (OrderId INTEGER PRIMARY KEY, OrderItemId INTEGER,
            QuantityOrdered INTEGER,
            ItemPrice REAL,
            PromotionDiscount REAL,
            total_sales REAL,
            region TEXT,
            net_sale REAL
        )
    ''')

    # Insert data into the sales_data table
    for index, row in data.iterrows():
        cursor.execute('''
            INSERT INTO sales_data (OrderId, OrderItemId, QuantityOrdered, ItemPrice, 
            PromotionDiscount, total_sales, region, net_sale)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row['OrderId'], row['OrderItemId'], row['QuantityOrdered'], row['ItemPrice'], 
              row['PromotionDiscount'], row['total_sales'], row['region'], row['net_sale']))
    
    conn.commit()
    conn.close()

load_data_to_db(transformed_data)

# SQL quaries and validation
def validate_data(db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 1. Count the total number of records
    cursor.execute('SELECT COUNT(*) FROM sales_data')
    total_records = cursor.fetchone()[0]
    print(f"Total records: {total_records}")

    # 2. Find total sales amount by region
    cursor.execute('SELECT region, SUM(total_sales) FROM sales_data GROUP BY region')
    region_sales = cursor.fetchall()
    for region, total_sales in region_sales:
        print(f"Total sales in {region}: {total_sales}")

    # 3. Find the average sales amount per transaction
    cursor.execute('SELECT AVG(net_sale) FROM sales_data')
    avg_sales = cursor.fetchone()[0]
    print(f"Average sales per transaction: {avg_sales}")

    # 4. Ensure no duplicate OrderId values
    cursor.execute('SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1')
    duplicates = cursor.fetchall()
    if duplicates:
        print(f"Duplicate OrderIds: {duplicates}")
    else:
        print("No duplicate OrderIds found.")

    conn.close()


validate_data()
