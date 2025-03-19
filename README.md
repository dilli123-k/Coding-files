# Coding-filesfrom dataclasses import dataclass
from typing import Optional
from datetime import date

@dataclass
class EmployeeRecord:
    employee_id: int
    name: str
    email: str
    department: str
    designation: str
    salary: float
    date_of_joining: date


import asyncio
import aiohttp
import csv
from typing import List
from datetime import datetime
from dataclasses import dataclass

# Define EmployeeRecord data class
@dataclass
class EmployeeRecord:
    employee_id: int
    name: str
    email: str
    department: str
    designation: str
    salary: float
    date_of_joining: datetime

# Function to parse CSV and create EmployeeRecord instances
def parse_csv(file_path: str) -> List[EmployeeRecord]:
    records = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            records.append(EmployeeRecord(
                employee_id=int(row['employee_id']),
                name=row['name'],
                email=row['email'],
                department=row['department'],
                designation=row['designation'],
                salary=float(row['salary']),
                date_of_joining=datetime.strptime(row['date_of_joining'], "%Y-%m-%d")
            ))
    return records

# Asynchronous function to send data to the server
async def send_record(session: aiohttp.ClientSession, url: str, record: EmployeeRecord):
    async with session.post(url, json=record.__dict__) as response:
        return await response.text()

# Main function to process CSV and send records
async def main(csv_file_path: str, server_url: str):
    records = parse_csv(csv_file_path)
    async with aiohttp.ClientSession() as session:
        tasks = [send_record(session, server_url, record) for record in records]
        results = await asyncio.gather(*tasks)
        print(f"Processed {len(results)} records")

# Run the client
if __name__ == "__main__":
    asyncio.run(main('employee_records.csv', 'http://localhost:5000/submit'))

from flask import Flask, request, jsonify
from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Optional
import pymysql
from functools import wraps

# Initialize Flask app
app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)

# MySQL connection settings
DB_HOST = "localhost"
DB_USER = "user"
DB_PASS = "password"
DB_NAME = "employee_db"

# Employee Record Data Class
@dataclass
class EmployeeRecord:
    employee_id: int
    name: str
    email: str
    department: str
    designation: str
    salary: float
    date_of_joining: datetime

# Function decorator for logging and validating incoming records
def log_request(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info("Request received")
        result = func(*args, **kwargs)
        logging.info("Request processed")
        return result
    return wrapper

def validate_record(record):
    if not all([record.get('employee_id'), record.get('name'), record.get('email')]):
        raise ValueError("Missing required fields")

# Insert record into MySQL database
def insert_record_to_db(record: EmployeeRecord):
    connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME)
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO employee (employee_id, name, email, department, designation, salary, date_of_joining)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (record.employee_id, record.name, record.email, record.department, record.designation, record.salary, record.date_of_joining))
    connection.commit()
    connection.close()

# Define POST endpoint for receiving employee records
@app.route('/submit', methods=['POST'])
@log_request
def submit_employee():
    try:
        data = request.get_json()
        employee = EmployeeRecord(**data)
        validate_record(data)
        insert_record_to_db(employee)
        return jsonify({"status": "success", "message": "Employee record stored"}), 200
    except ValueError as ve:
        logging.error(f"Validation error: {ve}")
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"status": "error", "message": "An error occurred"}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
