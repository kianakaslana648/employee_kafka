import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

def parse_date(date_str):
    """Parse the date string into a date object in YYYY-MM-DD format."""
    return datetime.strptime(date_str, '%d-%b-%Y').date()

def insert_into_db(record):
    conn = None
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        # Insert into department_employee
        cur.execute(
            """
            INSERT INTO department_employee (
                department, department_division, position_title, hire_date, salary
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                record['Department'],
                record['Department-Division'],
                record['Position Title'],
                parse_date(record['Initial Hire Date']),
                record['Salary']
            )
        )

        # Update department_employee_salary
        cur.execute(
            """
            INSERT INTO department_employee_salary (department, total_salary)
            VALUES (%s, %s)
            ON CONFLICT (department) DO UPDATE SET total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary
            """,
            (
                record['Department'],
                record['Salary']
            )
        )

        # Commit the changes
        conn.commit()
        cur.close()
        print('Inserted record: ', record)
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

def consume_from_kafka(topic):
    conf = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'department_salary_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**conf)
    consumer.subscribe([topic])

    print("Consumer started, waiting for messages...")

    while True:
        msg = consumer.poll(1.0)  # Increased poll timeout
        if msg is None:
            print("No message received in this poll.")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("End of partition reached {} [{}] at offset {}.".format(msg.topic(), msg.partition(), msg.offset()))
                continue
            else:
                print("Error: ", msg.error())
                break

        try:
            message_value = msg.value().decode('utf-8')
            print('Consumed message: ', message_value)
            record = json.loads(message_value)
            print('Decoded record: ', record)
            insert_into_db(record)
        except Exception as e:
            print(f"Error processing message: {e}")

    consumer.close()

if __name__ == "__main__":
    consume_from_kafka('employee_salaries')

