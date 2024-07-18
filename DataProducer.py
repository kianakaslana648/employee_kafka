import csv
import json
from confluent_kafka import Producer
from datetime import datetime

def read_and_transform_csv(file_path):
    data = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['Department'] in ['ECC', 'CIT', 'EMS'] and datetime.strptime(row['Initial Hire Date'], '%d-%b-%Y').year > 2010:
                row['Salary'] = int(float(row['Salary']))
                data.append(row)
    return data

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def send_to_kafka(data, topic):
    conf = {'bootstrap.servers': 'localhost:29092'}
    producer = Producer(**conf)

    for record in data:
        producer.produce(topic, key=None, value=json.dumps(record).encode('utf-8'), callback=delivery_report)
        producer.poll(0)
    producer.flush()

if __name__ == "__main__":
    file_path = 'resources/Employee_Salaries_1.csv'
    data = read_and_transform_csv(file_path)
    send_to_kafka(data, 'employee_salaries')
