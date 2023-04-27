import csv
import json
import time
from kafka import KafkaProducer




csv_file_path = "newdata.csv"
batch_size = 22

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

with open(csv_file_path, newline='') as csv_file:
    csv_reader = csv.reader(csv_file)
    headers = next(csv_reader) # skip headers
    row_count = sum(1 for row in csv_reader)
    csv_file.seek(0) # rewind file pointer to start
    next(csv_reader) # skip headers again
    
    current_row = 0
    while current_row < row_count:
        print(f"Sending rows {current_row+1} to {min(current_row+batch_size, row_count)}:")
        for i in range(current_row, current_row+batch_size):
            try:
                row = next(csv_reader)
                data = {
                    'PLANT_ID': float(row[0]),
                    'SOURCE_KEY': row[1],
                    'DC_POWER': float(row[2]),
                    'AC_POWER': float(row[3]),
                    'DAILY_YIELD': float(row[4]),
                    'TOTAL_YIELD': float(row[5])
                }
                topic_name = row[1] # use SOURCE_KEY as topic name
                producer.send(topic_name, value=data)
            except StopIteration:
                break
        current_row += batch_size
        # time.sleep(3)

producer.close()