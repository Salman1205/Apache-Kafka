from kafka import KafkaConsumer
import json
import subprocess

# Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic = 'topic1'
group_id = 'son_consumer_group'
hdfs_path = '/input/asg.txt'  # Change this to your desired HDFS path

# Function to append data to HDFS file using Hadoop command
def append_to_hdfs(data):
    # Construct Hadoop append command
    command = ['hadoop', 'fs', '-appendToFile', '-', hdfs_path]
    # Execute command and pass data through stdin
    subprocess.run(command, input=data.encode('utf-8'), check=True)

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    price_data = json.dumps(message.value)
    # Append data to HDFS file
    append_to_hdfs(price_data + '\n')
    print('Received and appended to HDFS:', price_data)
