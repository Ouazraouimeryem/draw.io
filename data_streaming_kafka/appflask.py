from flask import Flask, request, jsonify
import pymysql
from kafka.admin import KafkaAdminClient,NewTopic
from kafka import KafkaProducer, KafkaConsumer
import json
import requests
import logging
logging.basicConfig(level=logging.DEBUG)



app = Flask(__name__)


# Kafka admin client setup
admin_client = KafkaAdminClient(bootstrap_servers="kafka:9093", client_id='test')

# Check if the topic exists
existing_topics = admin_client.list_topics()
if 'topic' not in existing_topics:
    topic_list = []
    new_topic = NewTopic(name="topic", num_partitions=2, replication_factor=1)
    topic_list.append(new_topic)
    admin_client.create_topics(new_topics=topic_list)
    print("Topic 'topic' created.")
else:
    print("Topic 'topic' already exists.")

# Kafka producer
producer = KafkaProducer(bootstrap_servers='kafka:9093', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer
consumer = KafkaConsumer('topic',
                         bootstrap_servers=['kafka:9093'],
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         consumer_timeout_ms=1000
                         )  # Set timeout to 1 second 



# Initial connection to MySQL to create the database if it doesn't exist
con = pymysql.connect(
    host='mysql',
    port=3306,
    user='root',
    password=''
)
with con.cursor() as cursor:
    cursor.execute("CREATE DATABASE IF NOT EXISTS test")
con.commit()
con.close()

# Reconnect to MySQL with the specified database
con = pymysql.connect(
    host='mysql',
    port=3306,
    user='root',
    password='',
    database='test'
)
# Create database and table if they don't exist
# Create table if not exists
with con.cursor() as cursor:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bd_coupa (
            id INT AUTO_INCREMENT PRIMARY KEY,
            item VARCHAR(255),
            supplier VARCHAR(255),
            price DECIMAL(10, 2),
            quantitie INT,
            status VARCHAR(50),
            model_name VARCHAR(255),
            category VARCHAR(100),
            category_type VARCHAR(100)              
        )
    """)
    con.commit()

# Get API_URL and API_TOKEN from environment variables
import os
api_url = 'http://host.docker.internal/api/v1'
api_token= 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxIiwianRpIjoiZmJiNWU0NjdhNmQxNTQ5NGUzNzRlM2EzZjA2MDFmNGFkNDI5N2NkYmNlY2FlZWJiNWViODQ5MDRhZjVjODg4N2Y5NDJhYzFhMjRiNDViNTgiLCJpYXQiOjE3MTU3Mjc2ODMuODQ2NzY4LCJuYmYiOjE3MTU3Mjc2ODMuODQ2Nzc1LCJleHAiOjIxODkwMjY4ODMuNzQ2MjkzLCJzdWIiOiIxIiwic2NvcGVzIjpbXX0.Z2KKrCYqlCwsRPRLGbw31QbqZUpDT15BQ0TNoVu8vqTsuDzJTON74opnDaaFNJiHaj6aJ-7xTFo9NvvDLNLdVtaJRa6EELdpVP4VJUwW3SP9O7pZKQmKdBtXdRUoqOrVi2N7YlabLylHBHxOpGcUWYGAwFUwM8lVABWEgmV1YxW-2HJqFl6ajcQDYsN2tuCaoj6J78Tzb-BPqtJdGJNUr8nlKJxeJ02LhBrXGov4R-t_2IzQU7cx6eLGWZG84ZGOcrJ46kUHXwl9A9UrYvcIFZyZThQX7_s0aPtyWcgQLWWdCxHOcTvBDLwsI5akmBLdsIWdBd0MMbJCZi6qg3dTb71aqBESScm_mec-kY9EMPP84GtPir5LuHrYMLyWj44MkrJwS4oBh0DM4DxeaQG-L2IT5281u4ibuTFZw9ljedXHkz5YdZAQ5RjRDds1FVOCtGHO4XSrrOOQsnyZLvaPwZ3LOP5-CNCkMys-yRBmu1a4h2u5E_Nu7C0JhEpctIzxwiplmcsRfxNbu5Z4dFk0oc_qGS0L4fJKufqapC5fkcfFCPPmX_OPVgYrtmUe0XUaPSa3NwYh2PbMkyuEIM4r27GGkLz-ZHgARzJzfzxr4Hf4hgeQUfAKZSEXbtgjYY80hbriir9EXs7R9mJs8K1swGw173RLxYLOAsjwaljvtko'




def authenticate():
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json"
    }
    return headers

def create_category(category_type, category):
    url = f"{api_url}/categories"
    headers = authenticate()
    data = {
        "name": category,
        "category_type": category_type
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Ensure we catch any HTTP errors
        if response.status_code == 200:
            print("Category created")
        else:
            print("Failed to create category:", response.text)
    except requests.exceptions.RequestException as e:
        print("Error creating category:", str(e))

def create_model(model_name,category_id):
    url = f'{api_url}/models'
    headers = authenticate()
    data = {
    "name": model_name,
    "category_id": category_id
    } 
    try:
        response = requests.post(url,headers=headers,json=data)
        if response.status_code == 200:
            print("model created")
        else:
            print("Failed to create model:", response.text)
    except Exception as e:
        print("Error created model:", str(e))


def get_model_id(model_name, headers):
    try:
        headers = authenticate()
        url = f"{api_url}/models"
        params = {
            "search": model_name
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if "rows" in data and len(data["rows"]) > 0:
            return data["rows"][0]["id"]
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None

def get_status_id(status_name, headers):
    try:
        url = f"{api_url}/statuslabels"
        params = {
            "search": status_name
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if "rows" in data and len(data["rows"]) > 0:
            return data["rows"][0]["id"]
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None

def get_category_id(category, headers):
    try:
        url = f"{api_url}/categories"
        params = {
            "search": category
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if "rows" in data and len(data["rows"]) > 0:
            
            return data["rows"][0]["id"]
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None



last_status = {"status": "unknown"}

@app.route('/check_send_status', methods=['GET'])
def check_send_status():
    global last_status
    return jsonify(last_status)

# Function to send data to another system via API
def send_to_other_system(data):
    global last_status
    url = 'http://host.docker.internal/api/v1/hardware'
    headers = authenticate()
    print(data)
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            last_status = {"status": "success"}
            check_send_status()
            print("Data sent to other system successfully")
            logging.debug("Data sent to other system successfully")
        else:
            last_status = {"status": "failed"}
            check_send_status()
            print("Failed to send data to other system:", response.text)
            logging.debug("Failed to send data to other system:", response.text)
    except Exception as e:
        last_status = {"status": "failed"}
        print("Error sending data to other system:", str(e))
        logging.debug("Error sending data to other system:", str(e))

@app.route('/post', methods=['POST'])
def post():
    data = request.get_json()
    item = data['item']
    supplier = data['supplier']
    price = data['price']
    quantitie = data['quantitie']
    status = data['status']
    model_name = data['model_name']
    category = data['category']
    category_type = data['category_type']

    headers = authenticate()
    model_id = get_model_id(model_name, headers)
    status_id = get_status_id(status, headers)
    category_id = get_category_id(category, headers)

    cursor = con.cursor()
    try:
        cursor.execute("INSERT INTO bd_coupa (item, supplier, price, quantitie,status , model_name , category , category_type) VALUES (%s, %s, %s, %s ,%s,%s,%s,%s)", (item, supplier, price, quantitie, status, model_name, category, category_type))
        con.commit()
        logging.debug("mysql set up successfully, Message sent to database:", {"model_id": model_id , "status_id": status_id})

        if model_id is None:
          if category_id is None:
              create_category(category_type,category)
              category_id = get_category_id(category,headers)
              print(category_id)
              create_model(model_name,category_id)
              model_id=get_model_id(model_name, headers)
          else:    
              create_model(model_name,category_id)
              model_id=get_model_id(model_name, headers)
              print("model------------")
        else:
          model_id

        # Produce message to Kafka
        producer.send('topic', value={"model_id": model_id, "status_id": status_id})
        producer.flush()
        print("Message sent to Kafka:", {"model_id": model_id, "status_id": status_id})
        logging.debug("Kafka producer set up successfully, Message sent to Kafka:", {"model_id": model_id , "status_id": status_id})
        # Consume only the latest message from Kafka
        latest_data = None
        for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            latest_data = data
            print("Message received from Kafka:", data)
            logging.debug("Kafka consumer set up successfully,Message received from Kafka:", data)
            break  # Exit the loop after processing the first message

        if latest_data:
            send_to_other_system(latest_data)  # Send only the latest data to another system via API
            logging.debug(latest_data)
            return "POSTED"
        else:
            return "No data received from Kafka"

    except Exception as e:
        con.rollback()
        return str(e)

@app.route('/get', methods=['GET'])
def get():
    cursor = con.cursor(pymysql.cursors.DictCursor)
    try:
        cursor.execute("SELECT * FROM bd_cp")
        rows = cursor.fetchall()
        return jsonify(rows)
    except Exception as e:
        return str(e)



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)