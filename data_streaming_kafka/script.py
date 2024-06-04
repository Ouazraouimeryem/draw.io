from flask import Flask, request ,jsonify
import pymysql
from kafka import KafkaProducer ,KafkaConsumer
import json
import requests
app = Flask(__name__)

# Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer
consumer = KafkaConsumer('topic',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         consumer_timeout_ms=1000)  # Set timeout to 1 second 


con = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    database='test'
)
api_url = 'http://localhost/api/v1'
api_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxIiwianRpIjoiZmJiNWU0NjdhNmQxNTQ5NGUzNzRlM2EzZjA2MDFmNGFkNDI5N2NkYmNlY2FlZWJiNWViODQ5MDRhZjVjODg4N2Y5NDJhYzFhMjRiNDViNTgiLCJpYXQiOjE3MTU3Mjc2ODMuODQ2NzY4LCJuYmYiOjE3MTU3Mjc2ODMuODQ2Nzc1LCJleHAiOjIxODkwMjY4ODMuNzQ2MjkzLCJzdWIiOiIxIiwic2NvcGVzIjpbXX0.Z2KKrCYqlCwsRPRLGbw31QbqZUpDT15BQ0TNoVu8vqTsuDzJTON74opnDaaFNJiHaj6aJ-7xTFo9NvvDLNLdVtaJRa6EELdpVP4VJUwW3SP9O7pZKQmKdBtXdRUoqOrVi2N7YlabLylHBHxOpGcUWYGAwFUwM8lVABWEgmV1YxW-2HJqFl6ajcQDYsN2tuCaoj6J78Tzb-BPqtJdGJNUr8nlKJxeJ02LhBrXGov4R-t_2IzQU7cx6eLGWZG84ZGOcrJ46kUHXwl9A9UrYvcIFZyZThQX7_s0aPtyWcgQLWWdCxHOcTvBDLwsI5akmBLdsIWdBd0MMbJCZi6qg3dTb71aqBESScm_mec-kY9EMPP84GtPir5LuHrYMLyWj44MkrJwS4oBh0DM4DxeaQG-L2IT5281u4ibuTFZw9ljedXHkz5YdZAQ5RjRDds1FVOCtGHO4XSrrOOQsnyZLvaPwZ3LOP5-CNCkMys-yRBmu1a4h2u5E_Nu7C0JhEpctIzxwiplmcsRfxNbu5Z4dFk0oc_qGS0L4fJKufqapC5fkcfFCPPmX_OPVgYrtmUe0XUaPSa3NwYh2PbMkyuEIM4r27GGkLz-ZHgARzJzfzxr4Hf4hgeQUfAKZSEXbtgjYY80hbriir9EXs7R9mJs8K1swGw173RLxYLOAsjwaljvtko'

def authenticate():
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json"
    }
    return headers

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
            print(data["rows"][0]["name"])
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




# Function to send data to another system via API
def send_to_other_system(data):
    # Replace the URL below with the endpoint of the other system's API
    url = 'http://localhost/api/v1/hardware'
    headers = authenticate()
    print(data)
    try:
        response = requests.post(url,headers=headers,json=data)
        if response.status_code == 200:
            print("Data sent to other system successfully")
        else:
            print("Failed to send data to other system:", response.text)
    except Exception as e:
        print("Error sending data to other system:", str(e))

@app.route('/post', methods=['POST'])
def post():
    data = request.get_json()
    item = data['item']
    supplier = data['supplier']
    price = data['price']
    quantitie = data['quantitie']
    status = data['status']
    model_name = data['model_name']

    

    headers = authenticate()
    model_id=get_model_id(model_name, headers)
    status_id=get_status_id(status, headers)

    cursor = con.cursor()
    try:
        cursor.execute("INSERT INTO bd_cp (item, supplier, price, quantitie,status , model_name) VALUES (%s, %s, %s, %s ,%s,%s)", (item, supplier, price, quantitie,status ,model_name ))
        con.commit()

         # Produce message to Kafka
        producer.send('topic', value={"model_id": model_id, "status_id": status_id})
        producer.flush()
        
        
        # Consume only the latest message from Kafka
        latest_data = None
        for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            latest_data = data
            break  # Exit the loop after processing the first message

        if latest_data:
            send_to_other_system(latest_data)  # Send only the latest data to another system via API
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
        cursor.execute("SELECT * FROM bdnew")
        rows = cursor.fetchall()
        return jsonify(rows)
    except Exception as e:
        return str(e)   

if __name__ == '__main__':
    app.run(debug=True)
