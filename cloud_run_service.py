import base64
from google.cloud import bigquery
from datetime import datetime

from flask import Flask, request

def data_preprocessing(data):
    if len(data.split("."))==1:
        return datetime.fromtimestamp(float(data))
    return data


client = bigquery.Client()
table_id = "kp031-traineeship-catello."
app = Flask(__name__)
@app.route("/measure", methods=["POST"])
def index():
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]
    
    
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        message = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
        split = message.split(",")
        row = {{'car_id':split[0],
               'ts':data_preprocessing(split[1]),
               'sensor_id:':split[2],
               'value':split[3]
        }}
        errors = client.insert_rows_json(table_id, row)
        if len(errors)!=0:
            print("Error during row insertion:", errors)
        

    return ("", 204)