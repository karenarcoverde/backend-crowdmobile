from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine
from env_variables import *
import re
import math
from datetime import datetime


app = Flask(__name__)

parts = DATABASE_URL.split("://")
protocol = parts[0]

user_pass, rest = parts[1].split("@")
postgres_user, postgres_pass = user_pass.split(":")

url_port_db, _ = rest.split("?")
postgres_url, port_db = url_port_db.split(":")
postgres_port, postgres_database = port_db.split("/")

engine = create_engine(f"postgresql://{postgres_user}:{postgres_pass}@{postgres_url}:{postgres_port}/{postgres_database}")

def is_valid(value):
    return pd.notna(value) and value != ""

def convert_to_geojson(df, column):
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    print(column)
    df = df[df.apply(lambda row: is_valid(row['CLIENT_LATITUDE']) and 
                                  is_valid(row['CLIENT_LONGITUDE']) and 
                                  is_valid(row[column]), axis=1)]
    
    for index, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['CLIENT_LONGITUDE'], row['CLIENT_LATITUDE']]
            },
            "properties": {
                "intensity": row[column]
            }
        }
        geojson['features'].append(feature)

    return jsonify(geojson)

@app.route('/get_columns_table', methods=['GET'])
def get_columns_table_names():
    try:
        with open('get_columns_table.sql', 'r') as file:
            query = file.read()

        df = pd.read_sql(query, engine)
        
        return jsonify(df.to_dict(orient='records'))
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
@app.route('/get_filters', methods=['GET'])
def get_filters():
    try:
        with open('get_filters.sql', 'r') as file:
            query = file.read()
            
        df = pd.read_sql(query, engine)
        
        
        return jsonify(df.to_dict(orient='records'))
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
@app.route('/generate_heatmap_byfilter', methods=['POST'])
def generate_heatmap_byfilter():
    try:
        data = request.get_json()

        required_parameters = ['start_date', 'end_date', 'TEST_CARRIER_A', 'BRAND', 'DEVICE', 'HARDWARE', 'MODEL', 'column']

        for param in required_parameters:
            if data.get(param, None) is None:
                return jsonify({'error': f'Parameter is missing'}), 400

        start_date = data['start_date']
        end_date = data['end_date']
        test_carrier_a = data['TEST_CARRIER_A']
        brand = data['BRAND']
        device = data['DEVICE']
        hardware = data['HARDWARE']
        model = data['MODEL']
        column = data['column']

        with open('get_heatmap_byfilter.sql', 'r') as file:
            query = file.read()
        
        query = query.format('"' + column + '"')
    
        df = pd.read_sql(query, engine)
        df = df.dropna(subset=['CLIENT_LONGITUDE', 'CLIENT_LATITUDE','TEST_DATE', column])
        if start_date != "" and end_date != "":
            format = "%Y-%m-%d %H:%M:%S"
            start_datetime = str(datetime.strptime(start_date, format))
            end_datetime = str(datetime.strptime(end_date, format))
            df = df[(df['TEST_DATE'] >= start_datetime) & (df['TEST_DATE'] <= end_datetime)]
        if test_carrier_a != "":
            if ',' in test_carrier_a:
                test_carrier_a = test_carrier_a.strip(',')
                for index in range(len(test_carrier_a)):
                    df = df[df['TEST_CARRIER_A'] == test_carrier_a[index]]
            else:
                df = df[df['TEST_CARRIER_A'] == test_carrier_a]
           
        if brand != "":
            if ',' in brand:
                brand = brand.strip(',')
                for index in range(len(brand)):
                    df = df[df['BRAND'] == brand[index]]
            else:
                df = df[df['BRAND'] == brand]
          
        if device != "":
            if ',' in device:
                device = device.strip(',')
                for index in range(len(device)):
                    df = df[df['DEVICE'] == device[index]]
            else:
                df = df[df['DEVICE'] == device]
            
        if hardware != "":
            if ',' in hardware:
                hardware = hardware.strip(',')
                for index in range(len(hardware)):
                    df = df[df['HARDWARE'] == hardware[index]]
            else:
                df = df[df['HARDWARE'] == hardware]
          
        if model != "":
            if ',' in model:
                model = model.strip(',')
                for index in range(len(model)):
                    df = df[df['MODEL'] == model[index]]
            else:
                df = df[df['MODEL'] == model]
            
        df = df.drop(columns=['TEST_DATE','TEST_CARRIER_A','BRAND','DEVICE','HARDWARE','MODEL'])    
        print(df)
        convert_to_geojson(df, column)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/execute_sql', methods=['POST'])
def execute_sql():
    data = request.get_json()
    if not data or 'query' not in data:
        return jsonify({'error': 'Query not provided'}), 400

    query = data['query']

    prohibited_keywords = ['insert', 'update', 'delete', 'drop', 'alter', 'grant', 'revoke', 'execute', 'truncate',
    'create', 'commit', 'rollback', 'savepoint','do', 'call', 'lock', 'reindex']

    if any(re.match(f"^{keyword}", query.lower()) for keyword in prohibited_keywords):
        return jsonify({'error': 'Prohibited query type'}), 400
    
    if not query.strip().lower().startswith('select'):
        return jsonify({'error': 'Only SELECT queries are allowed'}), 400

    try:
        df = pd.read_sql(query, engine)
        print(df)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == "__main__":
    app.run(debug=False)