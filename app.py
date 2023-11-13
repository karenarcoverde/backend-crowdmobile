from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine
from env_variables import *
import re
from datetime import datetime
from flask_cors import CORS
from flask_compress import Compress


app = Flask(__name__)
CORS(app)
Compress(app)

parts = DATABASE_URL.split("://")
protocol = parts[0]

user_pass, rest = parts[1].split("@")
postgres_user, postgres_pass = user_pass.split(":")

url_port_db, _ = rest.split("?")
postgres_url, port_db = url_port_db.split(":")
postgres_port, postgres_database = port_db.split("/")

engine = create_engine(f"postgresql://{postgres_user}:{postgres_pass}@{postgres_url}:{postgres_port}/{postgres_database}")

def convert_to_geojson(df, intensity, longitud, latitud):
    df = df.dropna(subset=[longitud, latitud, intensity])
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    for index, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row[longitud], row[latitud]]
            },
            "properties": {
                "intensity": row[intensity]
            }
        }
        geojson['features'].append(feature)

    return jsonify(geojson)

def filter_column(df, column_value, column_name):
    if column_value != "":
            if ',' in column_value:
                column_value = column_value.strip(',')
                for index in range(len(column_value)):
                    df = df[df[column_name] == column_value[index]]
            else:
                df = df[df[column_name] == column_value]
    return df

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

        required_parameters = ['start_date', 'end_date', 'TEST_CARRIER_A', 'BRAND', 'DEVICE', 'HARDWARE', 'MODEL', 'intensity']

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
        intensity = data['intensity']

        with open('get_heatmap_byfilter.sql', 'r') as file:
            query = file.read()
        
        query = query.format('"' + intensity + '"')
    
        df = pd.read_sql(query, engine)
        df = df.dropna(subset=['CLIENT_LONGITUDE', 'CLIENT_LATITUDE','TEST_DATE', intensity])
        if start_date != "" and end_date != "":
            format = "%Y-%m-%d %H:%M:%S"
            start_datetime = str(datetime.strptime(start_date, format))
            end_datetime = str(datetime.strptime(end_date, format))
            df = df[(df['TEST_DATE'] >= start_datetime) & (df['TEST_DATE'] <= end_datetime)]

        df = filter_column(df, test_carrier_a, 'TEST_CARRIER_A')
        df = filter_column(df, brand, 'BRAND')
        df = filter_column(df, device, 'DEVICE')
        df = filter_column(df, hardware, 'HARDWARE')
        df = filter_column(df, model, 'MODEL')
            
        df = df.drop(columns=['TEST_DATE','TEST_CARRIER_A','BRAND','DEVICE','HARDWARE','MODEL'])    
        return convert_to_geojson(df, intensity,'CLIENT_LONGITUDE', 'CLIENT_LATITUDE')
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/execute_sql', methods=['POST'])
def execute_sql():
    longitud = None
    latitud = None 
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
        if df.shape[1] != 3:
            return jsonify({'error': 'Need to have only 3 columns'}), 400
        for col_name in df.columns:
            if 'LONGITUD' in col_name.upper():
                longitud = col_name
            if 'LATITUD' in col_name.upper():
                latitud = col_name
            
        if longitud == None or latitud == None:
            return jsonify({'error': 'Column is missing: Latitud or Longitud'}), 400
        
        remaining_columns = [col for col in df.columns if col != longitud and col != latitud]
        if remaining_columns:
            intensity = remaining_columns[0]
        
        return convert_to_geojson(df, intensity, longitud, latitud)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5000)