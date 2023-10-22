from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine
from env_variables import *
import re
import math

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
    return not (isinstance(value, str) and value == "") and not math.isnan(value)

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
    
        return jsonify({'message': 'ok'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/convert', methods=['POST'])
def convert_to_geojson():
    data = request.get_json()
    
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    for item in data:
            if len(item.keys()) != 3:
                return jsonify({"error": "Only 3 keys for each item"}), 400
            
            latitud_key = next((key for key in item if "LAT" in key.upper()), None)
            longitud_key = next((key for key in item if "LONG" in key.upper()), None)

            if not latitud_key or not longitud_key:
                return jsonify({"error": "Need to have latitud and longitud for each item"}), 400
            
            intensity_key = next((key for key in item if key != latitud_key and key != longitud_key), None)
            
            if latitud_key and longitud_key and intensity_key:
                latitud = item[latitud_key]
                longitud = item[longitud_key]
                intensity = item[intensity_key]

                if is_valid(latitud) and is_valid(longitud) and is_valid(intensity):
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [longitud, latitud]
                        },
                        "properties": {
                            "intensity": intensity
                        }
                    }
                    geojson['features'].append(feature)
            else:
                return jsonify({"error": "Invalid data"}), 400
        
    return jsonify(geojson)

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
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == "__main__":
    app.run(debug=False)