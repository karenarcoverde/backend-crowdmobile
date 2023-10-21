from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine
from env_variables import *
import re

app = Flask(__name__)

parts = DATABASE_URL.split("://")
protocol = parts[0]

user_pass, rest = parts[1].split("@")
postgres_user, postgres_pass = user_pass.split(":")

url_port_db, _ = rest.split("?")
postgres_url, port_db = url_port_db.split(":")
postgres_port, postgres_database = port_db.split("/")

engine = create_engine(f"postgresql://{postgres_user}:{postgres_pass}@{postgres_url}:{postgres_port}/{postgres_database}")

@app.route('/execute_sql', methods=['POST'])
def execute_sql():
    data = request.get_json()
    if not data or 'query' not in data:
        return jsonify({'error': 'Query not provided'}), 400

    query = data['query']

    prohibited_keywords = ['insert', 'update', 'delete', 'drop', 'alter', 'grant', 'revoke', 'execute', 'truncate',
    'create', 'deny', 'commit', 'rollback', 'savepoint', 'set transaction', 'do', 'call',
    'explain', 'listen', 'load', 'lock', 'notify', 'prepare', 'reassign', 'reindex', 'release', 'reset']

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
