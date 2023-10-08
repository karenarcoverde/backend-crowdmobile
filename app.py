from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine
from env_variables import *

app = Flask(__name__)

engine = create_engine(f"postgresql://{postgres_user}:{postgres_pass}@{postgres_url}:{postgres_port}/{postgres_database}")

@app.route('/execute_sql', methods=['POST'])
def execute_sql():
    data = request.get_json()
    if not data or 'query' not in data:
        return jsonify({'error': 'Query not provided'}), 400

    query = data['query']

    if not query.lower().startswith('select'):
        return jsonify({'error': 'Only SELECT queries are allowed'}), 400

    try:
        df = pd.read_sql(query, engine)
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == "__main__":
    app.run(debug=False)
