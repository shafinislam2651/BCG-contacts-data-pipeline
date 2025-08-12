from flask import Flask, jsonify, request
import os
import subprocess
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
CLEANING_DIR = os.path.join(os.path.dirname(__file__), 'cleaning')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')

@app.route('/scripts', methods=['GET'])
def list_scripts():
    scripts = [f for f in os.listdir(CLEANING_DIR) if f.endswith('.py')]
    return jsonify({'scripts': scripts})

@app.route('/run', methods=['POST'])
def run_script():
    data = request.get_json()
    script_name = data.get('script')
    if not script_name or not script_name.endswith('.py'):
        return jsonify({'error': 'Invalid script name'}), 400
    script_path = os.path.join(CLEANING_DIR, script_name)
    if not os.path.isfile(script_path):
        return jsonify({'error': 'Script not found'}), 404
    try:
        result = subprocess.run(['python3', script_path], capture_output=True, text=True, timeout=300)
        return jsonify({
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/output/<filename>', methods=['GET'])
def get_output_file(filename):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.isfile(file_path):
        return jsonify({'error': 'File not found'}), 404
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return jsonify({'content': content})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/output-files', methods=['GET'])
def list_output_files():
    if not os.path.exists(OUTPUT_DIR):
        return jsonify({'files': []})
    files = [f for f in os.listdir(OUTPUT_DIR) if os.path.isfile(os.path.join(OUTPUT_DIR, f))]
    return jsonify({'files': files})

if __name__ == '__main__':
    app.run(debug=True)