from flask import Flask
import json
import requests

app = Flask(__name__)


@app.route('/match',methods=['POST'])
def match() :
	return json.dumps(requests.form('input_match'))


if __name__ == "__main__":
	app.run(debug=1)