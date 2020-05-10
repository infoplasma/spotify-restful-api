# testing the spotify API

"""
endpoints:
    	https://api.spotify.com/v1/search
"""
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/autentico')
def autentico():
    return "AUTENTICAO?"

if  __name__ == '__main__':
    app.run(host='127.0.0.1')


