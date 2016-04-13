'''
Created on Apr 7, 2016

@author: tylerlisowski
'''
from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3333)


