from flask import Flask

app = Flask(__name__)

@app.route('/', methods = ['GET'])
def hello_world():
    return 'Hello, world!\n'

@app.route('/<name>', methods = ['GET'])
def hello_name(name):
    return 'Hello, {}!\n'.format(name)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')