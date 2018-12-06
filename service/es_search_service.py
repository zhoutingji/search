import begin
from gevent import monkey
from flask import Flask
from flask import request, jsonify
from flask_cors import CORS
from config import configer
from es_tool.search import search_content

app = Flask(__name__)
CORS(app)


@app.route('/search', methods=["GET"])
def search():
    data = request.args.get('text')
    ret = search_content(data)
    return jsonify(ret)


@begin.start
def run():
    monkey.patch_all()
    bind = configer.get('app', 'host')
    port = configer.getint('app', 'port')
    app.config['DEBUG'] = configer.getboolean('app', 'debug')
    app.run(port=port, host=bind)
