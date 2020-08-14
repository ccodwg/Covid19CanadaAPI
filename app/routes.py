from app import app
from flask import request
import pandas as pd

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"

@app.route('/individual')
def individual():
    stat = request.args.get('stat')
    loc = request.args.get('loc')
    date = request.args.get('date')
    after = request.args.get('after')
    before = request.args.get('before')
    version = request.args.get('version')
    print(stat)
    print(loc)
    return "Hello, World!"

@app.route('/timeseries')
def timeseries():
    stat = request.args.get('stat')
    loc = request.args.get('loc')
    date = request.args.get('date')
    after = request.args.get('after')
    before = request.args.get('before')
    version = request.args.get('version')
    return "Hello, World!"

@app.route('/summary')
def summary():
    loc = request.args.get('loc')
    date = request.args.get('date')
    version = request.args.get('version')
    return "Hello, World!"

@app.route('/version')
def version():
    return "Hello, World!"
