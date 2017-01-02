# -*- coding: utf-8 -*-

import time
import datetime
from datetime import timedelta
import json
import pymongo
from flask import Flask
from flask import request, make_response, current_app
from functools import update_wrapper
from bson.code import Code

app = Flask(__name__)

MONGOD_HOST = '219.224.134.213'
MONGOD_PORT = 27017

def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

def _default_mongo(host=MONGOD_HOST, port=MONGOD_PORT, usedb='test'):
    # 强制写journal，并强制safe
    connection = pymongo.MongoClient(host=host, port=port, j=True, w=1)
    # db = connection.admin
    # db.authenticate('root', 'root')
    db = getattr(connection, usedb)
    return db


mongo = _default_mongo(usedb='flightradar')

collection_name = "all_trails"
daily_collection_name_prefix = "trails_"


def HMS2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S')))


@app.route('/')
@crossdomain(origin='*')
def date_trails():
    # return url_for('date_trails')
    date = request.args.get("date")  # '2016-07-01'

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}})

    results = []
    results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/airline')
@crossdomain(origin='*')
def airline_trails():
    date = request.args.get("date")
    airline_code1 = request.args.get("airline")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airline_code1": airline_code1})

    results = []
    results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/flight')
@crossdomain(origin='*')
def flight_trails():
    date = request.args.get("date")
    fid = request.args.get("flight")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "fid": fid})

    results = []
    results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/dep')
@crossdomain(origin='*')
def dep_trails():
    date = request.args.get("date")
    dep = request.args.get("dep")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_dep": dep})

    results = []
    results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/dep-flight')
@crossdomain(origin='*')
def dep_flight_trails():
    date = request.args.get("date")
    dep = request.args.get("dep")
    fid = request.args.get("flight")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "fid": fid, "airport_dep": dep})

    results = []
    results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/arr')
@crossdomain(origin='*')
def arr_trails():
    date = request.args.get("date")
    arr = request.args.get("arr")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_arr": arr})

    results = []
    results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/arr-flight')
@crossdomain(origin='*')
def arr_flight_trails():
    date = request.args.get("date")
    arr = request.args.get("arr")
    fid = request.args.get("flight")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "fid": fid, "airport_arr": arr})

    results = []
    results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/dep-arr')
@crossdomain(origin='*')
def dep_arr_trails():
    date = request.args.get("date")
    dep = request.args.get("dep")
    arr = request.args.get("arr")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_dep": dep, "airport_arr": arr})

    results = []
    results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/dep-arr-multi')
@crossdomain(origin='*')
def dep_arr_trails_multi():
    dates = request.args.get("dates").split(",")  # 2016-06-10,2016-06-11
    dep = request.args.get("dep")
    arr = request.args.get("arr")

    results = []
    for date in dates:
        f_second = date + ' 00:00:00'
        l_second = date + ' 23:59:59'
        daily_collection_name_prefix + date.replace("-", "")
        cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].find(
            {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_dep": dep, "airport_arr": arr})
        results.extend([r for r in cursor])

    return json.dumps(results)


@app.route('/count')
@crossdomain(origin='*')
def date_trails_count():
    date = request.args.get("date")  # '2016-07-01'

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    return json.dumps(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}}).count())


@app.route('/airline-count')
@crossdomain(origin='*')
def airline_trails_count():
    date = request.args.get("date")
    airline_code1 = request.args.get("airline")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    return json.dumps(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airline_code1": airline_code1}).count())


@app.route('/flight-count')
@crossdomain(origin='*')
def flight_trails_count():
    date = request.args.get("date")
    fid = request.args.get("flight")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    return json.dumps(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "fid": fid}).count())


@app.route('/dep-count')
@crossdomain(origin='*')
def dep_trails_count():
    date = request.args.get("date")
    dep = request.args.get("dep")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    return json.dumps(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_dep": dep}).count())


@app.route('/dep-flight-count')
@crossdomain(origin='*')
def dep_flight_trails_count():
    date = request.args.get("date")
    dep = request.args.get("dep")
    fid = request.args.get("flight")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    return json.dumps(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "fid": fid, "airport_dep": dep}).count())


@app.route('/arr-count')
@crossdomain(origin='*')
def arr_trails_count():
    date = request.args.get("date")
    arr = request.args.get("arr")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    return json.dumps(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_arr": arr}).count())


@app.route('/arr-flight-count')
@crossdomain(origin='*')
def arr_flight_trails_count():
    date = request.args.get("date")
    arr = request.args.get("arr")
    fid = request.args.get("flight")

    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    return json.dumps(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "fid": fid, "airport_arr": arr}).count())


@app.route('/dep-arr-count')
@crossdomain(origin='*')
def dep_arr_trails_count():
    date = request.args.get("date")
    dep = request.args.get("dep")
    arr = request.args.get("arr")

    results = []
    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'
    return json.dumps(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
        {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_dep": dep,
         "airport_arr": arr}).count())


@app.route('/dep-arr-multi-count')
@crossdomain(origin='*')
def dep_arr_trails_multi_count():
    dates = request.args.get("dates").split(",")  # 2016-06-10,2016-06-11
    dep = request.args.get("dep")
    arr = request.args.get("arr")

    results = 0
    for date in dates:
        f_second = date + ' 00:00:00'
        l_second = date + ' 23:59:59'
        daily_collection_name_prefix + date.replace("-", "")
        results += mongo[daily_collection_name_prefix + date.replace("-", "")].find(
            {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_dep": dep,
             "airport_arr": arr}).count()

    return json.dumps(results)


@app.route('/real-time')
@crossdomain(origin='*')
def real_time():
    f_second = int(time.time()) - 300
    l_second = int(time.time())
    t = datetime.date.today().timetuple()
    month = str(t[1])
    date = str(t[2])
    if t[1] < 10:
        month = '0' + month
    if t[2] < 10:
        date = '0' + date
    date = str(t[0]) + '-' + month + '-' + date # 2016-10-18
    mapper = Code("""
                        function() {
                            if (this.timestamp >= """ + str(f_second) + """ && this.timestamp <=""" + str(l_second) + """ ) {
                                emit(this.fid, {timestamp: this.timestamp, lon: this.lon, lat: this.lat, num1: this.num1});
                            }
                        }
                     """)
    reducer = Code("""
                        function(key, values) {
                            var result = {};
                            values.sort(function(a, b) {return - a.timestamp + b.timestamp});
                            result = values[0];
                            return result;
                        }
                      """)
    cursor = mongo[daily_collection_name_prefix + date.replace("-", "")].inline_map_reduce(mapper, reducer)

    results = []
    results.extend([r for r in cursor])
    return json.dumps(results)


@app.route('/test')
@crossdomain(origin='*')
def test():
    date = "2016-06-10"
    f_second = date + ' 00:00:00'
    l_second = date + ' 23:59:59'

    airports = ["PVG", "PEK", "SHA", "CTU", "XMN", "SYX", "HGH", "CAN", "SZX", "HAK", "CKG", "NKG", "TYN", "SHE", "KWE",
                "XIY", "YNT", "TNA", "NAY", "TAO", "SJW", "WUX", "CGO", "TSN", "XNN", "KMG", "JJN", "CGQ", "LJG", "FOC",
                "MFM", "CSX", "KHN", "HRB", "URC", "LHW", "ZUH", "BHY", "DLC", "XUZ", "HET", "HFE", "KWL", "SWA", "WUH",
                "NGB", "DAT", "JUZ", "NNG"]

    results = []
    for dep in airports:
        results.append(mongo[daily_collection_name_prefix + date.replace("-", "")].find(
            {"timestamp": {"$gte": HMS2ts(f_second), "$lte": HMS2ts(l_second)}, "airport_dep": dep,
             "airport_arr": "LAX"}).count())

    return json.dumps(results)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)
