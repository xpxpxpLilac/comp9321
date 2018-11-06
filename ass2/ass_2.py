import json
import requests
import time
import re

import pandas as pd
from pymongo import MongoClient
from flask_cors import CORS
from flask import Flask
from flask_restplus import Resource, Api
from flask import request
from flask_restplus import fields
from flask_restplus import inputs
from flask_restplus import reqparse

app = Flask(__name__)
api = Api(app,
          default="Worldbank",  # Default namespace
          title="Worldbank Dataset",  # Documentation Title
          description="This is an api for interacting with world bank api.\n comp9321 assignment2")  # Documentation Description
CORS(app)

db_name = 'assignment2'
indicator = api.model('indicator_id',{'indicator_id': fields.String})
query_model = api.model('q',{'q': fields.String})

parser = reqparse.RequestParser()
# parser.add_argument('indicator_id')
# parser.add_argument('ascending', type=inputs.boolean)


@api.route('/assignment2')
class a2_collection(Resource):

    @api.response(200, 'OK But Already Exist')
    @api.response(201, 'Post Created Successfully')
    @api.response(404, 'Invalid Attempt, Non-exist Indicator')
    @api.doc(description="Add a new post")
    @api.expect(indicator, validate=True)
    def post(self):
        
        customer_input = request.json
        indicator_id = customer_input['indicator_id']
        client = MongoClient( \
            host='mongodb://user_001:Abc123456@ds251632.mlab.com:51632/assignment2', port=27017 )
        db = client[db_name]

        #=========================================
        #check if indicator is already in database
        name = []
        name = db.collection_names()
        if indicator_id in name:
            c = db[str(indicator_id)]
            cursor = c.find_one({ 'indicator' : indicator_id })
            result = {}
            result['location'] = cursor['location']
            result['collection_id'] = cursor['collection_id']
            result['creation_time'] = cursor['creation_time']
            result['indicator'] = cursor['indicator']
            return result, 200

        #get data from api
        r = requests.get('http://api.worldbank.org/v2/countries/all/indicators/'+ str(indicator_id) +'?date=2012:2017&format=json')
        data_dict = r.json()

        #=================================
        #check if indicator exists in api
        key = 'message'
        if key in data_dict[0]:
            if data_dict[0]['message'][0]['id'] == "120":
                return {"message": "Identifier={} doesn't exist in world bank dataset searching through api".format(indicator_id)}, 404
        #get related page number to retrive all data
        page_number = 3

        #post_res is the return dictionary
        post_res = {}
        post_res['location'] = "/"+ str(db_name) +"/"+ str(indicator_id) 
        post_res['collection_id'] = indicator_id
        post_res['indicator'] = indicator_id
        post_res['indicator_value'] = data_dict[1][0]['indicator']['value']
        post_time = str(time.ctime())
        post_res['creation_time'] = post_time
        post_res['entries'] = []

        #extract data
        page_value = {}
        for page in range(1,page_number):
            r = requests.get('http://api.worldbank.org/v2/countries/all/indicators/'+ \
                str(indicator_id) +'?date=2012:2017&format=json&page=' + str(page))
            data_dict = r.json()
            # print("==========================  new page "+str(page)+"  ============================")
            for entity in data_dict[1]:
                page_value = {}
                page_value['country'] = entity['country']['value']
                page_value['date'] = entity['date']
                page_value['value'] = entity['value']
                post_res['entries'].append(page_value)

        c = db[indicator_id]
        record = json.loads(json.dumps(post_res))
        c.insert(record)
       
        result = {}
        result['location'] = post_res['location']
        result['collection_id'] = post_res['collection_id']
        result['creation_time'] = post_res['creation_time']
        result['indicator'] = post_res['indicator']
        return result, 201



    @api.response(200, 'Retrieve All Collections Successfully')
    @api.doc(description="Retrieve all collections in the database")
    def get(self):

        client = MongoClient( \
            host='mongodb://user_001:Abc123456@ds251632.mlab.com:51632/assignment2', port=27017 )
        db = client[db_name]

        get_res = []
        name = []
        name = db.collection_names()
        name.remove('system.indexes')
        name.remove('objectlabs-system')
        name.remove('objectlabs-system.admin.collections')
        for co_name in name:
            meta_data = {}
            c = db[co_name]
            cursor = c.find()
            # print(co_name)
            df = pd.DataFrame(list(cursor))
            # print(df['location'])
            meta_data['location'] = df['location'][0]
            meta_data['collection_id'] = df['collection_id'][0]
            meta_data['creation_time'] = df['creation_time'][0]
            meta_data['indicator'] = df['indicator'][0]
            get_res.append(meta_data)

        return get_res, 200

@api.route('/assignment2/<collection_id>')
class a2_collection_id(Resource):

    @api.response(200, 'Retrieve The Collection Successfully')
    @api.response(404, 'Invalid attempt')
    @api.doc(description="Retrieve a specific collection and print out the content")
    def get(self, collection_id):
        client = MongoClient( \
            host='mongodb://user_001:Abc123456@ds251632.mlab.com:51632/assignment2', port=27017 )
        db = client[db_name]
        #============================================
        #invalid input, non-exist
        name = []
        name = db.collection_names()
        if collection_id not in name:
            return {"message": "Collection {} is not in the databaset/ invalid input".format(collection_id)}, 404
        #==============================================
        c = db[str(collection_id)]
        df = pd.DataFrame(list(c.find()))
        # df = df[df.columns.values[1:]
        # data = df.to_json()
        get_id_res = {}
        get_id_res['location'] = df['location'][0]
        get_id_res['collection_id'] = df['collection_id'][0]
        get_id_res['creation_time'] = df['creation_time'][0]
        get_id_res['indicator'] = df['indicator'][0]
        get_id_res['entries'] = df['entries'][0]

        return get_id_res, 200

    @api.response(200, 'Deleted Successfully')
    @api.response(404, "Validation Error, resource doesn't exist")
    @api.doc(description="Delete a collection from database, if doesn't exist return 404")
    def delete(self, collection_id):
        client = MongoClient( \
            host='mongodb://user_001:Abc123456@ds251632.mlab.com:51632/assignment2', port=27017 )
        db = client[db_name]
        name = []
        name = db.collection_names()
        print(name)
        if collection_id not in name:
            return {"message": "A collection with Identifier={} doesn't exist in the dataset".format(collection_id)}, 404

        db.drop_collection(str(collection_id))

        return {"message": "Collection = {} is removed from the databaset".format(collection_id)}, 200

@api.route('/assignment2/<collection_id>/<int:year>/<country>')
class a2_collection_year_country(Resource):

    @api.response(200, 'OK get successfully')
    @api.response(400, 'Validation Error, non-existed country or year')
    @api.doc(description="Get the info when given correct collection name, country and year")
    def get(self, collection_id, year, country):

        country = str(country)
        year = str(year)
        year_country_post = {}

        client = MongoClient( \
                host='mongodb://user_001:Abc123456@ds251632.mlab.com:51632/assignment2', port=27017 )
        db = client[db_name]
        c = db[collection_id]
        my_query = { 'entries.country' : country, 'entries.date' : year }
        cursor = c.find_one(my_query)
        #================================
        #nothing return, invalid
        #return invalid input
        if cursor is None:
                return { 'message' : 'invalid country or year, please check your input' }, 400
        #================================
        
        info = next(item for item in cursor['entries'] \
                    if item["country"] == country and item["date"] == year)

        year_country_post['collection_id'] = collection_id
        year_country_post['indicator'] = collection_id
        year_country_post['country'] = info['country']
        year_country_post['year'] = info['date']
        year_country_post['value'] = info['value']

        return year_country_post, 200


@api.route('/assignment2/<collection_id>/<int:year>')
class a2_collection_year_query(Resource):
    @api.response(200, 'OK get successfully')
    @api.response(400, 'Validation Error, non-existed year')
    @api.doc(description="Get the info when given correct collection name, year with optional query")
    @api.param('q','optional query, should in form of top/bottom<[1-100]>')
    def get(self, collection_id, year):

        query = request.args.get('q')
        collection_id = str(collection_id)
        year = str(year)

        client = MongoClient( \
                host='mongodb://user_001:Abc123456@ds251632.mlab.com:51632/assignment2', port=27017 )
        db = client[db_name]
        c = db[collection_id]

        my_query = { 'entries.date' : year }
        cursor = c.find_one(my_query)

        if cursor is None:
                return { 'message' : 'invalid collection_id or year, please check your input' }, 400

        res = {}
        res['indicator'] = cursor['indicator']
        res['indicator_value'] = cursor['indicator_value']
        info = (item for item in cursor['entries'] if item['date'] == year)
        sort_list = []
        for val in info:
            sort_list.append(val)
        newlist = sorted(sort_list, key=lambda k: k['value'], reverse=True) 
        # #=====================================
        # # preproceed the query
        # # exception handle no query==============================================
        if query is None:
            res['entries'] = newlist
            return res, 200
        else:
            query = str(query)
            num = re.compile('[0-9]+')
            dirc = re.compile('[a-z]+')
            number = num.search(query)
            direction = dirc.search(query)
            n = int(number.group())
            d = direction.group()
            if n > 100 or n < 0:
                return { 'message' : 'query number is less than 0 or greater than 100' }, 400
            if d != 'top' and d != 'bottom':
                return { 'message' : 'wrong query, must be top<N> or bottom<N>' }, 400
            entries = []
            if d == 'top':
                for index in range(n):
                    try:
                        entries.append(newlist[index])
                    except IndexError:
                        pass
            if d == 'bottom':
                for index in range(n,0,-1):
                    i = len(newlist) - index
                    try:
                        entries.append(newlist[i])
                    except IndexError:
                        pass
            res['entries'] = entries

            return res, 200


if __name__ == '__main__':
    app.run(debug=True)