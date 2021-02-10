"""
This is the people module and supports all the ReST actions for the
PEOPLE collection
"""
# System modules
import deviations_analytics
import pymongo
from datetime import datetime
from pymongo import MongoClient



import sys
import configparser
from urllib.parse import quote_plus
sys.path.append('./controller')

from flask import jsonify
# 3rd party modules
from flask import make_response, abort
def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))

PEOPLE = {
  "CONTENT": [
    {
      "ABSTNR": "00000000",
      "BUKRS": "YS90",
      "GRPNR": -1,
      "HBKID": "FI-RE",
      "HKTID": "FI-RE",
      "INSDATE": "2017-02-17",
      "INSTIME": "09:17:31",
      "KTDBTR": 7000,
      "KTDLFDNR": "00000002",
      "KTDVALUTA": "2017-02-17",
      "MANDT": "020",
      "VALUTA": "2017-02-17",
      "VERSDATUM": "2019-04-08",
      "VERSZEIT": "14:08:20"
    }
  ],
  "TABLE": "CKTB"
}



def result():

    client = MongoClient('localhost:27017')
    mydb = client["Cash_management"]
    mycol1 = mydb["ResultAmounts"]
    mycol2 = mydb["ResultDays"]
    #should be the last calculated result
    
    
    last_doc1 = mycol1.find_one(
  sort=[( '_id', pymongo.DESCENDING )]
)
    last_doc2 = mycol2.find_one(
  sort=[( '_id', pymongo.DESCENDING )]
)
   
    del last_doc1["_id"]
    del last_doc2["_id"]
    z={"AMOUNTS":last_doc1,"DAYS": last_doc2}
 
    return z

def analytics(table):
    
    config = configparser.RawConfigParser()
    config.read('./config.txt')
    details = dict(config.items('mongodb'))
    username=details['user']
    password=details['password']
    host=details['host']
    port=details['port']
    host=host+":"+port
#    client = MongoClient('localhost:27017')
    uri = "mongodb://%s:%s@%s"%(quote_plus(username), quote_plus(password),host)
    client = MongoClient(uri)
    mydb = client["Cash_management"]
    mycol = mydb["dfd"]
    groups = table.get("PLANNING_GROUPS", None)
    accounts = table.get("ACCOUNTS", None)
    interval = table.get("INTERVAL",None)

 
   # if groups and accounts: 
    if 1==1:     
        data = {"PLANNING_GROUPS": groups,"ACCOUNTS": accounts , "INTERVAL":interval}
         
     #   y = json.loads(data)
     #   print('This is error output', file=sys.stderr)
     #   print('This is standard output', file=sys.stdout)
     #   sys.stdout.write("Download progress: %d%%   \r" )
     #   data=dumps(data)
     #   data=json.loads(data)
     #   data = data.replace("\'", "\"")
     #
     #   datadic = json.loads(data)
        mycol.insert(data)
        num=deviations_analytics.stpanalytics(data)
     #    data = {'paremeters': 'successfully transfered'}
        data= {'Result_id': num}
    
    else:
        abort(
            406,
            "Something wrong with parameters tranfer".format(groups=groups),
        )
        
   # last_doc =last_doc.__str__()   
    # Create the list of people from our data

    return data, 201


def transfer(table): 
    
    """
    This function creates a new person in the people structure
    based on the passed in person data
    :param person:  person to create in people structure
    :return:        201 on success, 406 on person exists
    """
    
    # header = connexion.request.headers['table-name']
  #  header = table.get("table-name","Contacts")   
    
    config = configparser.RawConfigParser()
    config.read('./config.txt')
    details = dict(config.items('mongodb'))
    username=details['user']
    password=details['password']
    host=details['host']
    port=details['port']
    host=host+":"+port
#    client = MongoClient('localhost:27017')
    uri = "mongodb://%s:%s@%s"%(quote_plus(username), quote_plus(password),host)
    client = MongoClient(uri)
    
    mydb = client["Cash_management"]
    
    collist = mydb.list_collection_names()
    
    mycol0= mydb["counters"]       
    mycol1 = mydb["M_CKTD"]
    mycol11 = mydb["M_CKTB"]
    mycol2 = mydb["M_CKTAE"]
    mycol5 = mydb["M_SNAPSALD"]
    
#    collist = mydb.list_collection_names()
    
    
        
    if mycol0.count() == 0:
         mycol0.insert({'_id': "result_id", 'seq': 0})
           
    if mycol1.count() == 0:
        mycol1.insert({'_id': "result_id", 'seq': 0})
        
    if mycol11.count() == 0:
        mycol11.insert({'_id': "result_id", 'seq': 0})
        
    if mycol2.count() == 0:
        mycol2.insert({'_id': "result_id", 'seq': 0})
        
    if mycol5.count() == 0:
         mycol5.insert({'_id': "result_id", 'seq': 0})
    
    tablename = table.get("TABLE", None)
    content = table.get("CONTENT", None)
    # print(type(content))
    
    mycol = mydb[tablename]
    
    #time=get_timestamp()
    
    if tablename and content:
        
        time={'datetime':get_timestamp()}
        content.append(time)
        status=mycol.insert(content)
        
        data = {'additional table': 'successfully transfered'}
        return jsonify(data), 201
    

    else:
        abort(
            406,
            "Something wrong with table tranfer".format(tablename=tablename),
        )
