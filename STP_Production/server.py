# -*- coding: utf-8 -*-
"""
Created on Fri May 10 11:09:46 2019

@author: m.zhukov
"""
#from multiprocessing import Process
from flask import render_template
import connexion
import sys
#import configparser
#swagger_path=r"C:/Users/m.zhukov/Documents/Text_Classific_Prototype/deployment/swagger_ui"
sys.path.append('./controller/')


#from importlib.machinery import SourceFileLoader
#people = SourceFileLoader("people.py", "C:\\Users\\m.zhukov\\Documents\\ML_Prototype\\controller\\people.py").load_module()
#people.create

# Create the application instance
app = connexion.App(__name__, specification_dir='./')
#app = connexion.App(__name__, specification_dir='./controller/')

# Read the swagger.yml file to configure the endpoints
app.add_api('swagger2.yml')

@app.route('/')
def home():
    
    """
    This function just responds to the browser ULR
    localhost:5000/

    :return:        the rendered template 'home.html'
    """
    return render_template('home.html')

   

# If we're running in stand alone mode, run the application
if __name__ == '__main__': 
    
#    config = configparser.RawConfigParser()
#    config.read('./config.txt')
#    details = dict(config.items('server'))
#     sys.exit()
     app.run(host='0.0.0.0', port=5001, debug=True)
 #   app.run(host=details['host'],port=int(details['port']), debug=True)
   
   

