# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 13:56:22 2019

@author: m.zhukov
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor

Dies ist eine temporäre Skriptdatei.
"""
from datetime import datetime
from urllib.parse import quote_plus
import http.client
from pymongo import MongoClient

import json
from pandas import DataFrame
import ssl


import pandas as pd
import configparser

pd.set_option('display.max_columns', 100)

def getNextSequence(collection,name):  
    return collection.find_and_modify(query= { "_id": name },update= { '$inc': {'seq': 1}}, new=True ).get('seq');

def get_timestamp():
    return datetime.now().strftime("%Y%m%d%H%M%S")


def maximum_interval(cf,filter2, initial):
    
    if(filter2.empty==False): 
        cf['Bank Statement date']=initial
       # print("cf",initial)
       # print("!!!!!!!!!!!", 
        #if (row['ABSTNR']==140):
        m=int(filter2['AZDAT'].count())
        print(m)
        if(m>0):
            maxim=(pd.to_datetime(cf['VALUTA'])-pd.to_datetime(filter2['AZDAT'].iloc[0])).astype('timedelta64[D]').astype(int).iloc[0].item()
            i=0
         #   print("maxim", maxim)
            while (m-i>0):
                diff=(pd.to_datetime(cf['VALUTA'])-pd.to_datetime(filter2['AZDAT'].iloc[i])).astype('timedelta64[D]').astype(int).iloc[0].item()
            #    print("diff", diff)
              #  print("fdfd",type(diff))
                if(abs(maxim)<abs(diff)):
                    maxim=(pd.to_datetime(cf['VALUTA'])-pd.to_datetime(filter2['AZDAT'].iloc[i])).astype('timedelta64[D]').astype(int).iloc[0].item()  
                    cf['Bank Statement date']=pd.to_datetime(filter2['AZDAT'].iloc[i])
                i=i+1
      #  If(filter2[['AZDAT']].count()>1):      
                
       # if (filter2[['AZDAT']].value_counts()>1):
       #     print("cf['Bank Statement date']", filter2['AZDAT'].iloc[0])
            #print ("cf['Bank Statement date']", cf['Bank Statement date'])
    else :  cf['Bank Statement date']=cf['Reconciliation date'] 
    return cf['Bank Statement date']

#printmath.copysign(2, -1) def stpanalytics (data)

#увеличим дефолтный размер графикоv
def stpanalytics (data):
    
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

    mycol1 = mydb["M_CKTD"]
    mycol11 = mydb["M_CKTB"]
    mycol2 = mydb["M_CKTAE"]
    mycol5 = mydb["M_SNAPSALD"]
    
    collist = mydb.list_collection_names()
    
    
    if "counters" in collist:        
        mycol0 = mydb["counters"]
    else:
        mydb.createCollection("counters")        
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

    df1 = DataFrame(list(mycol1.find({})))
    df11 = DataFrame(list(mycol11.find({})))
    df2 = DataFrame(list(mycol2.find({})))
    df5 = DataFrame(list(mycol5.find({})))
    
    
 
  #  os.chdir("../MZ")
 #   df1.sort_values(by=['ABSTNR'])

#-------------------------------------------------------------------------------------------------------------------------------------------------------------
#df1=df1[['Reconciliation number','Short-term planning value date','ST planning value date (techn)', 'Short-term planning amount','Plan group number','Serial no. STP','Company code','Account','House bank','Entered on','Entered at']]
    df1.drop(df1.loc[df1['ABSTNR'].isnull()].index, inplace=True)
    df11.drop(df11.loc[df11['ABSTNR'].isnull()].index, inplace=True)
    df2.drop(df2.loc[df2['ABSTNR'].isnull()].index, inplace=True)
    df1.drop(df1.loc[df1['ABSTNR'].astype(int)==0].index, inplace=True)
  
#-------------------------------------------------------------------------------------------------------------------------------------------------------------
 #print(df1.head(20))
    i=0
    df1['ABSTNR']=df1['ABSTNR'].astype(int)
    df11['ABSTNR']=df11['ABSTNR'].astype(int)
    df2['ABSTNR']=df2['ABSTNR'].astype(int)
#---------------------Account assignments----------------------------------------------------
    k=0
    df1_empty = pd.DataFrame().reindex_like(df1)
    df1_res = pd.DataFrame()
    df1_new = pd.DataFrame()
    if(data["ACCOUNTS"]):   
        for x in data["ACCOUNTS"]:
            print("!",x)
            df1_empty=df1.loc[(df1['HKTID'] ==  data["ACCOUNTS"][k]["HKTID"]) & (df1['BUKRS'] ==  data["ACCOUNTS"][k]['BUKRS'])& (df1['HBKID'] ==  data["ACCOUNTS"][k]["HBKID"])]
            df1_res=df1_res.append(df1_empty) 
            k=k+1
        df1=df1_res
        k=0
    if(data["PLANNING_GROUPS"]):        
        for x in data["PLANNING_GROUPS"]:
            df1_empty=df1_res.loc[(df1_res['GRPNR'] == data["PLANNING_GROUPS"][k])]
            df1_new=df1_new.append(df1_empty) 
            k=k+1
   # df1_empty=df1_empty.dropna()
        df1=df1_new
    df1.sort_values(by=['ABSTNR'],ascending=True, inplace=True)
 #   df1=df1.loc[(df1['HKTID'] ==  "COEU2") & (df1['BUKRS'] == "HOAG")& (df1['HBKID'] == "COM01")]#& (df1['Plan group number'] == 55)]
#print (cf.info())-----------------------------------------------------------------------------
    list1=[]
#index = pd.date_range(todays_date-datetime.timedelta(20), periods=20, freq='D')
    columns = ['Delta-0d','Delta-1d','Delta-2d','Delta-3d','Delta-4d','Delta-5d', 'Delta-6d', 'Delta-7d', 'Delta-8d', 'Delta-9d', 'Delta-10d' , 'BUKRS', 'HBKID', 'HKTID', 'PLANNING_GROUP']#, 'Delta-11d', 'Delta-12d', 'Delta-13d', 'Delta-14d']
    
    if(data["INTERVAL"]): 
        start = datetime.strptime(data["INTERVAL"]["START"],"%Y%m%d")
        end=datetime.strptime(data["INTERVAL"]["END"],"%Y%m%d")
    else:
        start=datetime.strptime("20130901","%Y%m%d")
        end = datetime.strptime(get_timestamp(),"%Y%m%d%H%M%S")
 #   if(data["INTERVAL"]): 
 #       start = datetime.strptime(data["INTERVAL"]["START"], '%y-%m-%d')
 #       end=data["INTERVAL"]["END"]
 #   else:
 #       end=get_timestamp()
        
#df_.xs('2019-04-20')['Delta-1-day'] = 10
    same={}
    a=3
#print (df_)
    n=1
    

#Forming the list of reconciliation days
#Going through sorted df1 "short term plannings" and selecting all reconciled items 
#I am going through short term plannings because all information needed for tking snapsalds is stored there, for example "Serial no. STP"
    
    for index, row in df1.iterrows():  
#    print(row['ABSTNR'])  #'ABSTNR'- reconciliation number
    #Reconciliation table, from here we take reconciliation time, bank statement amount
        filter1 = df2[df2['ABSTNR'] == row['ABSTNR']] 
        str1=filter1['DATUM'].item()
    # if filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item() not in list1:
        if (str1  not in list1) :
            list1.append(str1)
#Forming the list of same items       
        if(a==row['ABSTNR']): 
            n=n+1
            same[row['ABSTNR']]=n   #To do through the dictionary for most general case
        else:
            n=1   
            same[row['ABSTNR']]=1
            a=row['ABSTNR']
   # print(filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item())     

# not to go out of range
#same.append(i)
    a=3
    print(same)
    i=0
    n=0
    old= pd.DataFrame({'Short-term planning amount' : []})
    strin0=""
    strin1=""
    strin2=""
    strin3=""
    strin4=""
    strin5=""
    strin6=""
    strin7=""
    strin8=""
    strin9=""
    strin10=""
    strin0_1=""
    strin1_1=""
    strin2_1=""
    strin3_1=""
    strin4_1=""
    strin5_1=""
    strin6_1=""
    strin7_1=""
    strin8_1=""
    strin9_1=""
    strin10_1=""
    ttt=0
    df_ = pd.DataFrame(0.00,index=list1, columns=columns, dtype='object')
    df_per = pd.DataFrame(0.00,index=list1, columns=columns, dtype='object')
    df_1 = pd.DataFrame(0,index=list1, columns=columns)
    num=0
    for index, row in df1.iterrows():
        
        df_ = pd.DataFrame(0.00,index=list1, columns=columns, dtype='object')
        df_per = pd.DataFrame(0.00,index=list1, columns=columns, dtype='object')
        df_1 = pd.DataFrame(0,index=list1, columns=columns)
  #  Reconciliation table, from here we take reconciliation time, bank statement amount
        filter1 = df2[df2['ABSTNR'] == row['ABSTNR']]
     #  Bank statement table, from here we take bank statement date
        filter2 = df11[df11['ABSTNR'] ==row['ABSTNR'] ]
        str1=datetime.strptime(filter1['DATUM'].item(),"%Y-%m-%d")
        print("Rec_number", row['ABSTNR'])
  

#Filtering out related snapsalds   
  #  print(row['KTDLFDNR'])
        cf=df5.loc[(df5['KTDLFDNR'] == row['KTDLFDNR'])
        &(df5['INSDATE'] == row['INSDATE'])
        &(df5['INSTIME'] == row['INSTIME'])&(df5['KTDVALUTA'] == row['KTDVALUTA'])]
        cf=cf[['_id','GRPNR','VALUTA', 'KTDBTR','KTDLFDNR','BUKRS','HBKID','HKTID','INSDATE','INSTIME', 'VERSDATUM']]
        cf['GRPNR']=row['GRPNR']
        cf['BUKRS']=row['BUKRS']
        cf['HBKID']=row['HBKID']
        cf['HKTID']=row['HKTID']
   #     print("str1",str1)
   #     print("start",start)
  #  print("fddfdfdfdf", cf)
        
        if(same[row['ABSTNR']]>1)and(cf.empty==False) and (str1>=start) and (str1<=end): 
     #   cf.columns= ['_idMongo','Plan group#','ST value date', 'STP ammount','STP serial','Company','Bank','Currency','Insdate','Instime','version_date']
      #      print(cf)
            i=i+1
        #Short term planning amount
            if cf['KTDBTR'].empty == False:
                n=n+1
 #Taking out all interesting fields
                print ("Same elements",n)        
                if n==1: 
                #STP planning amount
               # cf['Bank statement amount']=filter1['SUMKTB'].astype(int)
                    cf['Reconciliation date']=filter1['DATUM'].item()
                    cf['Reconciliation date']=pd.to_datetime(cf['Reconciliation date'])
                    old['KTDBTR']=cf['KTDBTR']
                    cf['VALUTA'] = pd.to_datetime(cf['VALUTA'])
                    cf['VERSDATUM'] = pd.to_datetime(cf['VERSDATUM'])
                #Short term planning value date- version date
                    cf['delta_days']=cf['VALUTA']-cf['VERSDATUM']
                    cf['delta_values']=0
                    cf['stp-minus-b_statement-date']=0                              
                    old['Bank Statement date']= maximum_interval(cf, filter2, pd.to_datetime(filter2['AZDAT'].iloc[0]))
                    cf['Company']=""
                    cf['Bank']=""
                    cf['Currency']=""  
                    cf['Plan group#']=""
                    cf['RecDate']=pd.to_datetime(cf['Reconciliation date'])
              #  print("old!!!!",old['Bank Statement date'])
                elif (n>1) and (n<same[row['ABSTNR']]):
                    cf['VALUTA'] = pd.to_datetime(cf['VALUTA'])
                    cf['VERSDATUM'] = pd.to_datetime(cf['VERSDATUM'])
               # cf['Reconciliation date']=pd.to_datetime(cf['Reconciliation date'])
               # cf['Bank statement amount']=filter1['SUMKTB'].astype(int)
                    cf['KTDBTR']=cf['KTDBTR'].values+old['KTDBTR'].values
                    cf['delta_days']=cf['VALUTA']-cf['VERSDATUM']
                    cf['delta_values']=0
                    cf['stp-minus-b_statement-date']=0
                    old['KTDBTR']=cf['KTDBTR']
                    old['Bank Statement date']=maximum_interval(cf, filter2, old['Bank Statement date'])
                 
               
                else:
                    cf['Reconciliation date']=filter1['DATUM'].item()
                    cf['Reconciliation date']=pd.to_datetime(cf['Reconciliation date'])
                    cf['VALUTA'] = pd.to_datetime(cf['VALUTA'])
                    cf['KTDBTR']=cf['KTDBTR'].values+old['KTDBTR'].values
                    cf['VERSDATUM'] = pd.to_datetime(cf['VERSDATUM'])                   
                    if(filter2.empty==False)and (filter2['AZDAT'].iloc[0] !=0):   
                        cf['Bank Statement date']=maximum_interval(cf, filter2, pd.to_datetime(filter2['AZDAT'].iloc[0]))
                    else: cf['Bank Statement date']=cf['Reconciliation date']             
                    cf['Bank statement amount']=filter1['SUMKTB'].item()                
                    cf['delta_days']=cf['VALUTA']-cf['VERSDATUM']
                                        
                    cf['delta_values']=cf['Bank statement amount']-cf['KTDBTR']
                    cf['stp-minus-b_statement-date']=(cf['Bank Statement date']-cf['VALUTA']).dt.days
        
                    n=0
                    old= pd.DataFrame({'Short-term planning amount' : []})
                    i=0
             
              #  old['Short-term planning amount']=cf['Short-term planning amount']
              #  cf=cf.drop(columns=['Bank Statement date2'])
                    cf.columns= ['_idMongo','Plan group#','ST value date', 'STP ammount','STP serial','Company','Bank','Currency','Insdate','Instime','version_date','RecDate','--Bank statement Date--','Bank statement amount','delta_days','delta_values', 'stp-minus-b_statement-date']
                    print("Company",row['BUKRS'])
                    print("Bank",row['HBKID'])
                    print("Currency",row['HKTID'])
                    print( "PLANNING_GROUP",row['GRPNR'])
                    print(cf)
                    print("------------------------NEXT DATAFRAME----------------------")

        elif (cf.empty==False)and (str1>=start) and (str1<=end):

            cf['Bank statement amount']=filter1['SUMKTB'].item()
            cf['Reconciliation date']=filter1['DATUM'].item()
            cf['KTDBTR']=filter1['SUMKTD'].item()
            cf['Reconciliation date']=pd.to_datetime(cf['Reconciliation date'])
            
            if(filter2.empty==False)and (filter2['AZDAT'].iloc[0] !=0):   
                cf['Bank Statement date']=maximum_interval(cf, filter2, pd.to_datetime(filter2['AZDAT'].iloc[0]))
            else: cf['Bank Statement date']=cf['Reconciliation date']
      
            cf['VALUTA'] = pd.to_datetime(cf['VALUTA'])
            cf['VERSDATUM'] = pd.to_datetime(cf['VERSDATUM'])
            cf['delta_days']=cf['VALUTA']-cf['VERSDATUM']
           
            cf['delta_values']=cf['Bank statement amount']-cf['KTDBTR']
           
               
            
            cf['b_statement-date']=(cf['Bank Statement date']-cf['VALUTA']).dt.days
           
         #    print("uvaga", cf['b_statement-date'])
   
            cf.columns= ['_idMongo','Plan group#','ST value date', 'STP ammount','STP serial','Company','Bank','Currency','Insdate','Instime','version_date','Bank statement amount','RecDate','--Bank statement Date--','delta_days','delta_values','stp-minus-b_statement-date' ]
            print("Company",row['BUKRS'])
            print("Bank",row['HBKID'])
            print("Currency",row['HKTID'])
            print( "PLANNING_GROUP",row['GRPNR'])
            print(cf)
            print("------------------------NEXT DATAFRAME----------------------")
        if (cf.empty==False) and (str1>=start) and (str1<=end)and ((n>=same[row['ABSTNR']])or(n==0)):
            for index, row in cf.iterrows():    
                
 #                if(row['delta_days'].days==14):                                  
 #                  df_.at[filter1['DATUM'].item(),'Delta-14d'] =df_.at[filter1['DATUM'].item(),'Delta-14d'] if abs(df_.at[filter1['DATUM'].item(),'Delta-14d'])>abs(row['delta_values']) else row['delta_values']               
 #                if(row['delta_days'].days==13):   
 #                   df_.at[filter1['DATUM'].item(),'Delta-13d'] =df_.at[filter1['DATUM'].item(),'Delta-13d'] if abs(df_.at[filter1['DATUM'].item(),'Delta-13d'])>abs(row['delta_values']) else row['delta_values']              
 #                if(row['delta_days'].days==12): 
 #                   df_.at[filter1['DATUM'].item(),'Delta-12d'] =df_.at[filter1['DATUM'].item(),'Delta-12d'] if abs(df_.at[filter1['DATUM'].item(),'Delta-12d'])>abs(row['delta_values']) else row['delta_values'] 
 #                if(row['delta_days'].days==11): 
 #                   df_.at[filter1['DATUM'].item(),'Delta-11d'] =df_.at[filter1['DATUM'].item(),'Delta-11d'] if abs(df_.at[filter1['DATUM'].item(),'Delta-11d'])>abs(row['delta_values']) else row['delta_values'] 
                 if(row['delta_days'].days==10):                   
                 #  print("row['delta_values']", row['delta_values'])
                 #  print("df_.at[filter1['DATUM'].item(),'Delta-10d']",df_.at[filter1['DATUM'].item(),'Delta-10d'])
                   df_.at[filter1['DATUM'].item(),'Delta-10d'] =row['delta_values'] 
                   if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-10d']=row['delta_values']*100/abs((row['Bank statement amount']))
                   elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-10d']=row['delta_values']*100/abs((row['STP ammount']))
                   else: df_per.at[filter1['DATUM'].item(),'Delta-10d']=0
                # df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-10d'] =max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-10d'], row['reconciled-minus-planned'])
                 if(row['delta_days'].days==9):   
                    df_.at[filter1['DATUM'].item(),'Delta-9d'] =row['delta_values'] 
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-9d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-9d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-9d']=0
                   
                # df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-9d'] =max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-9d'], row['reconciled-minus-planned'])
                 if(row['delta_days'].days==8): 
                    df_.at[filter1['DATUM'].item(),'Delta-8d'] =row['delta_values'] 
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-8d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-8d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-8d']=0
               #  df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-8d'] =max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-8d'], row['reconciled-minus-planned'])
                 if(row['delta_days'].days==7): 
                    df_.at[filter1['DATUM'].item(),'Delta-7d'] =row['delta_values'] 
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-7d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-7d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-7d']=0
               #  df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-7d'] =max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-7d'], row['reconciled-minus-planned']) 
                 if(row['delta_days'].days==6): 
                    df_.at[filter1['DATUM'].item(),'Delta-6d'] =row['delta_values']
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-6d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-6d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-6d']=0
               #  df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-6d'] =max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-6d'], row['reconciled-minus-planned']) 
                 if(row['delta_days'].days==5):
                    df_.at[filter1['DATUM'].item(),'Delta-5d'] = row['delta_values'] 
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-5d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-5d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-5d']=0
                 if (row['delta_days'].days==4): 
                    df_.at[filter1['DATUM'].item(),'Delta-4d'] = row['delta_values'] 
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-4d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-4d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-4d']=0
               #  df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-4d'] =max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-4d'] , row['reconciled-minus-planned']) 
                 if (row['delta_days'].days==3): 
                    df_.at[filter1['DATUM'].item(),'Delta-3d'] = row['delta_values'] 
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-3d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-3d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-3d']=0
               #  df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-5d'] =max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-3d'] , row['reconciled-minus-planned']) 
                 if (row['delta_days'].days==2): 
                    df_.at[filter1['DATUM'].item(),'Delta-2d'] = row['delta_values'] 
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-2d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-2d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-2d']=0
               #  df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-5d'] =max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-2d'] , row['reconciled-minus-planned']) 
                 if (row['delta_days'].days==1): 
                    df_.at[filter1['DATUM'].item(),'Delta-1d'] = row['delta_values']
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-1d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-1d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-1d']=0
               #  df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-1d'] = max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-1d'] , row['reconciled-minus-planned']) 
                 if (row['delta_days'].days==0):  
                    df_.at[filter1['DATUM'].item(),'Delta-0d'] = row['delta_values']
                    if (row['Bank statement amount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-0d']=row['delta_values']*100/abs((row['Bank statement amount']))
                    elif(row['STP ammount']!=0): df_per.at[filter1['DATUM'].item(),'Delta-0d']=row['delta_values']*100/abs((row['STP ammount']))
                    else: df_per.at[filter1['DATUM'].item(),'Delta-0d']=0
                #    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", row['delta_values'], row['Bank statement amount'] , row['STP ammount'] )
               #  if (row['delta_days'].days==0):  
               #     df_.at[filter1['DATUM'].item(),'Delta-0d'] = df_.at[filter1['DATUM'].item(),'Delta-0d'] if abs(df_.at[filter1['DATUM'].item(),'Delta-0d'])>abs(row['delta_values']) else row['delta_values']
                #    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", row['delta_values'], row['Bank statement amount'] , row['STP ammount'] )
               #  df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-0d'] = max(df_1.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-0d'] , row['reconciled-minus-planned']) 
               #  df_.at[filter1['DATUM'].item(),'BUKRS'] =row['Company']
               #  df_.at[filter1['DATUM'].item(),'HBKID'] =row['Bank']
               #  df_.at[filter1['DATUM'].item(),'HKTID'] =row['Currency']
               #  df_.at[filter1['DATUM'].item(),'PLANNING_GROUP'] =row['Plan group#']
            
            
            amount= row['Bank statement amount'].__str__() if row['Bank statement amount'] != 0 else row['STP ammount'].__str__()
          #  print("index",ttt)
           # df_.round(2) 
            if ttt==0:               
                strin10=strin10+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-10d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-10d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin9=strin9+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-9d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-9d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin8=strin8+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-8d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-8d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin7=strin7+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-7d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-7d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin6=strin6+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-6d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-6d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin5=strin5+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-5d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-5d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin4=strin4+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-4d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-4d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin3=strin3+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-3d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-3d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin2=strin2+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-2d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-2d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin1=strin1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-1d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-1d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin0=strin0+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-0d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-0d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
            else:
                strin10=strin10+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-10d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-10d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin9=strin9+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-9d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-9d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin8=strin8+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-8d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-8d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin7=strin7+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-7d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-7d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin6=strin6+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-6d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-6d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin5=strin5+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-5d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-5d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin4=strin4+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-4d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-4d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin3=strin3+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-3d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-3d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin2=strin2+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-2d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-2d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin1=strin1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-1d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-1d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
                strin0=strin0+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_AMOUNT':"+round(df_.at[filter1['DATUM'].item(),'Delta-0d'],2).__str__()+",'DEVIATION_PERCENT':"+round(df_per.at[filter1['DATUM'].item(),'Delta-0d'],2).__str__()+",'TOTAL_AMOUNT':"+amount+"}"
   
                
         #   print(strin0+strin1+strin2+strin3+strin4+strin5+strin6+strin7+strin8+strin9+strin10)
           
            
            for index, row in cf.iterrows():
                
          
                if(row['delta_days'].days==10):
        
                    df_1.at[filter1['DATUM'].item(),'Delta-10d'] =row['stp-minus-b_statement-date'] 
                   # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", type(df_.at[filter1['DATUM'].item(),'Delta-10d']) )
                # df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-10d'] =max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-10d'], row['reconciled-minus-planned'])
                if(row['delta_days'].days==9):   
                    df_1.at[filter1['DATUM'].item(),'Delta-9d'] =row['stp-minus-b_statement-date'] 
                # df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-9d'] =max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-9d'], row['reconciled-minus-planned'])
                if(row['delta_days'].days==8): 
                    df_1.at[filter1['DATUM'].item(),'Delta-8d'] =row['stp-minus-b_statement-date'] 
               #  df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-8d'] =max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-8d'], row['reconciled-minus-planned'])
                if(row['delta_days'].days==7): 
                    df_1.at[filter1['DATUM'].item(),'Delta-7d'] =row['stp-minus-b_statement-date'] 
               #  df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-7d'] =max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-7d'], row['reconciled-minus-planned']) 
                if(row['delta_days'].days==6): 
                    df_1.at[filter1['DATUM'].item(),'Delta-6d'] =row['stp-minus-b_statement-date'] 
               #  df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-6d'] =max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-6d'], row['reconciled-minus-planned']) 
                if(row['delta_days'].days==5):
                    df_1.at[filter1['DATUM'].item(),'Delta-5d'] =row['stp-minus-b_statement-date']             
                if (row['delta_days'].days==4): 
                    df_1.at[filter1['DATUM'].item(),'Delta-4d'] =row['stp-minus-b_statement-date'] 
               #  df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-4d'] =max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-4d'] , row['reconciled-minus-planned']) 
                if (row['delta_days'].days==3): 
                    df_1.at[filter1['DATUM'].item(),'Delta-3d'] = row['stp-minus-b_statement-date'] 
               #  df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-5d'] =max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-3d'] , row['reconciled-minus-planned']) 
                if (row['delta_days'].days==2): 
                    df_1.at[filter1['DATUM'].item(),'Delta-2d'] = row['stp-minus-b_statement-date'] 
               #  df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-5d'] =max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-2d'] , row['reconciled-minus-planned']) 
                if (row['delta_days'].days==1): 
                    df_1.at[filter1['DATUM'].item(),'Delta-1d'] = row['stp-minus-b_statement-date']
               #  df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-1d'] = max(df_11.at[filter1['Reconciliation date'].dt.strftime('%Y-%m-%d').item(),'Delta-1d'] , row['reconciled-minus-planned'])               
                if (row['delta_days'].days==0):  
                    df_1.at[filter1['DATUM'].item(),'Delta-0d'] = row['stp-minus-b_statement-date']
                
            df_1.round(2)    
            if ttt==0:                 
                 strin10_1=strin10_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-10d'].__str__()+"}"
                 strin9_1=strin9_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-9d'].__str__()+"}"
                 strin8_1=strin8_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-8d'].__str__()+"}"
                 strin7_1=strin7_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-7d'].__str__()+"}"
                 strin6_1=strin6_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-6d'].__str__()+"}"
                 strin5_1=strin5_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-5d'].__str__()+"}"
                 strin4_1=strin4_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-4d'].__str__()+"}"
                 strin3_1=strin3_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-3d'].__str__()+"}"
                 strin2_1=strin2_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-2d'].__str__()+"}"
                 strin1_1=strin1_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-1d'].__str__()+"}"
                 strin0_1=strin0_1+"{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-0d'].__str__()+"}"
            else:
                 strin10_1=strin10_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-10d'].__str__()+"}"
                 strin9_1=strin9_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-9d'].__str__()+"}"
                 strin8_1=strin8_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-8d'].__str__()+"}"
                 strin7_1=strin7_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-7d'].__str__()+"}"
                 strin6_1=strin6_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-6d'].__str__()+"}"
                 strin5_1=strin5_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-5d'].__str__()+"}"
                 strin4_1=strin4_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-4d'].__str__()+"}"
                 strin3_1=strin3_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-3d'].__str__()+"}"
                 strin2_1=strin2_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-2d'].__str__()+"}"
                 strin1_1=strin1_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-1d'].__str__()+"}"
                 strin0_1=strin0_1+",{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+int(row['Plan group#']).__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION_DAYS':"+df_1.at[filter1['DATUM'].item(),'Delta-0d'].__str__()+"}"
   

         #       strin10_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-10d'].__str__()+"}"
        #        strin9_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-9d'].__str__()+"}"
         #       strin8_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-8d'].__str__()+"}"
         #       strin7_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-7d'].__str__()+"}"
         #       strin6_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-6d'].__str__()+"}"
         #       strin5_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-5d'].__str__()+"}"
         #       strin4_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-4d'].__str__()+"}"
         #       strin3_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-3d'].__str__()+"}"
         #       strin2_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-2d'].__str__()+"}"
         #       strin1_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-1d'].__str__()+"}"
         #       strin0_1="{'BUKRS':'"+row['Company']+"','HBKID':'"+row['Bank']+"','HKTID':'"+ row['Currency']+"','PLANNING_GROUP':"+row['Plan group#'].__str__()+",'DATE':'"+row['RecDate'].date().__str__()+"','DEVIATION':"+df_1.at[filter1['DATUM'].item(),'Delta-0d'].__str__()+"}"
                         
        #    print(strin0_1+strin1_1+strin2_1+strin3_1+strin4_1+strin5_1+strin6_1+strin7_1+strin8_1+strin9_1+strin10_1)          
            ttt=ttt+1
            

#df_ = pd.DataFrame(index=list1, columns=columns)
    #df_.at['2019-04-16','Delta-1-day'] = 10
    print("------------------------delta values----------------------")
 #   print (df_)
    print("------------------------delta days----------------------")
 #   print (df_1)
#client = MongoClient('localhost:27017')
#db = client["Cash_management"]
    col = mydb['ResultAmounts']
    
    if (df_.empty==False)and(df_1.empty==False):
        
        strin0="{'DELTA':0, 'VALUES':["+strin0+"]},"
        strin1="{'DELTA':1, 'VALUES':["+strin1+"]},"
        strin2="{'DELTA':2, 'VALUES':["+strin2+"]},"
        strin3="{'DELTA':3, 'VALUES':["+strin3+"]},"
        strin4="{'DELTA':4, 'VALUES':["+strin4+"]},"
        strin5="{'DELTA':5, 'VALUES':["+strin5+"]},"
        strin6="{'DELTA':6, 'VALUES':["+strin6+"]},"
        strin7="{'DELTA':7, 'VALUES':["+strin7+"]},"
        strin8="{'DELTA':8, 'VALUES':["+strin8+"]},"
        strin9="{'DELTA':9, 'VALUES':["+strin9+"]},"
        strin10="{'DELTA':10, 'VALUES':["+strin10+"]}"
        num=getNextSequence(mycol0,"result_id").__str__()
        last_doc="{'RESULT':{'FOR_TABLE': 'STP_Deviation_Amounts','DATETIME':" + get_timestamp() + ",'RESULT_ID':"+num+",'RESULT_SET': ["+strin0+strin1+strin2+strin3+strin4+strin5+strin6+strin7+strin8+strin9+strin10+"]}}"
        
  #      last_doc['RESULT_ID']=getNextSequence(mydb.counters,"result_id")
        last_doc=last_doc.replace("\'", "\"")
   #     last_doc=json.loads(last_doc)
       
#        print(last_doc)

       
    
   
 #-------------------------------------------------------------------------------------------------   
 #   password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
   # with open(r"C:\Users\m.zhukov\Documents\ML_Prototype\controller\qs8certificate.cer", "r") as my_cert_file:        
     #   my_cert_text = my_cert_file.read()
   # r = requests.post("https://ho1000029.hanseorga-ag.de:8103/serrala/ml/ml_provide_result", alldoc, verify='C:/Users/m.zhukov/Documents/ML_Prototype/certificates/')
        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
   # socket.getaddrinfo('localhost', 8080)
   # params = '{"name":"John", "age":31, "city":"New York"}'
        headers = {"Content-Type": "application/json; charset=utf-8",
                   "Accept": "application/json"}

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        conn = http.client.HTTPSConnection('ho1000029.hanseorga-ag.de',port=8103, context=context) 
        conn.request("POST", "/serrala/ml/hoag/m_ml_provide_result ", last_doc, headers)
 #       conn.request("POST", "/serrala/ml/ml_provide_result", last_doc, headers)
        response = conn.getresponse()
        print(response.status, response.reason)
        data = response.read()
        print(data)
        conn.close()
        last_doc=json.loads(last_doc)
   #  print(alldoc)
  # alldoc['DATETIME']=get_timestamp()
#alldoc['RESULT_ID']=getNextSequence(db.counters,"result_id")
 #   print(alldoc)
        col.insert(last_doc)


        col = mydb['ResultDays']
        
 #------------------------------------------------------------------------------------------------------------    
  
        strin0_1="{'DELTA':0, 'VALUES':["+strin0_1+"]},"
        strin1_1="{'DELTA':1, 'VALUES':["+strin1_1+"]},"
        strin2_1="{'DELTA':2, 'VALUES':["+strin2_1+"]},"
        strin3_1="{'DELTA':3, 'VALUES':["+strin3_1+"]},"
        strin4_1="{'DELTA':4, 'VALUES':["+strin4_1+"]},"
        strin5_1="{'DELTA':5, 'VALUES':["+strin5_1+"]},"
        strin6_1="{'DELTA':6, 'VALUES':["+strin6_1+"]},"
        strin7_1="{'DELTA':7, 'VALUES':["+strin7_1+"]},"
        strin8_1="{'DELTA':8, 'VALUES':["+strin8_1+"]},"
        strin9_1="{'DELTA':9, 'VALUES':["+strin9_1+"]},"
        strin10_1="{'DELTA':10, 'VALUES':["+strin10_1+"]}"
        
       
        last_doc="{'RESULT':{'FOR_TABLE': 'STP_Deviation_Days','DATETIME':" + get_timestamp() + ",'RESULT_ID':"+num+",'RESULT_SET': ["+strin0_1+strin1_1+strin2_1+strin3_1+strin4_1+strin5_1+strin6_1+strin7_1+strin8_1+strin9_1+strin10_1+"]}}"
        
      #  last_doc['RESULT_ID']=getNextSequence(mydb.counters,"result_id")
        last_doc=last_doc.replace("\'", "\"")
     #   last_doc=json.loads(last_doc)
        
#        print(last_doc)
    
    
     #-------------------------------------------------------------------------------------------------   
 #   password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
   # with open(r"C:\Users\m.zhukov\Documents\ML_Prototype\controller\qs8certificate.cer", "r") as my_cert_file:        
     #   my_cert_text = my_cert_file.read()
   # r = requests.post("https://ho1000029.hanseorga-ag.de:8103/serrala/ml/ml_provide_result", alldoc, verify='C:/Users/m.zhukov/Documents/ML_Prototype/certificates/')
        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
   # socket.getaddrinfo('localhost', 8080)
   # params = '{"name":"John", "age":31, "city":"New York"}'
        headers = {"Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json"}

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        conn = http.client.HTTPSConnection('ho1000029.hanseorga-ag.de',port=8103, context=context)     
     #   conn.request("POST", "/serrala/ml/ml_provide_result", last_doc, headers)
        conn.request("POST", "/serrala/ml/hoag/m_ml_provide_result ", last_doc, headers)
        response = conn.getresponse()
        print(response.status, response.reason)
        data = response.read()
        print(data)
        conn.close()
 #------------------------------------------------------------------------------------------------------------    
 
        last_doc=json.loads(last_doc)
 #   alldoc['DATETIME']=get_timestamp()
#alldoc['RESULT_ID']=getNextSequence(db.counters,"result_id")
        col.insert(last_doc)
        
    else: print("empty input datasets")
    return num
#data={
  #  "ACCOUNTS": [
   # {
   #   "BUKRS": "HOAG",
   #   "HBKID": "COM01",
   #   "HKTID": "COEU2"
   # },     
  #  {
   #   "BUKRS": "HOAG",
   #   "HBKID": "COM01",
   #   "HKTID": "COEU1"
   # }         
 #  ],
  # "PLANNING_GROUPS": [
    
 #  ]
#}
 

data=    {
  "ACCOUNTS": [
#    {
      
#    }
  ],
  "INTERVAL": {
    
  },
  "PLANNING_GROUPS": [
    
  ]
   }
  
#l=len(data["ACCOUNTS"])#[0]["BUKRS"]  2
  
#data1=data["PLANNING_GROUPS"][1]

#i
#while(len>0):
#k=0
#for x in data["ACCOUNTS"]:
#    print("!",x)
#    df1=df1.loc[(df1['HKTID'] ==  data["ACCOUNTS"][k]["HKTID"]) & (df1['BUKRS'] ==  data["ACCOUNTS"][k]['BUKRS'])& (df1['HBKID'] ==  data["ACCOUNTS"][k]["HBKID"])]
#    k=k+1
    
    
#data=""

#
#print(data1)

#stpanalytics(data)

#print(alldoc)

#https://stackoverflow.com/questions/11322430/how-to-send-post-request



#print(r.status_code, r.reason)
#print(r.text[:300] + '...')




