from __future__ import with_statement
from multiprocessing import AuthenticationError
from posixpath import split
from pkg_resources import split_sections
import requests
import sys
import json
import pandas as pd
import logging
import datetime
import os
import glob
import Config
import snowflake.connector
from requests.structures import CaseInsensitiveDict

# initialize the work directory and log settings
work_dir = Config.common_dict["work_dir"]
logging.basicConfig(filename=work_dir+'\\app.log',level=logging.INFO)
logging.info(20 * '*-*-*-*-' + '\n')

#saving user argument
#print(len(sys.argv))
if len(sys.argv) > 1:
     days_to_load = int(sys.argv[1])
else:days_to_load = 0
print ("DayReload-"+str(days_to_load))
logging.info(str(datetime.datetime.now()) + ' - ' + 'Days Reload:'+str(days_to_load))
#print (days_to_load)

auth_base_url  = Config.zoho_dict["base_url"]
recruitapi_base_url =Config.zoho_dict["api_url"]
headers = CaseInsensitiveDict()
object_list = ['Candidates','JobOpenings','Interviews','Notes']



#open connection with snowflake
conSnow = snowflake.connector.connect (
    user = Config.cfsnow_dict["user"],
    password = Config.cfsnow_dict["password"],
    account = 'zohoaccount'
)
cs = conSnow.cursor()
clientid = Config.zoho_dict["clientid"]
clientsecret = Config.zoho_dict["clientsecret"]
#you need get this from https://www.zoho.com/analytics/api/#oauth whenever you want to generate new refresh token
code = Config.zoho_dict["code"]
#permanent token
refresh_token=Config.zoho_dict["refresh_token"]

#get refresh access token-onetime run code
def get_refresh_token():
    logging.info(str(datetime.datetime.now()) + ' - ' + 'Requesting refresh_token')
    param = {"code":code,"client_id":clientid,"client_secret":clientsecret,"grant_type":"authorization_code"}
    resp = requests.post(auth_base_url + 'oauth/v2/token', params=param)
    logging.info(str(datetime.datetime.now()) + ' - ' + 'Response status code: '+str(resp.status_code))
    if resp.status_code==200:
       token_info = resp.json()
       refresh_token = token_info['refresh_token']
       access_token = token_info['access_token']
       print(refresh_token)
       print(access_token)

##Get it using refresh_token - life of access_token is 1 hr
def get_access_token():
    logging.info(str(datetime.datetime.now()) + ' - ' + 'Requesting access_token')
    param = {"refresh_token":refresh_token,"client_id":clientid,"client_secret":clientsecret,"grant_type":"refresh_token"}
    resp = requests.post(auth_base_url + 'oauth/v2/token', params=param)
    logging.info(str(datetime.datetime.now()) + ' - ' + 'Response status code: '+str(resp.status_code))
    if resp.status_code==200:
       token_info = resp.json()
       access_token = token_info['access_token']
       #print(access_token)
       return access_token

def get_association(obj_association,headers):
     try:
          #get form record count
          logging.info(str(datetime.datetime.now()) + ' - ' + 'Requesting Association for:'+obj_association)
          association_file = open(work_dir + '\\' + obj_association + '_association_'+ str(datetime.datetime.now().strftime('%Y%m%d%H%M%S')) + '.json','a+')
          logging.info(str(datetime.datetime.now()) + ' - ' + 'Started Writing Association response to file:' + association_file.name)
          #print('Job Count: ' + str(len(job_id_list)))
          for job_id in job_id_list:
            page = 1
            per_page = 200
            association_res = []
            #Loop to iteriate through all records in multiple pages until you find no data
            while 1 == 1:
                param = {"page":page,"per_page":per_page}
                #print(param)
                #print(headers)
                response = requests.get(recruitapi_base_url + obj_association +'/' + str(job_id) +'/associate',headers=headers,params=param)
                logging.info(str(datetime.datetime.now()) + ' - ' + 'Response status code: '+str(response.status_code))
                print('Response status code: '+str(response.status_code))
                if response.status_code==200:
                    association_res = response.json()
                    try:
                        if association_res['data']!='[]':
                           #Logic to save Job id to find association with Candidates
                            for entries in association_res['data']:
                                entries['job_id'] = job_id
                            association.extend(association_res['data'])
                    except IndexError:
                        pass
                    if str(association_res['info']['more_records']) =='True':
                         page = page + 1          
                    else:
                         #print("break")
                         break
                elif response.status_code==204:
                     logging.info(str(datetime.datetime.now()) + ' - ' + 'No data from Endpoint:' + obj_association +'-'+str(job_id)+ ' Message:' + str(response.content))
                     break
                else:
                     logging.info(str(datetime.datetime.now()) + ' - ' + 'Error in pulling data from Endpoint:' + obj_association + ' Error Message:' + str(response.content))
                     break
          json.dump(association,association_file)          
          association_file.close()               
     except (IOError,KeyError) as e:
          logging.error(str(datetime.datetime.now()) + ' - ' + 'Error occurred: ' + str(e))
     except IndexError as e:
          print(response.content)
          logging.error(str(datetime.datetime.now()) + ' - ' + 'Error occurred: ' + str(e))       

try:
    #get_refresh_token()
    access_token = get_access_token()
    #access_token = '1000.3f3a61b6c36e23b382f92b91a840bd02.9fe22bfb8fe9f707e30730eb16d5daf9'
    headers["Authorization"] = "Zoho-oauthtoken " + access_token
    headers["If-Modified-Since"] = (datetime.date.today()+ datetime.timedelta(days=-days_to_load)).strftime('%Y-%m-%dT00:00:00')
    for obj in object_list:
        page = 1
        per_page = 200
        job_id_list = []
        association = list()
        logging.info(str(datetime.datetime.now()) + ' - ' + 'Requesting Data for Endpoint:'+obj)
        file = open(work_dir + '\\' + obj.split("/")[0] + '_' + str(datetime.datetime.now().strftime('%Y%m%d%H%M%S')) + '.json','a+')
        result = []
        #Loop to iteriate through all records in multiple pages until you find no data
        while 1 == 1:
            param = {"page":page,"per_page":per_page}
            #print(param)
            #print(headers)
            resp = requests.get(recruitapi_base_url + obj, headers=headers,params=param)
            logging.info(str(datetime.datetime.now()) + ' - ' + 'Response status code: '+str(resp.status_code))
            if resp.status_code==200:
                resj = resp.json()
                print("Object Name:"+str(obj)+ " Count:" + str(resj['info']['count']))        
                if resj['info']['count'] > 0:
                    if(obj == 'JobOpenings'):
                    #Logic to save Job id to find association with Candidates
                         for entries in resj['data']:
                              job_id_list.append(entries['id'])           
                    result.extend(resj['data'])  
                else:
                    logging.info(str(datetime.datetime.now()) + ' - ' + 'No records found for give time frame for Endpoint:'+obj)
                    break
                #print(str(data['info']['more_records']))
                if str(resj['info']['more_records']) =='True':
                    page = page + 1          
                else:
                    #print("break")
                    break
            else:
                logging.info(str(datetime.datetime.now()) + ' - ' + 'Error in pulling data from Endpoint:' + obj + 'Error Message:' + str(resp.content))
                break

        logging.info(str(datetime.datetime.now()) + ' - ' + 'Started Writing response to file:' + file.name)
        json.dump(result,file)
        #print(job_id_list)
        file.close()
        if(obj == 'JobOpenings'):
            obj_association = 'Job_Openings' 
            get_association(obj_association,headers)
    #Uploading Hubspot API data files to snowflake stage
    logging.info(str(datetime.datetime.now()) + ' - ' + 'Started Uploading Zoho API Data to Snowflake stage')
    cs.execute("USE DATABASE SNOWDB")
    cs.execute("USE SCHEMA API_ETL")
    cs.execute("PUT file://"+work_dir+"/*.json @API_PARSER_STAGE/ZOHO")
    logging.info(str(datetime.datetime.now()) + ' - ' + 'Uploaded Zoho API Data to Snowflake stage'+ '\n' )
          
    #cleaning data files in work directory
    #get a recursive list of f  ile paths that matches pattern
    fileList = glob.glob(work_dir+'/*.json')
    for file in fileList:
        os.remove(file)
    
except (IOError, snowflake.connector.errors.ProgrammingError ,KeyError) as e:
       logging.error(str(datetime.datetime.now()) + ' - ' + 'Error occurred: ' + str(e))

