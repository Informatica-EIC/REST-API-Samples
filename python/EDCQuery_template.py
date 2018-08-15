'''
Created on Jul 16, 2018

@author: dwrigley

This template can be copied & used to query the catalog and process each item returned
it handles the paging model (see pageSize variable) 

'''

#coding=utf8

import requests
from requests.auth import HTTPBasicAuth
import time
import edcutils

start_time = time.time()

# ******************************************************
# change these settings for your catalog service
# ******************************************************
# set variables for connecting to the catalog
# and running a query to get a result-set
# the processItem function will be called for each item
# ******************************************************
catalogServer='http://napslxapp01:9085'
uid='Administrator'
#pwd='admin'
pwd=uid;
query = " core.allclassTypes:( \
        com.infa.ldm.relational.Column OR \
        com.infa.ldm.relational.ViewColumn OR \
        com.infa.ldm.file.delimited.DelimitedField OR \
        com.infa.ldm.file.xml.XMLFileField OR \
        com.infa.ldm.file.json.JSONField OR \
        com.infa.ldm.adapter.Field OR \
        com.infa.ldm.file.avro.AVROField OR \
        com.infa.ldm.file.parquet.PARQUETField \
        ) \
        and core.resourceName:acme_crm \
        "
pageSize=10    # e.g. 10 objects for each page/chunk - change to suit your environment
# ******************************************************
# end of parameters that should be changed 
# ******************************************************

objectsurl = catalogServer + '/access/2/catalog/data/objects'
header = {"Accept": "application/json"} 
itemCount=0

# each item that is returned from the query - is processed here
# @note python 2.7 does not allow us to specify the parameter type...
def processItem(anItem, itemCount):
    ''' 
    put your code here - that does something with the item
    '''
    itemId=anItem["id"]
    itemName=edcutils.getFactValue(anItem, "core.name")
    print("\titem " + str(itemCount) + "=" + itemId + " name=" + itemName)
    

def main():
    # main starts here - run the query processing all items
    # note:  this version supports the paging model, to process the result set in chunks

    total=1000  # initial value - set to > 0 - will be over-written by the count of objects returned
    offset=0
    page=0
    
    print("catalog service=" + catalogServer )
    print("user=" + uid)
    print('query=' + query)
    print("")
    
    while offset<total:
        page_time = time.time()
        parameters = {'q': query, 'offset': offset, 'pageSize': pageSize}
        page += 1
        resp = requests.get(objectsurl, params=parameters, headers=header, auth=HTTPBasicAuth(uid,pwd))
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            break
        
        resultJson = resp.json()
        total=resultJson['metadata']['totalCount']
        print("objects found: " + str(total) + " offset: " + str(offset) + " pagesize="+str(pageSize) + " currentPage=" + str(page) );

        # for next iteration
        offset += pageSize;
 
        global itemCount
        for foundItem in resultJson["items"]:
            itemCount+=1
            processItem(anItem = foundItem, itemCount=itemCount)
            
        # end of page processing
        print("    page processed - %s seconds ---" % (time.time() - page_time))

    # end of while loop
    print("Finished - run time = %s seconds ---" % (time.time() - start_time))
    

# call main - if not already called or used by another script 
if __name__== "__main__":
    main()            

