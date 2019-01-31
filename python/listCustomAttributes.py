'''
Created on Jul 16, 2018

@author: dwrigley

get a list of all custom attributes in EDC - returning the Name and Id
the id would be used for any search/custom import activiies
output printed to the console
'''

import requests
from requests.auth import HTTPBasicAuth
import time

start_time = time.time()

# ******************************************************
# change these settings for your catalog service
# ******************************************************
# set variables for connecting to the catalog
# and running a query to get a result-set
# the processItem function will be called for each item
# ******************************************************
catalogServer = 'http://napslxapp01:9085'
uid = 'Administrator'
pwd = 'Administrator'
# pwd=uid;
pageSize = 500    # number of objects for each page/chunk
# ******************************************************
# end of parameters that should be changed
# ******************************************************
resturl = catalogServer + '/access/2/catalog/models/attributes'
header = {"Accept": "application/json"} 


# main function
def main():
    """
    call GET /access/2/catalog/models/attributes
     and GET /access/2/catalog/models/referenceAttributes
    the /access/2/catalog/models/attributes ca;ll returns all attributes
    (system + custom), so we filter for only the custom attrs
    these start with "com.infa.appmodels.ldm.

    output - prints the atrribute name, id and some other properties to console
    """
    global resturl

    total = 1000  # initial value - set to > 0 - replaced after first call
    offset = 0
    page = 0
    
    print("url=" + resturl)
    print("user=" + uid)
    print("")
    print(header)
        
    attrCount = 0
    custAttrCount = 0
    
    while offset < total:
        page += 1
        parameters = {'offset': offset, 'pageSize': pageSize}

        # execute catalog rest call, for a page of results
        resp = requests.get(resturl, params=parameters, headers=header,
                            auth=HTTPBasicAuth(uid, pwd))
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            break
        
        resultJson = resp.json()
        total = resultJson['metadata']['totalCount']
        #print("objects found: " + str(total) + " processing:" + str(offset+1) 
        #                        + "-" + str(offset+pageSize) + " pagesize="
        #                        + str(pageSize) + " currentPage=" + str(page) 
        #      );

        # for next iteration
        offset += pageSize
 
        # for each attribute found...
        for attrDef in resultJson["items"]:
            attrCount += 1
            attrId = attrDef["id"]
            attrName = attrDef["name"]
            dataType = attrDef["dataTypeId"]
            sortable = attrDef["sortable"]
            facetable = attrDef["facetable"]
            if attrId.startswith("com.infa.appmodels.ldm."):
                custAttrCount += 1
                print("Name: " + attrName + " id=" + attrId + " type=" +
                      dataType + " sortable=" + str(sortable) +
                      " facetable=" + str(facetable))         
                
    # end of while loop

    # note /access/2/catalog/models/attributes does not return classifications
    total = 1000  # initial value - set to > 0 - will be over-written by the count of objects returned
    offset = 0
    page = 0
    classificationCount = 0

    print("")
    print("reference attributes:")

    resturl = catalogServer + '/access/2/catalog/models/referenceAttributes'
    while offset < total:
        page += 1
        parameters = {'offset': offset, 'pageSize': pageSize}

        # execute catalog rest call, for a page of results
        resp = requests.get(resturl, params=parameters, headers=header,
                            auth=HTTPBasicAuth(uid, pwd))
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            break
        
        resultJson = resp.json()
        total = resultJson['metadata']['totalCount']

        # for next iteration
        offset += pageSize
 
        # for each attribute found...
        for attrDef in resultJson["items"]:
            classificationCount += 1
            attrId = attrDef["id"]
            attrName = attrDef["name"]
            dataType = attrDef["refDataTypeId"]
            sortable = attrDef["sortable"]
            facetable = attrDef["facetable"]
            if attrId.startswith("com.infa.appmodels.ldm."):
                custAttrCount += 1
                print("Name: " + attrName + " id=" + attrId + " type=" +
                      dataType + " sortable=" + str(sortable) +
                      " facetable=" + str(facetable))         

    print("")
    print("Finished - run time = %s seconds ---" % (time.time() - start_time))
    print("total attributes=" + str(attrCount))
    print("custom attributes=" + str(custAttrCount))
    print("classifification attributes=" + str(classificationCount))
   

# call main - if not already called or used by another script
if __name__ == "__main__":
    main()            

