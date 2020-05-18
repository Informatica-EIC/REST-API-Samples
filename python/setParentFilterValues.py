"""
Created on May 11, 2020

@author: lntinfa

usage:
  setParentFilterValues -c/--edcurl" EDCURL (or env:INFA_EDC_URL)
  -a/--auth BASICAUTH (or env:INFA_EDC_AUTH)
  -c/--customattr CUSTOMATTR

Note:  requires python 3 (3.6+)

get a list of all custom attributes in EDC & count the usage of each attribute
the id would be used for any search/custom import activiies
output printed to the console
"""
import requests
import time
import sys
import urllib3
import csv
import argparse
import os
import re
import json
#from pathlib import PurePath
from edcSessionHelper import EDCSession

# global var declaration (with type hinting)
#edcSession: EDCSession = None
urllib3.disable_warnings()

start_time = time.time()
# initialize http header - as a dict
# header = {}
# auth = None

pageSize = 500  # number of objects for each page/chunk

edcSession = EDCSession()
parser = argparse.ArgumentParser(parents=[edcSession.argparser])
#parser.add_argument("-o", "--output", default="", help="output folder - e.g. .out")
parser.add_argument("-t", "--customattr", default="Parent Filter", help="name of the custom attribute where the value should be set")
parser.add_argument("-r", "--resource", default="sql_oltp", help="name of the resource to update")


def main():
    """
    call the attribute API to identify the attribute ID
    call the object API to retrive all relational object from the resource provide as parameter
    update the value of the custom attribute for all object found with the parent schema

    output - prints the the progress on updatnge the objects in the catalog
    """
    #p = PurePath(sys.argv[0])
    #print(f"{p.name} starting in {os.getcwd()}")

    args, unknown = parser.parse_known_args()
    # initialize http session to EDC, storeing the baseurl
    edcSession.initUrlAndSessionFromEDCSettings()
    print(
        f"args from cmdline/env vars: url={edcSession.baseUrl}"
        f"  session={edcSession.session}"
    )

    if args.customattr is not None:
       customattrName = args.customattr
    if args.resource is not None:
       resourceName = args.resource

    attributeList = []

    # extract for all "attributes"
    print("\nlisting custom attributes (non reference)")
    baseurl = edcSession.baseUrl + "/access/2/catalog/models/"
    attrCount, custAttrCount, attList = getCustomAttributes(
        edcSession.session, baseurl + "attributes")
    attributeList = list(attList)

    attr = {}
    attr["name"] = customattrName

    for a in attributeList:
      if a["name"] == customattrName:
        attr["id"] = a["id"]
        
    if "id" in attr :
      print("Attribute to update is \""+attr["name"]+"\" with id \""+attr["id"]+"\"") 
    else:
      print("Could not find the custom attribute in the catalog, aborting")
      return


    updateObjectAttr(edcSession.session, edcSession.baseUrl,resourceName,attr)
   
    print("")
    print(f"Finished - run time = {time.time() - start_time:.2f} seconds ---")
    print(f"          attributes custom/total={custAttrCount}/{attrCount}")



    return


# {{{ getCustomAttributes
def getCustomAttributes(session, resturl ):
    """
    get a list of custom attributes or reference attributes
    write the result to the csv file colWriter

    both api calls will return standard and custom attributes
    so we filter for anything starting with com.infa.appmodels.ldm.
    """
    total = 1000  # initial value - set to > 0 - replaced after first call
    offset = 0
    page = 0

    attrList = []
    print(f"\turl={resturl}")

    attrCount = 0
    custAttrCount = 0

    while offset < total:
        page += 1
        parms = {"offset": offset, "pageSize": pageSize}

        # execute catalog rest call, for a page of results
        try:
            resp = session.get(resturl, params=parms, timeout=3)
        except requests.exceptions.RequestException as e:
            print("Error connecting to : " + resturl)
            print(e)
            # exit if we can't connect
            sys.exit(1)

        # no execption rasied - so we can check the status/return-code
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            # since we are in a loop to get pages of objects - break will exit
            # break
            # instead of break - exit this script
            sys.exit(1)

        resultJson = resp.json()
        # store the total, so we know when the last page of results is read
        total = resultJson["metadata"]["totalCount"]
        # for next iteration
        offset += pageSize

        # for each attribute found...
        for attrDef in resultJson["items"]:
            attrCount += 1
            attrId = attrDef["id"]
            # skip any non-custom attributes
            if not attrId.startswith("com.infa.appmodels.ldm."):
                continue

            # only custom attributes get to this point
            attrName = attrDef["name"]
            # regular custom attributes (non reference)
            if "dataTypeId" in attrDef:
                dataType = attrDef["dataTypeId"]
            # reference attributes (linked to axon/bg/user)
            if "refDataTypeId" in attrDef:
                dataType = attrDef["refDataTypeId"]
            sortable = attrDef["sortable"]
            facetable = attrDef["facetable"]

            custAttrCount += 1

            #instCount = countCustomAttributeInstances(session, session.baseUrl, attrId)

            # add to list (for further processing)
            attrList.append({"id": attrId, "name": attrName})

    # end of while loop
    return attrCount, custAttrCount, attrList
# }}}

# {{{ updateObjectAttr
def updateObjectAttr(session, catalogUrl, resourceName, attr):
    """
    given a resource name, and the attribute , we update the attribute value with the parent Schema
    """

    objectType = "com.infa.ldm.relational.*"

    objectParentType = ['core.DataSet','core.DataElement']

    decount=0
    dscount=0
    for opt in objectParentType:
  
      url = catalogUrl + "/access/2/catalog/data/objects"
      query = (
          "core.resourceName:"
          + resourceName
          + " and core.classType:"
          + objectType
  	+ " and core.allclassTypes:"
  	+ opt
      )
  
      print("\turl=" + url)
      print("\tquery=" + query)
   
      totalObjects = 10000000
      offset = 0
      pageSize = 10
      while offset <= totalObjects:
  
        objects = { "items" : [] }
        #parameters = {"q": query, "fl": "core.allclassTypes","offset": offset, "pageSize": pageSize}
        parameters = {"q": query, "includeSrcLinks": False, "includeDstLinks": False, "offset": offset, "pageSize": pageSize}
        # header using uid/pwd (no Authorization)
        header = {"Accept": "application/json"}
        # header using Authorization - no need to use uid/pwd in the get call
        # header = {"Accept": "application/json", "Authorization": authCredentials}
       # print("\theader=" + str(header))
    
        response = session.get(
            url, headers=header, params=parameters
        )
  
  
        #print("\tEtag:"+response.headers["ETag"])
        etag=response.headers["ETag"]
        # response = requests.get(url,params=parameters,headers=header)
        rc = response.status_code
        if rc != 200:
            print("error reading object: rc=" + str(rc) + " response:" + str(response.json))
            if rc == 401:
                print(
                    "\t401:Possible Missing/bad credentials"
                )
                print(str(response))
            return
    
        # get the total # of objects returned (first part of the json resultset)
        totalObjects = response.json()["metadata"]["totalCount"]

        print("\tProcessing :\t" +str(offset)+"-"+str(offset+pageSize)+" of "+ str(totalObjects)  )

        offset = offset + pageSize
        
        for item in response.json()["items"]:
           if opt == "core.DataElement":
              decount += 1
              m = re.search('\/([^\/]+)\/[^\/]+\/[^\/]+$',item["id"])
              parent=m.group(1)
           if opt == "core.DataSet":
              dscount += 1
              m = re.search('\/([^\/]+)\/[^\/]+$',item["id"])
              parent=m.group(1)
    
           fact = {
              "attributeId": attr["id"],
              "value": parent,
              "readOnly": False,
              "label": attr["name"],
              "providerId": "enrichment",
              "modifiedBy": "system"
           }

           updateNotRequired = 0
           for j in item["facts"]:
             if j["attributeId"] == attr["id"]:
               updateNotRequired = 1

           if updateNotRequired == 0:
             item["facts"].append(fact)
             
           objects["items"].append(item)

#        print(objects)  


        header = {
            "Accept": "application/json"
            ,"Content-Type":"application/json"
            ,"If-Match": etag
        }
        # header using Authorization - no need to use uid/pwd in the get call
        # header = {"Accept": "application/json", "Authorization": authCredentials}
        # print("\theader=" + str(header))
    
        response = session.put(
            url, headers=header,  data=json.dumps(objects)
        )
        rc = response.status_code
        if rc != 200:
            print("error reading object: rc=" + str(rc) + " response:" + str(response.json()))
            if rc == 401:
                print(
                    "\t401:Possible Missing/bad credentials"
                )
                print(str(response))
            return
        else:
           print("\tUpdate successful!")
      

      print("\tobjects returned: " + str(totalObjects))
  
    print("\t DataElement = " + str(decount))
    print("\t DataSet = " + str(dscount))

    return
# }}}

if __name__ == "__main__":
    # call main - if not already called or used by another script
    main()



# vim: set fdm=marker:
