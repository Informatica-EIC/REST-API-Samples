"""
Created on Jul 16, 2018

@author: dwrigley

get a list of all custom attributes in EDC - returning the Name and Id
the id would be used for any search/custom import activiies
output printed to the console
"""

import requests
from requests.auth import HTTPBasicAuth
import time
import sys
import os
import urllib3
import csv

urllib3.disable_warnings()


start_time = time.time()
# initialize http header - as a dict
header = {}
auth = None
# create a re-useable session
sess = requests.Session()
sess.headers.update({"Accept": "application/json"})
# s.auth = ('user', 'pass')

"""
# ******************************************************
# change these settings for your catalog service
# ******************************************************
# set variables for connecting to the catalog
# and running a query to get a result-set
# the processItem function will be called for each item
# Note:
#     preferred way is to use Authorization http header
#     vs id/pwd
# environment variables can be used here
  INFA_EDC_URL = url for edc http[s]://<server>:<port>
  INFA_EDC_AUTH=<auth string>
# ******************************************************
"""

# enter your default catalog url here - will be over-ridden by env var or cmd arg
catalogUrl = ""
verify = False
# catalogServer = "https://napslxapp01:19085"
if "INFA_EDC_URL" in os.environ:
    catalogUrl = os.environ["INFA_EDC_URL"]
    print("using EDC URL=" + catalogUrl + " from INFA_EDC_URL env var")

if "INFA_EDC_AUTH" in os.environ:
    print("using INFA_EDC_AUTH from environment")
    header["Authorization"] = os.environ["INFA_EDC_AUTH"]
    sess.headers.update({"Authorization": os.environ["INFA_EDC_AUTH"]})
    # print(os.environ['INFA_EDC_AUTH'])
    auth = None
else:
    # hard-code (or prompt the user for id/pwd here)
    uid = "edcuser"
    pwd = ""
    print("no setting found for INFA_EDC_AUTH - using user=" + uid + " from script")
    auth = HTTPBasicAuth(uid, pwd)

if "INFA_EDC_SSL_PEM" in os.environ:
    verify = os.environ["INFA_EDC_SSL_PEM"]
    sess.verify = os.environ["INFA_EDC_SSL_PEM"]
    print("using ssl certificate from env var INFA_EDC_SSL_PEM=" + verify)


# pwd=uid;
pageSize = 500  # number of objects for each page/chunk

# the csv lineage file to write to
csvFileName = "custom_attributes.csv"
csvFilePath = "out/"
outputFile = csvFilePath + csvFileName


# ******************************************************
# end of parameters that should be changed
# ******************************************************


def main():
    """
    call GET /access/2/catalog/models/attributes
     and GET /access/2/catalog/models/referenceAttributes
    the /access/2/catalog/models/attributes call returns all attributes
    (system + custom), so we filter for only the custom attrs
    these start with "com.infa.appmodels.ldm.

    output - prints the attribute name, id and some other properties to console

    TODO:  - add logging to file
    """
    global resturl
    global verify
    global sess

    # create and initialize the header for the output csv file
    fCSVFile = open(outputFile, "w", newline="", encoding="utf-8")
    print("custom attributes csv file initialized: " + outputFile)
    colWriter = csv.writer(fCSVFile)
    colWriter.writerow(["Name", "Id", "Type", "Facetable", "Sortable", "AttributeType"])

    # extract for all "attributes"
    baseurl = catalogUrl + "/access/2/catalog/models/"
    attrCount, custAttrCount = getCustomAttribute(
        sess, baseurl + "attributes", colWriter
    )

    # extract  all "referenceAttributes"
    allClassifications, classificationCount = getCustomAttribute(
        sess, baseurl + "referenceAttributes", colWriter
    )

    print("")
    print("Finished - run time = %s seconds ---" % (time.time() - start_time))
    print("total attributes=" + str(attrCount))
    print("custom attributes=" + str(custAttrCount))
    print("total classification attributes=" + str(allClassifications))
    print("custom classification attributes=" + str(classificationCount))

    fCSVFile.close()
    return


def getCustomAttribute(session, resturl, colWriter):
    """
    get a list of custom attributes or reference attributes
    write the result to the csv file colWriter

    both api calls will return standard and custom attributes
    so we filter for anything starting with com.infa.appmodels.ldm.
    """
    total = 1000  # initial value - set to > 0 - replaced after first call
    offset = 0
    page = 0

    print("url=" + resturl)
    # print("user=" + uid)
    print("")
    print("executing get: " + resturl.rsplit("/access/2", 1)[-1])

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
            attrName = attrDef["name"]
            if "dataTypeId" in attrDef:
                dataType = attrDef["dataTypeId"]
            if "refDataTypeId" in attrDef:
                # if (resturl.endswith("referenceAttributes")):
                dataType = attrDef["refDataTypeId"]
            sortable = attrDef["sortable"]
            facetable = attrDef["facetable"]
            if attrId.startswith("com.infa.appmodels.ldm."):
                custAttrCount += 1
                # print to console
                print(
                    "Name: "
                    + attrName
                    + " id="
                    + attrId
                    + " type="
                    + dataType
                    + " sortable="
                    + str(sortable)
                    + " facetable="
                    + str(facetable)
                )
                # write to csv
                colWriter.writerow(
                    [
                        attrName,
                        attrId,
                        dataType,
                        str(facetable),
                        str(sortable),
                        resturl.rsplit("/", 1)[-1],
                    ]
                )

    # end of while loop
    return attrCount, custAttrCount


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
