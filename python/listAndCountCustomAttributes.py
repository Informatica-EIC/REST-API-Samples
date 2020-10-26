"""
Created on Jan 7, 2020

@author: dwrigley

usage:
  listAndCountCustomAttributes options

  output written to custom_attributes.csv

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
from pathlib import PurePath
from edcSessionHelper import EDCSession

# global var declaration (with type hinting)
edcSession: EDCSession = None
urllib3.disable_warnings()

start_time = time.time()
# initialize http header - as a dict
header = {}
auth = None

# number of objects for each page/chunk
pageSize = 500

# the csv lineage file to write to
csvFileName = "custom_attributes.csv"
# the path to write to - can be overwritten by -o cmdlink parameter
csvFilePath = "out/"

edcSession = EDCSession()
parser = argparse.ArgumentParser(parents=[edcSession.argparser])
parser.add_argument("-o", "--output", default="", help="output folder - e.g. .out")


def main():
    """
    call GET /access/2/catalog/models/attributes
     and GET /access/2/catalog/models/referenceAttributes
    the /access/2/catalog/models/attributes call returns all attributes
    (system + custom), so we filter for only the custom attrs
    these start with "com.infa.appmodels.ldm.

    output - prints the attribute name, id and some other properties to console
             + count the # of objects using the attribute
    """
    p = PurePath(sys.argv[0])
    print(f"{p.name} starting in {os.getcwd()} args={sys.argv[1:]}")

    args, unknown = parser.parse_known_args()
    # initialize http session to EDC, storeing the baseurl
    edcSession.initUrlAndSessionFromEDCSettings()
    print(
        f"args from cmdline/env vars: url={edcSession.baseUrl}"
        f"  session={edcSession.session}"
    )

    # test the connection - see if the version is 10.4.0 or later
    rc, json = edcSession.validateConnection()
    print(f"validated connection: {rc} {json}")

    # create the output path if it does not exist
    if args.output is not None:
        csvFilePath = args.output
        print(f"csv file path={csvFilePath}")
        if csvFilePath != "":
            if not os.path.exists(csvFilePath):
                os.makedirs(csvFilePath)

        outputFile = csvFilePath + csvFileName

    attributeList = []

    # create and initialize the header for the output csv file
    fCSVFile = open(outputFile, "w", newline="", encoding="utf-8")
    print("custom attributes csv file initialized: " + outputFile)
    colWriter = csv.writer(fCSVFile)
    colWriter.writerow(
        ["Name", "Id", "Type", "Facetable", "Sortable", "AttributeType", "UsageCount"]
    )

    # extract for all "attributes"
    print("\nlisting custom attributes (non reference)")
    baseurl = edcSession.baseUrl + "/access/2/catalog/models/"
    attrCount, custAttrCount, attList = getCustomAttributes(
        edcSession.session, baseurl + "attributes", colWriter
    )
    attributeList = list(attList)

    # extract  all "referenceAttributes"
    print("\nlisting custom reference attributes")
    allClassifications, classificationCount, attList = getCustomAttributes(
        edcSession.session, baseurl + "referenceAttributes", colWriter
    )
    # append the classification attrs to the attribute list
    attributeList.extend(list(attList))

    print("")
    print(f"Finished - run time = {time.time() - start_time:.2f} seconds ---")
    print(f"          attributes custom/total={custAttrCount}/{attrCount}")
    print(
        f"classification attrs custom/total={classificationCount}/{allClassifications}"
    )

    fCSVFile.close()
    return


def getCustomAttributes(session, resturl, colWriter):
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
        # for edc 10.4.0 + we can filter by packageId (only lising custom attributes)
        parms = {
            "offset": offset,
            "pageSize": pageSize,
        }
        if edcSession.edcversion >= 10400:
            parms["packageId"] = "com.infa.appmodels.ldm"
            print(f"\tversion {edcSession.edcversion} > 10400 - adding package filter packageId=com.infa.appmodels.ldm")
            # print(f"\tv10.4+ found - parms={parms}")

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
        print(
            f"\tprocessing page {page} with {offset+1}-{offset+pageSize} "
            f"objects out of {total}"
        )
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

            instCount = countCustomAttributeInstances(session, session.baseUrl, attrId)

            # add to list (for further processing)
            attrList.append({"id": attrId, "name": attrName})
            # print to console
            print(
                f"Name: {attrName}"
                + f" id={attrId}"
                + f" type={dataType}"
                + f" sortable={sortable}"
                + f" facetable={facetable}"
                + f" usageCount={instCount}"
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
                    instCount,
                ]
            )

    # end of while loop
    return attrCount, custAttrCount, attrList


def countCustomAttributeInstances(session, catalogUrl, attrId):
    """
    find all instances for all objects using custom attribute
    """
    # print(f"counting instances for {attrId} attributes")
    # setup the parms for v1 search (for edc versions where v2 search does not exist)
    querystring = {
        "q": f"{attrId}:*",
        "offset": "0",
        "pageSize": "1",
        "hl": "false",
        "related": "false",
        "rootto": "false",
        "facet.field": ["core.classType"],
    }

    # execute catalog rest call, for a page of results
    resturl = catalogUrl + "/access/1/catalog/data/search"
    try:
        resp = session.get(resturl, params=querystring, timeout=3)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        # exit if we can't connect
        return 0

    # no execption rasied - so we can check the status/return-code
    status = resp.status_code
    if status != 200:
        # some error - e.g. catalog not running, or bad credentials
        print("error! " + str(status) + str(resp.json()))
        return 0

    resultJson = resp.json()
    totalCount = resultJson["totalCount"]

    return totalCount


if __name__ == "__main__":
    # call main - if not already called or used by another script
    main()
