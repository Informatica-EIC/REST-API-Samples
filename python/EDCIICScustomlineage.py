# -----------------------------------------------------------------------
# Informatica Inc.
# Build Custom Lineage for EDC Resource type IICS-CDI
# -----------------------------------------------------------------------
"""
Created on April 13, 2023
@author: tcoleman
usage:
   EDCIICScustomlineage.py -h to see command-line options
  output written to links.csv
Note:  requires python 3 (3.6+)
Purpose of this code is to create a custom lineage links.csv that contains both
table/file level and table/file column level lineage based on IICS-CDI resource
scan.  Then update the custom lineage resource with the new output csv file.

Tested with:  EDC v10.5.x
"""
import requests
from requests.auth import HTTPBasicAuth
import time
import sys
import os
import urllib3
import csv
import argparse
from edcSessionHelper import EDCSession
from pathlib import PurePath
import json
import datetime
import shutil

# global var declaration (with type hinting)
edcSession: EDCSession = None

fromtolist = {}

urllib3.disable_warnings()


start_time = time.time()
# initialize http header - as a dict
header = {}
auth = None

# number of objects for each page/chunk
pageSize = 500
tblcount = 0
colcount = 0
# the csv lineage file to write to = can be overwritten by -f cmdline parameter
csvFileName = "links.csv"
# the path to write to - can be overwritten by -o cmdline parameter
csvFilePath = "out/"

edcSession = EDCSession()
parser = argparse.ArgumentParser(parents=[edcSession.argparser])
# add additional command line parameter options
parser.add_argument("-o", "--output", default="out/",
                    help="output folder - e.g. .out")
parser.add_argument("-f", "--filename", default="links.csv",
                    help="Output Lineage CSV File Name")
parser.add_argument("-r", "--resname", default="None",
                    help="EDC Resource Name")
parser.add_argument("-d", "--debug", default="false",
                    help="debug true write json results")
parser.add_argument("-l", "--clresname", default="None",
                    help="EDC Custom Lineage Resource Name")

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
#  INFA_EDC_URL = url for edc http[s]://<server>:<port>
#  INFA_EDC_AUTH=<auth string>
# or
#  Execute setupConnection.py to store EDC Host URL and userid password encrypted
#  in .env file stored in the execution folder
# ******************************************************


def main():
    """    
    Program Flow
    call GET /access/2/catalog/data/objects/  return all table/file objects
    call GET /access/2/catalog/data/relationships/ return from/to table lineage
    call GET /access/2/catalog/data/objects/   return table/file column objects
    call GET /access/2/catalog/data/relationships/ return from/to column lineage
    output - write links.csv for custom lineage
    call GET /access/1/catalog/resources/clresourcename/files upload output csv file
    """
    global headers
    global outputFile
    global user
    global password
    global colcount
    global tblcount
    global flogfile
    global tablejsonfile
    global columnjsonfile
    global mapcount
    global fromtolist
    global debug

    mapcount = 0
    now = datetime.datetime.now()
    p = PurePath(sys.argv[0])
    print(f"{p.name} starting in {os.getcwd()} args={sys.argv[1:]}")

    args, unknown = parser.parse_known_args()
    # print("****resname:",args.resname)
    debug = 'false'
    if args.debug == "true":
        debug = "true"

    if args.resname == "None":
        print(
            "No Resource Name "
            "prompting for Resource Name"
        )
        args.resname = input("\tResource Name:")

    if args.clresname == "None":
        print(
            "No Custom Lineage Resource Name "
            "prompting for Resource Name"
        )
        args.clresname = input("\tCustom Lineage Resource Name:")

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
    print("")
    if args.output is not None:
        csvFilePath = args.output
        if csvFilePath != "":
            if os.path.exists(csvFilePath):
                shutil.rmtree(csvFilePath)
            if not os.path.exists(csvFilePath):
                os.makedirs(csvFilePath)
    if args.filename is not None:
        csvFileName = args.filename

    outputFile = csvFilePath + csvFileName
    logFile = outputFile + "_Processing.log"
    tablejsonfile = outputFile + "_table_result.json"
    columnjsonfile = outputFile + "_column_result.json"

    # set resource name
    if args.resname is not None:
        resourcename = args.resname
    # set custom lineage resource name
    if args.clresname is not None:
        clresourcename = args.clresname

    # -----------------------------------------------------------------------
    # create and initialize Processing Log File
    # -----------------------------------------------------------------------

    print(f"\tEDC Version {edcSession.edcversion}")

    flogfile = open(logFile, "w", newline="", encoding="utf-8")
    # Print to screen and log to processing file startup details

    print("Processing Log File initialized: " + logFile + "\n")

    logprocess = "\nExecuted on:" + str(now) + "\n"
    print(logprocess)
    flogfile.write(logprocess)

    EDCVersion = {edcSession.edcversion}
    logprocess = "EDC Version:  {} \n".format(EDCVersion)
    flogfile.write(logprocess)

    # -----------------------------------------------------------------------
    # create and initialize the header for the output links.csv file
    # -----------------------------------------------------------------------
    fileheader = ['Association', 'From Connection', 'To Connection', 'From Object',
                  'To Object']
    fCSVFile = open(outputFile, "w", newline="", encoding="utf-8")
    print("\nCustom lineage csv file initialized: " + outputFile)
    colWriter = csv.writer(fCSVFile)
    colWriter.writerow(fileheader)
    fCSVFile.close()

    # Process Table/File Objects in resource
    # com.infa.ldm.etl.DetailedDataSetMappingSetDataFlow
    print("\nProcessing Target (dstLink) Tables in resource " + resourcename)
    logprocess = "\nProcessing Target (dstLink) Tables in resource " + \
        resourcename
    flogfile.write(logprocess)
    ptype = "Table"  # Processing Tables
    tblcount = 0
    baseurl = edcSession.baseUrl + "/access/2/catalog/data/objects?associations=com.infa.ldm.etl.DetailedDataSetMappingSetDataFlow&includeDstLinks=true&includeRefObjects=false&includeSrcLinks=true&offset=0&pageSize=20&q=core.classType%3Acom.infa.ldm.AdvancedScanners.IICS.V2.MappingTaskInstance&fq=core.resourceName:"
    getObjects(
        edcSession.session, baseurl + resourcename, colWriter, ptype
    )

    # Process Column Level Objects in resource
    # com.infa.ldm.etl.DetailedDataSetDataElementDataFlow
    # Reset mapcount to zero to only count once at the column level
    mapcount = 0
    print("\n\nProcessing Target Columns (dstLink) in resource " + resourcename)
    logprocess = "\n\nProcessing Target Columns (dstLink) in resource " + \
        resourcename
    flogfile.write(logprocess)
    ptype = "Column"  # Processing Table Columns
    colcount = 0
    baseurl = edcSession.baseUrl + "/access/2/catalog/data/objects?associations=com.infa.ldm.etl.DetailedDataSetDataElementDataFlow&includeDstLinks=true&includeRefObjects=false&includeSrcLinks=true&offset=0&pageSize=20&q=core.classType%3Acom.infa.ldm.AdvancedScanners.IICS.V2.MappingTaskInstance&fq=core.resourceName:"
    getObjects(
        edcSession.session, baseurl + resourcename, colWriter, ptype
    )

    # --------------------------------------
    # Upload New csv file to resource
    # -------------------------------------
    scannerId = 'LineageScanner'
    baseurl2 = edcSession.baseUrl
    returnCode = uploadResourceFileUsingSession(
        baseurl2, edcSession.session,  clresourcename, csvFileName, csvFilePath, scannerId)
    # --------------------------------------
    # Processing Report - Totals
    # --------------------------------------
    print("")
    print("Mapping Task Count: {}".format(mapcount))
    print("Target Table (dstLink) Count:", tblcount)
    print("Target Column (dstLink) Count:", colcount)
    totalTime = "Finished - run time = %s seconds ---" % (
        time.time() - start_time)

    print(totalTime)
    logprocess = "\n\nMapping Task Count: " + str(mapcount)
    flogfile.write(logprocess)
    logprocess = "\nTarget Table (dstLink) Count: " + str(tblcount)
    flogfile.write(logprocess)
    logprocess = "\nTarget Column (dstLink) Count: " + str(colcount)
    flogfile.write(logprocess)
    logprocess = "\n" + totalTime
    flogfile.write(logprocess)
    flogfile.close()
    return

# -----------------------------------------------------------------------
# Get all tables/files ID's from dstlink in resource
#  or
# Get Column Level based on baseurl
# Table:  com.infa.ldm.etl.DetailedDataSetMappingSetDataFlow
# Column: com.infa.ldm.etl.DetailedDataSetDataElementDataFlow
# -----------------------------------------------------------------------


def getObjects(session, resturl, colWriter, ptype):
    """
    get a list of Object Id's then call getRelationship to write
    from/to relationship to csv file colWriter

    """
    global headers
    global outputFile
    global user
    global password
    global tblCount
    global colCount
    global flogfile
    global tablejsonfile
    global columnjsonfile
    global mapcount
    global debug

    # print("getObjects")
    # print("resturl", resturl)
    # print("")

    parms = {}

    # execute catalog rest call, Get Object
    try:
        resp = session.get(resturl, params=parms, timeout=3)
    except requests.exceptions.RequestException as e:
        print("")
        print("Error connecting to : " + resturl)
        print(e)
        print("")        # exit if we can't connect
        sys.exit(1)

        # no execption rasied - so we can check the status/return-code
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            sys.exit(1)

    resultJson = resp.json()
    # Write out json response when debug is turned on
    if debug == "true":
        if ptype == 'Table':
            ftablejsonfile = open(tablejsonfile, "w",
                                  newline="", encoding="utf-8")
            tableresult = json.dumps(resultJson, indent=4)
            ftablejsonfile.write(tableresult)
            ftablejsonfile.close()
        else:
            fcolumnjsonfile = open(columnjsonfile, "w",
                                   newline="", encoding="utf-8")
            columnresult = json.dumps(resultJson, indent=4)
            fcolumnjsonfile.write(columnresult)
            fcolumnjsonfile.close()

    # for each object found look to see if it contains dstlinks object
    objstatus = 0
    try:
        for i in resultJson["items"]:
            mapid = i["id"]
            print("\n      MapID: {}".format(mapid))
            mapcount += 1
            logprocess = "\n      MapID: " + mapid
            flogfile.write(logprocess)
            if len(i["facts"]) > 0:
                for f in i["facts"]:
                    if f["attributeId"] == "core.name":
                        mapname = f["value"]
                        print("\n      MapName:  {}".format(mapname))
                        logprocess = "\n      Map Name: " + mapname
                        flogfile.write(logprocess)
                        break
                    if len(i["dstLinks"]) > 0:
                        for j in i["dstLinks"]:
                            # tblcount += 1
                            # Get From/To relationship for object id
                            baseurl = edcSession.baseUrl + "/access/2/catalog/data/relationships?association=core.DataFlow&depth=0&direction=BOTH&includeRefObjects=false&includeTerms=false&removeDuplicateAggregateLinks=true&seed="
                            objid = j["id"]
                            # print("")
                            # print("Target Table Name:   {}".format(objid))
                            # print("")
                            # logprocess = "\n\nTarget Table Name " + objid
                            # flogfile.write(logprocess)
                            processrelation(
                                session, baseurl + objid, colWriter, flogfile, ptype
                            )
    except:
        pretty = json.dumps(resultJson, indent=4)
        print(pretty)
        print("Verify the URL does not end with /")
        sys.exit(2)

    return


# ******************************************************
# Process Relationship  Get and write from/to lineage
# ******************************************************
def processrelation(session, resturl, colWriter, logWriter, ptype):
    global headers
    global outputFile
    global user
    global password
    global tblcount
    global colcount
    global flogfile
    global fromtolist
    global debug

    # -----------------------------------------------------------------------
    # Build Relation API calls
    # -----------------------------------------------------------------------

    parms = {}
    # print("resturl:", resturl)
    # execute catalog rest call, Get Object
    try:
        resp = session.get(resturl, params=parms, timeout=3)
    except requests.exceptions.RequestException as e:
        print("")
        print("Error connecting to : " + resturl)
        print(e)
        # exit if we can't connect
        sys.exit(1)

    # no execption rasied - so we can check the status/return-code
    status = resp.status_code
    if status != 200:
        # some error - e.g. catalog not running, or bad credentials
        print("")
        print("error! " + str(status) + str(resp.json()))
        # since we are in a loop to get pages of objects - break will exit
        # break
        # instead of break - exit this script
        sys.exit(1)

    resultJson_relation = resp.json()
    if resultJson_relation and 'items' in resultJson_relation:
        # print("*****************************************")
        # print("resultJson_relation", resultJson_relation)
        # print("*****************************************")
        with open(outputFile, 'a', encoding='UTF8', newline='') as fCSVFile:
            # create the csv writer
            writer = csv.writer(fCSVFile)
            for items in resultJson_relation['items']:
                Association = items.get('associationId')
                FromConnection = ''
                ToConnection = ''
                FromObject = items.get('outId')
                ToObject = items.get('inId')
                data = [Association, FromConnection,
                        ToConnection, FromObject, ToObject]
                key = FromObject + ToObject
                fnd = key in fromtolist
                if fnd:
                    # Lineage has already been written to csv file
                    if debug == 'true':
                        logprocess = "\nAlready processed Target" + ToObject
                        flogfile.write(logprocess)
                else:
                    # Add From/To Key to avoid duplicates
                    fromtolist[key] = key
                    if FromObject == ToObject:
                        if debug == 'true':
                            logprocess = "\n\n          Skip Relationship Same: " + FromObject + "\n"
                            flogfile.write(logprocess)
                    else:
                        if ptype == 'Table':
                            tblcount += 1
                        else:
                            colcount += 1

                        if debug == 'true':
                            logprocess = "\n          " + FromObject
                            flogfile.write(logprocess)
                            logprocess = "\n                     " + ToObject
                            flogfile.write(logprocess)
                        # Write lineage to csv file
                        writer.writerow(data)

            fCSVFile.close()
    return

# -----------------------------------------------------------------------
# Upload and replace custom lineage input file with new file
# -----------------------------------------------------------------------


def uploadResourceFileUsingSession(
    url, session, clresourcename, fileName, fullPath, scannerId
):
    """
    upload a file for the resource - e.g. a custom lineage csv file
    works with either csv for zip files  (.csv|.zip)
    returns rc=200 (valid) & other rc's from the post
    """
    logprocess = "\nuploading file:" + fileName
    print(logprocess)
    flogfile.write(logprocess)
    logprocess = "Custom Lineage Resource Name: " + clresourcename
    print(logprocess)
    flogfile.write(logprocess)
    apiURL = url + "/access/1/catalog/resources/" + clresourcename + "/files"

    # header = {"accept": "*/*", }
    params = {"scannerid": scannerId, "filename": fileName, "optionid": "File"}

    #     files = {'file': fullPath}
    mimeType = "text/csv"
    readMode = "rt"
    if fileName.endswith(".zip"):
        mimeType = "application/zip"
        readMode = "rb"

    if fileName.endswith(".dsx"):
        mimeType = "text/plain"

    file = {"file": (fileName, open(fullPath+fileName, readMode), mimeType)}

    if debug == 'true':
        logprocess = "\turl=" + apiURL
        print(logprocess)
        flogfile.write(logprocess)
        logprocess = "\t" + str(params)
        print(logprocess)
        flogfile.write(logprocess)
        logprocess = f"\t{file}"
        print(logprocess)
        flogfile.write(logprocess)
        logprocess = "apiURL: " + apiURL
        print(logprocess)
        flogfile.write(logprocess)
        logprocess = "params: " + str(params)
        print(logprocess)
        flogfile.write(logprocess)
        logprocess = "file: " + str(file)
        print(logprocess)
        flogfile.write(logprocess)

    uploadResp = session.post(
        apiURL,
        data=params,
        files=file,
    )
    if debug == 'true':
        logprocess = '\tresponse=' + str(uploadResp.status_code)
        print(logprocess)
        flogfile.write(logprocess)

    if uploadResp.status_code == 200:
        # valid - return the json
        return uploadResp.status_code
    else:
        # not valid
        print("\tupload file failed")
        print("\t" + str(uploadResp))
        print("\t" + str(uploadResp.text))
        return uploadResp.status_code


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
