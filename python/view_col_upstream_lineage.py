"""
Note:  this script is not fully tested.  it is a quick example of how you could do the following:-
    - starting from a view name (passed as an input parameter)
    - find the view (search using objects endpoint)
    - get all columns in the view (assemble the id's)
    - execute relationships api endpoint - for all view columns - for upstream core.DirectionalDataFlow linked objects (depth=1)
    - dump the links to .json and .csv for analysis
"""
import requests
import json
from requests.auth import HTTPBasicAuth
import csv
import platform
import edcutils
import time
import sys
import argparse
import edcSessionHelper
import urllib3
import os
from pathlib import Path
import dbSchemaReplicationLineage

urllib3.disable_warnings()

# set edc helper session + variables (easy/re-useable connection to edc api)
edcHelper = edcSessionHelper.EDCSession()

# define script command-line parameters (in global scope for gooey/wooey)
parser = argparse.ArgumentParser(parents=[edcHelper.argparser])

# add args specific to this utility (left/right resource, schema, classtype...)
parser.add_argument(
    "-f",
    "--csvFileName",
    default="viewcol_linage.csv",
    required=False,
    help=(
        "csv file to create/write (no folder) default=dbms_externalDBLinks.csv "
    ),
)
parser.add_argument(
    "-o",
    "--outDir",
    default="out",
    required=False,
    help=(
        "output folder to write results - default = ./out "
        " - will create folder if it does not exist"
    ),
)


parser.add_argument(
    "-n",
    "--viewname",
    default="",
    required=True,
    help=(
        "custom lineage resource name to create/update - default value=externalDBlinker_lineage"
    ),
)
parser.add_argument(
    "-rr",
    "--resourcename",
    default="",
    required=False,
    help=(
        "resource name to find the view, not currently active/working"
    ),
)


def main():
    """
    main starts here - run the query processing all items

    """
    print("ExternalDBLinker started")
    start_time = time.time()

    args = args, unknown = parser.parse_known_args()
    # setup edc session and catalog url - with auth in the session header,
    # by using system vars or command-line args
    edcHelper.initUrlAndSessionFromEDCSettings()
    print(f"command-line args parsed = {args} ")

    tableLinksCreated = 0
    columnLinksCreated = 0
    errorsFound = 0

    columnHeader = [
        "Association",
        "From Connection",
        "To Connection",
        "From Object",
        "To Object",
    ]
    outputFile = args.outDir + "/" + args.csvFileName
    fullpath = os.path.abspath(outputFile)
    fCSVFile = open(outputFile, "w", newline="", encoding="utf-8")
    from pathlib import Path

    print("custom lineage file initialized. " + outputFile + " RELATIVE=" +fullpath)
    colWriter = csv.writer(fCSVFile)
    colWriter.writerow(columnHeader)

    query = f"+core.classType:com.infa.ldm.relational.View +core.name:\"{args.viewname}\""

    parameters = {
        "q": query,
        "offset": 0,
        "pageSize": 1000,
    }
    url = edcHelper.baseUrl + "/access/2/catalog/data/objects"

    print(
        f"executing query to find view named {args.viewname}: "
        f"{url} q={parameters.get('q')} {parameters}"
    )
    resp = edcHelper.session.get(url, params=parameters)
    status = resp.status_code
    print("extDB query rc=" + str(status))

    if status != 200:
        print(f"error - expecting 200 rc, got {status} - message={resp.json()}")
        return

    resultJson = resp.json()
    total = resultJson["metadata"]["totalCount"]
    print(f"external db objects found... {total}")

    id_list = list()

    # for each externalDatabase object
    for view in resultJson["items"]:
        itemId = view["id"]
        # print(f"\tview={itemId}")
        for dstlink in view['dstLinks']:
            print(f"\t{dstlink}")
            print(f"{dstlink.get('id')}")
            if dstlink['classType'] == 'com.infa.ldm.relational.ViewColumn':
                id_list.append(dstlink['id'])

    print(f"view columns found - used for lineage seed ids...{len(id_list)}")

    # get the lineage for the database object
    lineageURL = edcHelper.baseUrl + "/access/2/catalog/data/relationships"
    lineageParms = {
        "seed": id_list,
        "association": "core.DirectionalDataFlow",
        "depth": "1",
        "direction": "IN",
        "includeAttribute": {"core.name", "core.classType"},
        "includeTerms": "false",
        "removeDuplicateAggregateLinks": "false",
    }
    print(f"\tLineage query for: {args.viewname} params={lineageParms}")
    lineageResp = edcHelper.session.get(
        lineageURL, params=lineageParms,
    )

    lineageStatus = lineageResp.status_code
    print(f"\tlineage rc={lineageStatus}")

    # print(f"\n\n--------")
    # print(lineageResp.json())

    columnHeader = [
        "left_id",
        "left_name",
        "left_type",
        "right_id",
        "right_name",
        "right_type",
    ]
    outputFile = args.outDir + "/" + args.csvFileName
    fullpath = os.path.abspath(outputFile)
    fCSVFile = open(outputFile, "w", newline="", encoding="utf-8")
    print("csv file initialized. " + outputFile)
    colWriter = csv.writer(fCSVFile)
    colWriter.writerow(columnHeader)



    # dump the lineage result to file (for documentation/understanding)
    jsonFile = args.outDir + "/view_column_lineage.json"
    with open(jsonFile, "w") as json_file:
        json.dump(lineageResp.json(), json_file, indent=4,)

    # iterate over the lineage results and write to a csv file
    lineage_json = lineageResp.json()
    for lineageitem in lineage_json["items"]:
        outid = lineageitem["outId"]
        inid = lineageitem["inId"]
        outname = edcutils.getFactValue(lineageitem["outEmbedded"], "core.name")
        outclass = edcutils.getFactValue(lineageitem["outEmbedded"], "core.classType")
        inname = edcutils.getFactValue(lineageitem["inEmbedded"], "core.name")
        inclass = edcutils.getFactValue(lineageitem["inEmbedded"], "core.classType")
        print(f"a link to process - {outid} {outname} {outclass} -> {inid} {inname} {inclass}")
        colWriter.writerow([outid, outname, outclass, inid, inname, inclass])

    fCSVFile.close()






# call main - if not already called or used by another script
if __name__ == "__main__":
    main()