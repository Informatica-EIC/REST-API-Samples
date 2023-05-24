"""
Created on Oct 31, 2018

download (or read previously stored file) and analyze xdocs for a resource,
including connection assignment data

usage:  xdocAnalyzer  <parms>

@author: dwrigley
"""

import json
import csv
import pprint
import os
import requests
import sys
import argparse
import edcSessionHelper
from zipfile import ZipFile
import setupConnection

edcHelper = edcSessionHelper.EDCSession()

# define script command-line parameters (in global scope for gooey/wooey)
parser = argparse.ArgumentParser(parents=[edcHelper.argparser])

# check for args overriding the env vars
# parser = argparse.ArgumentParser()
# add args specific to this utility (resourceName, resourceType, outDir, force)
parser.add_argument(
    "-rn",
    "--resourceName",
    required=("--setup" not in sys.argv),
    help="resource name for xdoc download",
)
parser.add_argument(
    "-rt",
    "--resourceType",
    required=False,
    help="resource type (provider id) for xdoc download",
)
parser.add_argument(
    "-o",
    "--outDir",
    required=False,
    help=(
        "output folder to write results - default = ./out "
        " - will create folder if it does not exist"
    ),
)

parser.add_argument(
    "-f",
    "--force",
    action="store_true",
    required=False,
    help=(
        "force overwrite of xdoc json file (if already existing), "
        "otherwise just use the .json file"
    ),
)

parser.add_argument(
    "--setup",
    required=False,
    action="store_true",
    help=(
        "setup the connection to EDC by creating a .env file"
        " - same as running python3 setupConnection.py"
    ),
)


class mem:
    assocCounts = {}
    objCounts = {}
    leftRightConnectionObjects = {}
    connectables = set()
    connectionStats = {}
    connectionInstances = {}

    lineCount = 0
    data = []
    totalLinks = 0
    totalObjects = 0
    leftExternalCons = 0
    rightExternalCons = 0
    allproperties = 0
    notnull_properties = 0
    propDict = {}
    refObjects = 0


def main():
    """
    main function - determines if edc needs to be queried to download xdocs
    (if file is not there, or if it is and force=True)
    downloads the xdocs then calls the function to read the xdocs and print a summary
    """

    resourceName = ""
    resourceType = ""
    outFolder = "./out"

    args = args, unknown = parser.parse_known_args()
    if args.setup:
        # if setup is requested (running standalone)
        # call setupConnection to create a .env file to use for subsequent runs
        print("setup requested..., calling setupConnection & exiting")
        setupConnection.main()
        return
    # setup edc session and catalog url - with auth in the session header,
    # by using system vars or command-line args
    edcHelper.initUrlAndSessionFromEDCSettings()
    edcHelper.validateConnection()
    print(f"EDC version: {edcHelper.edcversion_str} ## {edcHelper.edcversion}")

    print(f"command-line args parsed = {args} ")
    print()

    # print(type(args))
    if args.resourceName is not None:
        resourceName = args.resourceName
    else:
        print(
            "no resourceName specified - we can't download xdocs without knowing "
            "what resource name to use. exiting"
        )
        return

    if args.resourceType is None:
        print("we have a resource name - but no type - need to look it up")
        resourceType = getResourceType(resourceName, edcHelper.session)
        if resourceType is None:
            print("cannot get resourcetype, exiting")
            return
    else:
        resourceType = args.resourceType

    if args.outDir is not None:
        outFolder = args.outDir
        print(f"new out folder={outFolder}")

    if not os.path.exists(outFolder):
        os.makedirs(outFolder)

    print(f"force={args.force}")

    if edcHelper.edcversion >= 105000:
        print("EDC 10.5+ found... calling 10.5 exdoc analyzer")
        get_exdocs_zip(resourceName, resourceType, outFolder, args.force)
    else:
        # 10.4.x and before - xdocs are returned as jsonl
        get_exdocs_json(resourceName, resourceType, outFolder, args.force)

    # write_xdoc_results(resourceName, outFolder)


class JsonSetEncoder(json.JSONEncoder):
    """
    convert set to list for dumping json to file (sort the set too)
    """

    def default(self, o):
        if isinstance(o, set):
            return list(sorted(o))
        return super(JsonSetEncoder, self).default(o)


def getResourceType(resourceName, session) -> str:
    """
    get the resource json, and extract the scannerId (provider id)
    """
    resourceType = None
    try:
        resturl = session.baseUrl + "/access/1/catalog/resources/" + resourceName
        resp = session.get(resturl, timeout=3)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        return None

    if resp.status_code == 200:
        resJson = resp.json()
        resourceType = resJson["scannerConfigurations"][0]["scanner"]["scannerId"]
    else:
        print(f"\terror getting resourceType for {resourceName}. {resp.text}")

    return resourceType


def get_exdocs_zip(
    resource_name: str, resource_type: str, out_folder: str, force_extract: bool
):
    """
    get the zipped xdocs from EDC (or use from cached file)
    """
    print(f"xdoc analyzer 10.5+ version starting extract for resource={resource_name}")
    xdocurl = f"{edcHelper.baseUrl}/access/1/catalog/data/downloadXdocs"
    parms = {"resourceName": resource_name, "providerId": resource_type}
    print(f"executing xdoc download via endpoint: {xdocurl} with params={parms}")

    xdoc_filename = out_folder + "/" + resource_name + "__" + resource_type + ".zip"

    # only get xdocs .zip file if either new, or forced (use cache otherwise)
    if (not os.path.isfile(xdoc_filename)) or (
        os.path.isfile(xdoc_filename) and force_extract
    ):
        print("generate new xdocs...")
        try:
            resp = edcHelper.session.get(xdocurl, params=parms, timeout=20)
            if resp.status_code == 200:
                with open(xdoc_filename, "wb") as outfile:
                    outfile.write(resp.content)
                    print(f"xdoc size={len(resp.text)}")
                    print(f"xdoc files written to {xdoc_filename}")
                    # processXdocDownloadFile(xdocFile, resourceName, outFolder)
            else:
                print(
                    f"api call returned non 200 status_code: {resp.status_code} "
                    f" {resp.text}"
                )

        except requests.exceptions.RequestException as e:
            print("Exception raised when executing edc query: " + xdocurl)
            print(e)

    init_files(resource_name, out_folder)
    archive = ZipFile(xdoc_filename, "r")
    files = archive.namelist()
    print(f"xdoc files in zip = {len(files)}")
    process_xdocs_zipped(xdoc_filename, resource_name, out_folder)
    close_files()


def get_exdocs_json(
    resource_name: str, resource_type: str, out_folder: str, force_extract: bool
):
    # 10.4.x and before - xdocs are returned as jsonl
    """
    get the pre 10.5 jsonl version of xdocs from EDC (or use from cached file)
    """
    print(
        "xdoc analyzer 10.4.x and earlier - "
        f"starting xdoc extract for resource={resource_name}"
    )
    xdocurl = f"{edcHelper.baseUrl}/access/1/catalog/data/downloadXdocs"
    parms = {"resourceName": resource_name, "providerId": resource_type}
    print(f"executing xdoc download via endpoint: {xdocurl} with params={parms}")

    # xdoc_filename = out_folder + "/" + resource_name + "__" + resource_type + ".zip"
    xdoc_filename = f"{out_folder}/{resource_name}__{resource_type}__xdocs.json"

    # only get xdocs .zip file if either new, or forced (use cache otherwise)
    if (not os.path.isfile(xdoc_filename)) or (
        os.path.isfile(xdoc_filename) and force_extract
    ):
        print("generate new xdocs...")
        try:
            resp = edcHelper.session.get(xdocurl, params=parms, timeout=20)
            if resp.status_code == 200:
                with open(xdoc_filename, "wb") as outfile:
                    outfile.write(resp.content)
                    print(f"xdoc size={len(resp.text)}")
                    print(f"xdoc files written to {xdoc_filename}")
                    # processXdocDownloadFile(xdocFile, resourceName, outFolder)
            else:
                print(
                    f"api call returned non 200 status_code: {resp.status_code} "
                    f" {resp.text}"
                )

        except requests.exceptions.RequestException as e:
            print("Exception raised when executing edc query: " + xdocurl)
            print(e)

    init_files(resource_name, out_folder)
    # archive = ZipFile(xdoc_filename, "r")
    # files = archive.namelist()
    # print(f"xdoc files in zip = {len(files)}")
    process_xdocs_jsonl(xdoc_filename, resource_name, out_folder)
    close_files()


def process_xdocs_zipped(zipfileName: str, resourceName: str, outFolder: str):
    """
    read a .zip file that was saved/downloaded with all xdocs.
    """
    archive = ZipFile(zipfileName, "r")
    files = archive.namelist()
    mem.lineCount = len(files)

    with ZipFile(zipfileName) as zipped_xdocs:
        for xdfile in files:
            with zipped_xdocs.open(xdfile) as xdoc_file:
                print(f"\tprocessing file: {xdfile}")
                data = json.loads(xdoc_file.read())
                # print(data)
                process_xdoc_json(data)

    # write results to file(s)
    write_xdoc_results(resourceName, outFolder)


def process_xdocs_jsonl(fileName: str, resourceName: str, outFolder: str):
    """
    read a .json file that was saved/downloaded with all xdocs.
    these are jsonl files (1 line per json) & comes from
    the 1/catalog/data/downloadXdocs endpoint

    after reading - collect information about what objects are in there,
    the counts of links, and especially collect details of connection objects
    it will help understand connection assignments
    """

    lineCount = 0

    with open(fileName) as f:
        for line in f:
            lineCount += 1
            data = json.loads(line)
            process_xdoc_json(data)

    write_xdoc_results(resourceName, outFolder)


def init_files(resourceName: str, outFolder: str):
    """
    open files for output - to be used for writing individual links
    """
    # create the files and store in mem object for reference later
    mem.fConnLinks = open(
        f"{outFolder}/{resourceName}_xdoc_connection_links.csv",
        "w",
        newline="",
        encoding="utf-8",
    )
    mem.connectionlinkWriter = csv.writer(mem.fConnLinks)
    mem.connectionlinkWriter.writerow(
        [
            # "association",
            # "fromObjectIdentity",
            # "toObjectIdentity",
            # "fromObjectConnectionName",
            # "toObjectConnectionName",
            "Association",
            "From Connection",
            "To Connection",
            "From Object",
            "To Object",
        ]
    )

    mem.allLinks = open(
        f"{outFolder}/{resourceName}_xdoc_links.csv", "w", newline="", encoding="utf-8"
    )
    mem.alllinkWriter = csv.writer(mem.allLinks)
    mem.alllinkWriter.writerow(
        [
            "association",
            "fromObjectIdentity",
            "toObjectIdentity",
            "fromObjectConnectionName",
            "toObjectConnectionName",
            "properties",
        ]
    )


def close_files():
    mem.allLinks.close()
    mem.fConnLinks.close()


def write_xdoc_results(resourceName: str, outFolder: str):
    """
    process has finished - print summary to screen & write some output summaries
    """
    print("totals-------------------------------------------------")
    print("total json files: " + str(mem.lineCount))
    print("total objects: " + str(mem.totalObjects))
    print("total links: " + str(mem.totalLinks))
    print(f"total properties: {mem.allproperties}")
    print(f"total properties with content: {mem.notnull_properties}")
    print("total connections: " + str(len(mem.connectionInstances)))
    print("total upstream linkable objects:" + str(mem.leftExternalCons))
    print("total downstream linkable objects:" + str(mem.rightExternalCons))
    print(f"reference objects: {mem.refObjects}")

    # print("\nconnectable id's")
    # pp.pprint(connectables)
    with open(outFolder + "/" + resourceName + "_connectables.txt", "w") as out_file:
        for value in sorted(mem.connectables):
            try:
                out_file.write(f"{value}\n")
            except Exception as e:
                print("error writing to file")

    pp = pprint.PrettyPrinter(depth=6)
    print("\nconnection assignment link counts:")
    pp.pprint(mem.leftRightConnectionObjects)
    with open(
        outFolder + "/" + resourceName + "_connection_assignment_link_counts.csv", "w"
    ) as out_file:
        for key, val in mem.connectionInstances.items():
            try:
                out_file.write(f"{key},{val}\n")
            except Exception as e:
                print("error writing dict value")
        # json.dump(mem.connectionInstances, json_file, indent=4, cls=JsonSetEncoder)

    print("\nconnection stats")
    pp.pprint(mem.connectionStats)

    jsonFile = outFolder + "/" + resourceName + "_connectionInstances.json"
    with open(jsonFile, "w") as json_file:
        json.dump(mem.connectionInstances, json_file, indent=4, cls=JsonSetEncoder)

    print("\nobjects:")
    pp.pprint(mem.objCounts)
    with open(outFolder + "/" + resourceName + "_object_counts.csv", "w") as out_file:
        for key, val in mem.objCounts.items():
            out_file.write(f"{key},{val}\n")

    print("\nassociations:")
    pp.pprint(mem.assocCounts)
    with open(outFolder + "/" + resourceName + "_assoc_counts.csv", "w") as out_file:
        for key, val in mem.assocCounts.items():
            out_file.write(f"{key},{val}\n")

    print(f"\nproperties {len(mem.propDict)} unique - {mem.notnull_properties} values")
    pp.pprint(mem.propDict)
    with open(outFolder + "/" + resourceName + "_property_counts.csv", "w") as out_file:
        for key, val in mem.propDict.items():
            out_file.write(f"{key},{val}\n")


def process_xdoc_json(data):
    """
    process a single xdoc entry (json doc)
    """
    # print(f"json line length: {len(line)}", end="")
    linkCount = process_xdoc_links(data)

    # objects count - by class type
    objects = 0
    for obj in data["objects"]:
        objects += 1
        objClass = obj.get("objectClass")
        mem.objCounts[objClass] = mem.objCounts.get(objClass, 0) + 1

    # property counts
    for prop in data["properties"]:
        mem.allproperties += 1
        attrName = prop["attributeName"]
        if "value" not in prop:
            print(f"\tmissing value from {attrName}")
            continue
        attrVal = prop["value"]
        # obj_id = prop["objectIdentity"]
        if attrVal != "":
            mem.notnull_properties += 1
            # "com.infa.ldm.etl.pc.Expression"
            # "com.infa.ldm.etl.pc.SqlQuery"
            # if attrName == "com.infa.ldm.etl.pc.Expression":
            #     print(f"PC Expression {attrVal} for id:{obj_id}")

            # if attrName == "com.infa.ldm.spotfire.SQLStatement":
            #     print(
            #         f"code::\n{obj.get('objectIdentity')}\n--com.infa.ldm.spotfire.SQLStatement={attrVal}"
            #     )
            # if attrName == "com.infa.ldm.spotfire.Script":
            #     print(
            #         f"code::\n{obj.get('objectIdentity')}\n--com.infa.ldm.spotfire.Script={attrVal}"
            #     )
            # if attrName == "com.infa.ldm.spotfire.Expression":
            #     print(
            #         f"code::\n{obj.get('objectIdentity')}\n--com.infa.ldm.spotfire.Expression={attrVal}"
            #     )
            # if attrName == "com.infa.ldm.warehouse.sapbwhana.Value":
            #     print(f">>sap value/calc::\n{attrVal}\n<<sap text::for id:{obj_id}")
        # increment the property counter, initialize to 1 for new properties
        mem.propDict[attrName] = mem.propDict.get(attrName, 0) + 1

    # property counts
    for prop in data["referenceObjects"]:
        mem.refObjects += 1

    mem.totalLinks += linkCount
    mem.totalObjects += objects
    print(
        f"\tobjects=: {objects} links: {linkCount} "
        f"properties: {len(data['properties'])}"
    )


def process_xdoc_links(data):
    """
    process the links within a json doc
    """
    linkCount = 0
    for links in data["links"]:
        linkCount += 1
        fromId = links.get("fromObjectIdentity")
        toId = links.get("toObjectIdentity")
        fromConn = links.get("fromObjectConnectionName", "")
        toConn = links.get("toObjectConnectionName", "")
        assoc = links.get("association")
        props = links.get("properties")
        from_ref_id = fromId
        to_ref_id = toId
        if fromId.startswith("${"):
            fromConn = fromId[2 : fromId.rfind("}")]
            from_ref_id = fromId[fromId.rfind("}") + 2 :]
            mem.connectables.add(fromId)
            mem.leftExternalCons += 1
            # thecount = mem.leftRightConnectionObjects.get(assoc, 0)
            mem.leftRightConnectionObjects[assoc] = (
                mem.leftRightConnectionObjects.get(assoc, 0) + 1
            )

            #  connection counts
            cStats = mem.connectionStats.get(fromConn, {})
            connLinkCount = cStats.get(assoc, 0)
            connLinkCount += 1
            cStats[assoc] = connLinkCount
            mem.connectionStats[fromConn] = cStats

            # connection instances
            cStats2 = mem.connectionInstances.get(fromConn, {})
            connLinks = set()
            connLinks = cStats2.get(assoc, set())
            connLinks.add(fromId)
            cStats2[assoc] = connLinks
            mem.connectionInstances[fromConn] = cStats2
            # connectionlinkWriter.writerow(,
            # #     [assoc, fromId, toId, fromConn, toConn, props],
            # # ),
            # end of if it is a to connection

        if toId.startswith("${"):
            toConn = toId[2 : toId.rfind("}")]
            to_ref_id = toId[toId.rfind("}") + 2 :]

            mem.rightExternalCons += 1
            mem.connectables.add(toId)
            mem.leftRightConnectionObjects[assoc] = (
                mem.leftRightConnectionObjects.get(assoc, 0) + 1
            )
            #  connection counts
            cStats = mem.connectionStats.get(toConn, {})
            connLinkCount = cStats.get(assoc, 0)
            connLinkCount += 1
            cStats[assoc] = connLinkCount
            mem.connectionStats[toConn] = cStats
            # connection instances
            cStats2 = mem.connectionInstances.get(toConn, {})
            connLinks = set()
            connLinks = cStats2.get(assoc, set())
            connLinks.add(toId)
            cStats2[assoc] = connLinks  # sorted ???connLinks
            mem.connectionInstances[toConn] = cStats2
            # mem.connectionlinkWriter.writerow(
            #     [assoc, fromId, toId, fromConn, toConn, props]
            # )

        mem.alllinkWriter.writerow([assoc, fromId, toId, fromConn, toConn, props])
        if fromConn != "" or toConn != "":
            mem.connectionlinkWriter.writerow(
                [assoc, fromConn, toConn, from_ref_id, to_ref_id]
            )
        if assoc in mem.assocCounts.keys():
            mem.assocCounts[assoc] = mem.assocCounts[assoc] + 1
        else:
            mem.assocCounts[assoc] = 1
    return linkCount


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
