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

edcHelper = edcSessionHelper.EDCSession()

# define script command-line parameters (in global scope for gooey/wooey)
parser = argparse.ArgumentParser(parents=[edcHelper.argparser])

# check for args overriding the env vars
# parser = argparse.ArgumentParser()
# add args specific to this utility (resourceName, resourceType, outDir, force)
parser.add_argument(
    "-rn", "--resourceName", required=True, help="resource name for xdoc download"
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
    # setup edc session and catalog url - with auth in the session header,
    # by using system vars or command-line args
    edcHelper.initUrlAndSessionFromEDCSettings()

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
    else:
        resourceType = args.resourceType

    if args.outDir is not None:
        outFolder = args.outDir
        print(f"new out folder={outFolder}")

    if not os.path.exists(outFolder):
        os.makedirs(outFolder)

    xdocFile = f"{outFolder}/{resourceName}__{resourceType}__xdocs.json"
    if os.path.isfile(xdocFile) and not args.force:
        print(f"file: {xdocFile} already exists, no need to refreshe from catalog")
        processXdocDownloadFile(xdocFile, resourceName, outFolder)
    else:
        print(f"running query to get exdocs for {resourceName} + type={resourceType}")

        try:
            xdocurl = f"{edcHelper.baseUrl}/access/1/catalog/data/downloadXdocs"
            parms = {}
            parms["resourceName"] = resourceName
            parms["providerId"] = resourceType
            print(
                f"executing xdoc download via endpoint: {xdocurl} with params={parms}"
            )

            resp = edcHelper.session.get(xdocurl, params=parms, timeout=10)
            if resp.status_code == 200:
                with open(xdocFile, "wb") as outfile:
                    outfile.write(resp.content)
                print(f"xdoc size={len(resp.text)}")
                processXdocDownloadFile(xdocFile, resourceName, outFolder)
            else:
                print(
                    f"api call returned non 200 status_code: {resp.status_code} "
                    f" {resp.text}"
                )

        except requests.exceptions.RequestException as e:
            print("Exception raised when executing edc query: " + xdocurl)
            print(e)
            # exit if we can't connect
            sys.exit(1)


def processXdocDownloadFile(fileName: str, resourceName: str, outFolder: str):
    """
    read a .json file that was saved/downloaded with all xdocs.
    these are jsonl files (1 line per json) & comes from
    the 1/catalog/data/downloadXdocs endpoint

    after reading - collect information about what objects are in there,
    the counts of links, and especially collect details of connection objects
    it will help understand connection assignments
    """
    fConnLinks = open(
        f"{outFolder}/{resourceName}_xdoc_connection_links.csv", "w", newline=""
    )
    connectionlinkWriter = csv.writer(fConnLinks)
    connectionlinkWriter.writerow(
        [
            "association",
            "fromObjectIdentity",
            "toObjectIdentity",
            "linkType",
            "ObjectID",
            "ObjectHref",
        ]
    )

    allLinks = open(f"{outFolder}/{resourceName}_xdoc_links.csv", "w", newline="")
    alllinkWriter = csv.writer(allLinks)
    alllinkWriter.writerow(
        [
            "association",
            "fromObjectIdentity",
            "toObjectIdentity",
            "fromObjectConnectionName",
            "toObjectConnectionName",
            "properties",
        ]
    )

    # linkList = []

    assocCounts = dict()
    objCounts = dict()
    leftRightConnectionObjects = dict()
    connectables = set()
    connectionStats = dict()
    connectionInstances = dict()

    lineCount = 0
    data = []
    totalLinks = 0
    totalObjects = 0
    leftExternalCons = 0
    rightExternalCons = 0
    allproperties = 0
    notnull_properties = 0
    propDict = dict()
    refObjects = 0

    with open(fileName) as f:
        for line in f:
            lineCount += 1
            data = json.loads(line)
            print(f"json line length: {len(line)}", end="")
            linkCount = 0
            for links in data["links"]:
                linkCount += 1
                fromId = links.get("fromObjectIdentity")
                toId = links.get("toObjectIdentity")
                fromConn = links.get("fromObjectConnectionName")
                toConn = links.get("toObjectConnectionName")
                assoc = links.get("association")
                props = links.get("properties")

                if fromId.startswith("${"):
                    fromConn = fromId[: fromId.find("}") + 1]
                    connectables.add(fromId)
                    leftExternalCons += 1
                    if assoc in leftRightConnectionObjects.keys():
                        leftRightConnectionObjects[assoc] = (
                            leftRightConnectionObjects[assoc] + 1
                        )
                    else:
                        leftRightConnectionObjects[assoc] = 1

                    #  connection counts
                    cStats = connectionStats.get(fromConn, {})
                    connLinkCount = cStats.get(assoc, 0)
                    connLinkCount += 1
                    cStats[assoc] = connLinkCount
                    connectionStats[fromConn] = cStats

                    # connection instances
                    cStats2 = connectionInstances.get(fromConn, {})
                    connLinks = set()
                    connLinks = cStats2.get(assoc, set())
                    connLinks.add(fromId)
                    cStats2[assoc] = connLinks
                    connectionInstances[fromConn] = cStats2

                    # connectionlinkWriter.writerow(
                    #     [assoc, fromId, toId, fromConn, toConn, props]
                    # )
                    # end of if it is a to connection

                if toId.startswith("${"):
                    toConn = toId[: toId.find("}") + 1]
                    rightExternalCons += 1
                    connectables.add(toId)
                    if assoc in leftRightConnectionObjects.keys():
                        leftRightConnectionObjects[assoc] = (
                            leftRightConnectionObjects[assoc] + 1
                        )
                    else:
                        leftRightConnectionObjects[assoc] = 1

                        #  connection counts
                    cStats = connectionStats.get(toConn, {})
                    connLinkCount = cStats.get(assoc, 0)
                    connLinkCount += 1
                    cStats[assoc] = connLinkCount
                    connectionStats[toConn] = cStats

                    # connection instances
                    cStats2 = connectionInstances.get(toConn, {})
                    connLinks = set()
                    connLinks = cStats2.get(assoc, set())
                    connLinks.add(toId)
                    cStats2[assoc] = connLinks
                    connectionInstances[toConn] = cStats2

                    # connectionlinkWriter.writerow(
                    #     [assoc, fromId, toId, fromConn, toConn, props]
                    # )

                alllinkWriter.writerow([assoc, fromId, toId, fromConn, toConn, props])
                if fromConn != "" and toConn != "":
                    connectionlinkWriter.writerow(
                        [assoc, fromId, toId, fromConn, toConn, props]
                    )

                if assoc in assocCounts.keys():
                    assocCounts[assoc] = assocCounts[assoc] + 1
                else:
                    assocCounts[assoc] = 1

            # objects count
            objects = 0
            for obj in data["objects"]:
                objects += 1
                objClass = obj.get("objectClass")
                if objClass in objCounts.keys():
                    objCounts[objClass] = objCounts[objClass] + 1
                else:
                    objCounts[objClass] = 1

            # property counts
            for prop in data["properties"]:
                allproperties += 1
                attrName = prop["attributeName"]
                attrVal = prop["value"]
                if attrVal != "":
                    notnull_properties += 1
                # increment the property counter, initialize to 1 for new properties
                propDict[attrName] = propDict.get(attrName, 0) + 1

            # property counts
            for prop in data["referenceObjects"]:
                refObjects += 1

            totalLinks += linkCount
            totalObjects += objects
            print("\tobjects=: " + str(objects) + " links: " + str(linkCount))

    print("total json lines: " + str(lineCount))
    print("total objects: " + str(totalObjects))
    print("total links: " + str(totalLinks))
    print(f"total properties: {allproperties}")
    print(f"total properties with content: {notnull_properties}")
    print("total connections: " + str(len(connectionInstances)))
    print("total upstream linkable objects:" + str(leftExternalCons))
    print("total downstream linkable objects:" + str(rightExternalCons))
    print(f"reference objects: {refObjects}")
    pp = pprint.PrettyPrinter(depth=6)

    print("\nconnection assignment link counts:")
    pp.pprint(leftRightConnectionObjects)

    print("\nconnection stats")
    pp.pprint(connectionStats)

    jsonFile = outFolder + "/" + resourceName + "_connectionInstances.json"
    with open(jsonFile, "w") as json_file:
        json.dump(connectionInstances, json_file, indent=4, cls=JsonSetEncoder)

    print("\nobjects:")
    pp.pprint(objCounts)

    print("\nassociations:")
    pp.pprint(assocCounts)

    print(f"\nproperties {len(propDict)} unique - {notnull_properties} values")
    pp.pprint(propDict)

    allLinks.close()
    # fLinks.close()
    fConnLinks.close()

    # print("\nconnectable id's")
    # pp.pprint(connectables)
    with open(outFolder + "/" + resourceName + "_connectables.txt", "w") as out_file:
        for value in sorted(connectables):
            out_file.write(f"{value}\n")

    print("##########")
    print(f"reference objects: {refObjects}")


class JsonSetEncoder(json.JSONEncoder):
    """
    convert set to list for dumping json to file
    """
    def default(self, o):
        if isinstance(o, set):
            return list(o)
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

    return resourceType


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
