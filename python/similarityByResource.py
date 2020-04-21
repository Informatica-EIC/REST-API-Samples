"""
Created on Jan 3, 2020

@author: dwrigley

get a list of resources that scan for similarity
for each resource (with similarity)
    get the dataelements that have similarity -> csv file
"""

import requests
import time
import sys
import urllib3
import csv
import edcSessionHelper
import argparse
import os

urllib3.disable_warnings()


class mem:
    """ globals class pattern for shared vars """

    pass


edcHelper = edcSessionHelper.EDCSession()

argparser = argparse.ArgumentParser(
    parents=[edcHelper.argparser],
    description="EDC similarity report/export utility script",
)
argparser.add_argument(
    "-ps",
    "--pageSize",
    required=False,
    help="pageSize for edc queries, default 250",
    default=250,
    type=int,
)
argparser.add_argument(
    "-out",
    "--outFile",
    required=False,
    help=(
        "output file (csv) to create, either relative to current folder or an "
        " absolute reference, default out/similarObjectsByResource.csv"
    ),
    default="out/similarObjectsByResource.csv",
    type=str,
)


def listResources():
    try:
        resturl = edcHelper.baseUrl + "/access/1/catalog/resources"
        resp = edcHelper.session.get(resturl, timeout=3)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        # exit if we can't connect
        sys.exit(1)

    resourceDict = dict()

    if resp.status_code == 200:
        allResources = resp.json()
        for aResource in allResources:
            # print(f"resource={aResource}")
            resName = aResource["resourceName"]
            resType = aResource["resourceTypeName"]
            resourceDict[resName] = resType
    else:
        print(f"resources rc={resp.status_code} - unable to list resources")

    return resourceDict


def doesResourceHaveSimilarity(resourceName) -> bool:
    """
    see if there is any similarity discovery
    """
    hasSim = False
    try:
        resturl = edcHelper.baseUrl + "/access/1/catalog/resources/" + resourceName
        resp = edcHelper.session.get(resturl, timeout=3)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        return False

    if resp.status_code == 200:
        resJson = resp.json()
        for aScanner in resJson["scannerConfigurations"]:
            if "configOptions" in aScanner:
                for config in aScanner["configOptions"]:
                    # print("config")
                    optionId = config.get("optionId")
                    if optionId == "RunSimilarityProfile":
                        # print("run similarity????==")
                        # print(config.get("optionValues"))
                        if "true" in config.get("optionValues"):
                            # print("winner!!!")
                            hasSim = True
            # print(aScanner)

    return hasSim


def getResourcesWithSimilarity() -> list:
    """
    find just the resources with similarity profiling
    """
    simResources = list()
    resourceTN = listResources()
    print(f"all resources count = {len(resourceTN)}")

    for aResource in resourceTN.keys():
        if doesResourceHaveSimilarity(aResource):
            simResources.append(aResource)
            print("+", end="", flush=True)
        else:
            print(".", end="", flush=True)
        # hasSim = doesResourfeHaveSimilarity(aResource)

    # print(f"\nsimResources = {len(simResources)}")
    return simResources


def getElementsForResource(resourceName):
    """
    get the count of elements for a resource
    """
    q = f'+core.resourceName:"{resourceName}" +core.allclassTypes:"core.DataElement"'
    parameters = {"q": q, "offset": 0, "pageSize": 1}

    try:
        resturl = edcHelper.baseUrl + "/access/1/catalog/data/search"
        resp = edcHelper.session.get(resturl, params=parameters, timeout=3)
    except requests.exceptions.RequestException as e:
        print("Error connecting to : " + resturl)
        print(e)
        # exit if we can't connect
        return

    # print(resp.text)
    searchJson = resp.json()
    # print(searchJson)
    totalElements = searchJson["totalCount"]
    # print(totalElements)

    return totalElements


def writeSimilarityResults(resourceName, theWriter, totalToExtract):
    objectsurl = (
        edcHelper.baseUrl + "/access/1/catalog/data/objects"
    )  # note:  v1 api used here for additionalFacts
    # header = {"Accept": "application/json"}
    # note:  if you want to test for a single resource, add and core.resourceName:<name>
    query = (
        f'+core.resourceName:"{resourceName}" +core.allclassTypes:(core.DataElement)'
    )
    # initial value - set to > 0 will be over-written by the count of objects returned
    total = totalToExtract
    offset = 0
    page = 0
    # pageSize = 250

    while offset < total:
        # itemCount = 0
        itemsWithSim = 0
        simLinks = 0
        page += 1
        parameters = {
            "q": query,
            "offset": offset,
            "pageSize": mem.pageSize,
            "sort": "id asc",
        }

        page_time = time.time()
        print(
            f"\tprocessing:{offset + 1}-{offset + mem.pageSize}/{total}"
            f" pagesize={mem.pageSize}  page={page}",
            end="",
            flush=True,
        )
        # execute catalog rest call, for a page of results
        resp = edcHelper.session.get(objectsurl, params=parameters)
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            # print("error! " + str(status) + str(resp.json()))
            break

        resultJson = resp.json()
        # note v1 total count is not indside of ['metadata']
        total = resultJson["totalCount"]

        # for next iteration
        offset += mem.pageSize

        # for each item found
        for foundItem in resultJson["items"]:
            hasSimilarity, itemSimLinks = processFoundItem(foundItem)
            simLinks += itemSimLinks
            if hasSimilarity:
                itemsWithSim += 1

        # end of while loop
        print(
            f" time={time.time() - page_time:.1f}s - "
            f"{itemsWithSim}/{simLinks}/{mem.maxSim}"
            f" items/simLinks"
        )
        mem.fCSVFile.flush()  # flush the csv file for each page/chunk


def get_parent_id(an_id: str) -> str:
    """
    given an id - get the parent id.
    for most items - it will be the id before the last /
    for .json or .xml - it will be the id up to the .json/.xml
    """
    parent_id = an_id.rsplit("/", 1)[0]
    if an_id.find(".xml") > 0:
        parent_id = an_id[: an_id.find(".xml") + 4]
    elif an_id.find(".json") > 0:
        parent_id = an_id[: an_id.find(".json") + 5]

    return parent_id


def processFoundItem(foundItem):
    itemId = foundItem["id"]
    simLinks = 0
    hasSim = False

    from_resource = itemId.split("://")[0]
    to_resource = ""

    nameCount = 0
    pattCount = 0
    valuCount = 0
    freqCount = 0

    # check if any additional facts are present
    for addFacts in foundItem["additionalFacts"]:
        provider = addFacts.get("id")
        href = addFacts.get("href")
        if provider == "similar-cols-provider":
            # format the query to get similar objects
            simUrl = edcHelper.baseUrl + "/access" + href
            simParms = {"pageSize": 1000}
            respSim = edcHelper.session.get(simUrl, params=simParms)
            simstatus = respSim.status_code
            if simstatus != 200:
                print(f"error getting similar-cols-provider data for {href}")
                continue
            # print ('\trc=' + str(respSim.status_code))
            simResult = respSim.json()
            hasSimLinks = False
            hasSim = True
            columnLinks = 0
            if "dstObjects" not in simResult.keys():
                print("\tno dstObjects in simResult for " + itemId)
                # don't process - this can happen for synonym columns named *
                continue

            simCount = 0
            for dstObject in simResult["dstObjects"]:
                simCount += 1
                patternSim = ""
                nameSim = ""
                valSim = ""
                frqSim = ""
                scoreSim = ""
                hasSimLinks = True

                dstId = dstObject["id"]
                to_resource = dstId.split("://")[0]

                # dstassoc = dstObject["association"]
                # print("\tsimilar column link: " + dstId)
                columnLinks += 1
                for linkProps in dstObject["linkProperties"]:
                    name = linkProps.get("name")
                    val = linkProps.get("value")
                    if name == "com.infa.ldm.similarity.patternSimScore":
                        patternSim = val
                        pattCount += 1
                    if name == "com.infa.ldm.similarity.nameSimScore":
                        nameSim = val
                        nameCount += 1
                    if name == "com.infa.ldm.similarity.valuesSimScore":
                        valSim = val
                        valuCount += 1
                    if name == "com.infa.ldm.similarity.vfSimScore":
                        frqSim = val
                        freqCount += 1
                    if name == "com.infa.ldm.similarity.confidenceScore":
                        scoreSim = val

                simLinks += 1
                mem.simLinks += 1

                # write individual object similar stats  (for each similar object)
                mem.colWriter.writerow(
                    [
                        from_resource,
                        itemId,
                        to_resource,
                        dstId,
                        scoreSim,
                        valSim,
                        frqSim,
                        patternSim,
                        nameSim,
                        get_parent_id(itemId),
                        get_parent_id(dstId),
                    ]
                )

            if hasSimLinks:
                mem.simObjects += 1
                # itemsWithSim += 1
    if simLinks > mem.maxSim:
        mem.maxSim = simLinks
        mem.maxSimId = itemId

    # write the total counts of similar items (with each metric count too)
    mem.countWriter.writerow(
        [
            itemId.split(":")[0],
            itemId,
            itemId.split("/")[-1],
            simLinks,
            nameCount,
            pattCount,
            valuCount,
            freqCount,
        ]
    )
    return hasSim, simLinks


def initCsvOutFile():
    # csvFileName = "out/similarObjectsByResource.csv"
    columnHeader = [
        "From Resource",
        "ObjectId",
        "To Resource",
        "SimilarObjectId",
        "Confidence",
        "Data Similarity",
        "Frequency Similarity",
        "Pattern Similarity",
        "Name Similarity",
        "From Struct",
        "To Struct",
    ]
    mem.fCSVFile = open(mem.outFile, "w", newline="", encoding="utf-8")
    mem.colWriter = csv.writer(mem.fCSVFile)
    mem.colWriter.writerow(columnHeader)

    mem.fSimCounts = open(
        mem.outFile.replace(".csv", "_counts.csv"), "w", newline="", encoding="utf-8"
    )
    mem.countWriter = csv.writer(mem.fSimCounts)
    mem.countWriter.writerow(
        ["resource", "id", "name", "sim count", "name", "pattern", "value", "freq", ]
    )


def main():
    # initialize the edc session and common connection parms
    print("similarityByResource - starting")
    print(os.getcwd())
    edcHelper.initUrlAndSessionFromEDCSettings()
    # parse the script specific parameters
    args, unknown = argparser.parse_known_args()
    mem.pageSize = args.pageSize
    mem.outFile = args.outFile

    start_time = time.time()
    mem.simObjects = 0
    mem.simLinks = 0
    mem.maxSim = 0
    mem.maxSimId = ""

    # create the csv file to store the results
    initCsvOutFile()

    simResources = getResourcesWithSimilarity()
    simResources.sort(key=lambda y: y.lower())

    print(f"\nsimilar resources {len(simResources)}")
    print(simResources)
    elCount = 0
    resource_count = 0

    for aResource in simResources:
        resource_count += 1
        elems = getElementsForResource(aResource)
        elCount += elems
        print(f"{resource_count}/{len(simResources)} - {aResource} elements={elems}")
        writeSimilarityResults(aResource, mem.colWriter, elems)

    end_time = time.time()
    print("\n*********")
    print(f"Finished - {(end_time - start_time):.0f} seconds")
    print(f"\ttotal objs={elCount:,}")
    print(f"\tsimObjects={mem.simObjects:,} simLinks={mem.simLinks:,}")
    print("*********\n")

    print(f"max sim links: {mem.maxSim}")
    print(f"max sim id   : {mem.maxSimId}")

    # close files
    mem.fCSVFile.close()
    mem.fSimCounts.close()


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
