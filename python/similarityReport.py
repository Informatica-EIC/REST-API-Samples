"""
Created on Jul 11, 2018

@author: dwrigley

similarityReport - get information about all similar relationships for an object

Note:  this is 10.2.1+ only - the method is different starting with 10.2.1
       & uses v1 (undocumented) api to get the additional context

Note:  this script is rough - it works, but has not been optimized in any way
       or verbose comments added
        things to add/change include
            refine the output to the console - this is raw at the moment
            add error checking

"""

import requests
from requests.auth import HTTPBasicAuth
import csv
import platform
import time
import sys

print("ColumnSimilarity Report")
# ******************************************************
# change these settings for your catalog service
# ******************************************************
catalogServer = "http://napslxapp01:9085"
uid = "Administrator"
# pwd='admin'
pwd = uid
pageSize = 100
# ******************************************************
# end of parameters that should be changed
# ******************************************************

objectsurl = (
    catalogServer + "/access/1/catalog/data/objects"
)  # note:  v1 api used here for additionalFacts
header = {"Accept": "application/json"}
# note:  if you want to test for a single resource, add and core.resourceName:<name>
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
 "
# and core.resourceName:* \

# format the csv output file (note may differ for python 2.7 & 3)
csvFileName = "out/similarObjects.csv"
columnHeader = [
    "ObjectId",
    "SimilarObjectId",
    "Confidence",
    "Data Similarity",
    "Frequency Similarity",
    "Pattern Similarity",
    "Name Similarity",
]
if str(platform.python_version()).startswith("2.7"):
    fCSVFile = open(csvFileName, "w")
else:
    fCSVFile = open(csvFileName, "w", newline="", encoding="utf-8")
colWriter = csv.writer(fCSVFile)
colWriter.writerow(columnHeader)

start_time = time.time()
print("sys.stdout.encoding=" + sys.stdout.encoding)

# initial value - set to > 0 will be over-written by the count of objects returned
total = 1000
offset = 0
page = 0

print("url=" + objectsurl)
print("user=" + uid)
print("query=" + query)
print("")
itemCount = 0
itemsWithSim = 0
simLinks = 0

while offset < total:
    page += 1
    parameters = {"q": query, "offset": offset, "pageSize": pageSize}

    # execute catalog rest call, for a page of results
    resp = requests.get(
        objectsurl, params=parameters, headers=header, auth=HTTPBasicAuth(uid, pwd)
    )
    status = resp.status_code
    if status != 200:
        # some error - e.g. catalog not running, or bad credentials
        print("error! " + str(status) + str(resp.json()))
        break

    resultJson = resp.json()
    total = resultJson[
        "totalCount"
    ]  # note v1 total count is not indside of ['metadata']
    print(
        "processing:"
        + str(offset + 1)
        + "-"
        + str(offset + pageSize)
        + "/"
        + str(total)
        + " pagesize="
        + str(pageSize)
        + " currentPage="
        + str(page)
    )

    # for next iteration
    offset += pageSize

    # for each item found
    for foundItem in resultJson["items"]:
        itemCount += 1
        itemId = foundItem["id"]
        itemHref = foundItem["href"]
        itemType = ""
        itemName = ""
        print("item " + str(itemCount) + " id=" + itemId)

        # check if any additional facts are present
        for addFacts in foundItem["additionalFacts"]:
            provider = addFacts.get("id")
            href = addFacts.get("href")
            if provider == "similar-cols-provider":
                # format the query to get similar objects
                simUrl = catalogServer + "/access" + href
                simParms = {"pageSize": 1000}
                # print("\tcalling get: " + simUrl)
                respSim = requests.get(
                    simUrl,
                    params=simParms,
                    headers=header,
                    auth=HTTPBasicAuth(uid, pwd),
                )
                simstatus = respSim.status_code
                # print ('\trc=' + str(respSim.status_code))
                simResult = respSim.json()
                hasSimLinks = False
                columnLinks = 0

                for dstObject in simResult["dstObjects"]:
                    patternSim = ""
                    nameSim = ""
                    valSim = ""
                    frqSim = ""
                    scoreSim = ""
                    hasSimLinks = True

                    dstId = dstObject["id"]
                    dstassoc = dstObject["association"]
                    # print("\tsimilar column link: " + dstId)
                    columnLinks += 1
                    for linkProps in dstObject["linkProperties"]:
                        name = linkProps.get("name")
                        val = linkProps.get("value")
                        if name == "com.infa.ldm.similarity.patternSimScore":
                            patternSim = val
                        if name == "com.infa.ldm.similarity.nameSimScore":
                            nameSim = val
                        if name == "com.infa.ldm.similarity.valuesSimScore":
                            valSim = val
                        if name == "com.infa.ldm.similarity.vfSimScore":
                            frqSim = val
                        if name == "com.infa.ldm.similarity.confidenceScore":
                            scoreSim = val

                    # print("\t\t" + scoreSim +":" + patternSim +":" + nameSim +":" +
                    # valSim +":" + frqSim +":")
                    simLinks += 1
                    colWriter.writerow(
                        [itemId, dstId, scoreSim, valSim, frqSim, patternSim, nameSim]
                    )
                if hasSimLinks:
                    itemsWithSim += 1

                print("\tsimilar column count: " + str(columnLinks))

    # end of while loop
    fCSVFile.flush  # flush the csv file for each page/chunk

# finished
print("")
print("Finished - run time = %s seconds ---" % (time.time() - start_time))
print("result written to: " + csvFileName)
print(
    "counts: items="
    + str(itemCount)
    + " items that have similar links="
    + str(itemsWithSim)
    + " similarLinks="
    + str(simLinks)
)
