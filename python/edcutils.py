"""
Created on Aug 2, 2018

utility functions for processing catalog objects

@author: dwrigley
"""

import requests
import json
from requests.auth import HTTPBasicAuth
import os


def getFactValue(item, attrName):
    """
    returns the value of a fact (attribute) from an item

    iterates over the "facts" list - looking for a matching attributeId
    to the paramater attrName
    returns the "value" property or ""
    """
    # get the value of a specific fact from an item
    value = ""
    for facts in item["facts"]:
        if facts.get("attributeId") == attrName:
            value = facts.get("value")
            break
    return value


def exportLineageLink(fromObject, toObject, linkType, csvFile):
    """
    write a custom lineage line to the csv file
    assumptions
      - csvFile is already created
      - csv header is Association,From Connection,To Connection,From Object,To Object
    Association=linkType, From Object=fromObject,To Object=toObject
    From Connection and To Connection will be empty
    """
    # TODO: add error checking
    row = [linkType, "", "", fromObject, toObject]
    csvFile.writerow(row)
    return


def getAllResource(url, user, pWd):
    """
    get the resource definition - given a resource name (and catalog url)
    catalog url should stop at port (e.g. not have ldmadmin, ldmcatalog etc...
    or have v2 anywhere
    since we are using v1 api's

    returns rc=200 (valid) & other rc's from the get
            resourceDef (json)

    """

    print("getting resource for catalog:-" + url + " user=" + user)
    apiURL = url + "/access/1/catalog/resources/"
    # print("\turl=" + apiURL)
    header = {"Accept": "application/json"}
    tResp = requests.get(
        apiURL, params={}, headers=header, auth=HTTPBasicAuth(user, pWd), verify=False
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        return tResp.status_code, json.loads(tResp.text)
    else:
        # not valid
        return tResp.status_code, None


def getResourceDefUsingSession(url, session, resourceName, sensitiveOptions=False):
    """
    get the resource definition - given a resource name (and catalog url)
    catalog url should stop at port (e.g. not have ldmadmin, ldmcatalog etc...
    or have v2 anywhere
    since we are using v1 api's

    returns rc=200 (valid) & other rc's from the get
            resourceDef (json)

    """

    print("getting resource for catalog:-" + url + " resource=" + resourceName)
    apiURL = url + "/access/1/catalog/resources/" + resourceName
    if sensitiveOptions:
        apiURL += "?sensitiveOptions=true"
    # print("\turl=" + apiURL)
    header = {"Accept": "application/json"}
    tResp = session.get(
        apiURL,
        params={},
        headers=header,
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        return tResp.status_code, json.loads(tResp.text)
    else:
        # not valid
        return tResp.status_code, None


def getResourceDef(url, user, pWd, resourceName, sensitiveOptions=False):
    """
    get the resource definition - given a resource name (and catalog url)
    catalog url should stop at port (e.g. not have ldmadmin, ldmcatalog etc...
    or have v2 anywhere
    since we are using v1 api's

    returns rc=200 (valid) & other rc's from the get
            resourceDef (json)

    """

    print(
        "getting resource for catalog:-"
        + url
        + " resource="
        + resourceName
        + " user="
        + user
    )
    apiURL = url + "/access/1/catalog/resources/" + resourceName
    if sensitiveOptions:
        apiURL += "?sensitiveOptions=true"
    # print("\turl=" + apiURL)
    header = {"Accept": "application/json"}
    tResp = requests.get(
        apiURL, params={}, headers=header, auth=HTTPBasicAuth(user, pWd), verify=False
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        return tResp.status_code, json.loads(tResp.text)
    else:
        # not valid
        return tResp.status_code, None


def updateResourceDef(url, user, pWd, resourceName, resJson):
    """
    update a setting in an existing resource

    returns rc=200 (valid) & other rc's from the put
            resourceDef (json)

    """

    print(
        "\tupdating resource for catalog:-"
        + url
        + " resource="
        + resourceName
        + " user="
        + user
    )
    print("\t" + json.dumps(resJson))
    apiURL = url + "/access/1/catalog/resources/" + resourceName
    print("\turl=" + apiURL)
    header = {"Accept": "application/json", "Content-Type": "application/json"}
    tResp = requests.put(
        apiURL,
        data=json.dumps(resJson),
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        print("\tyay - update resource worked...")
        print(tResp)
        return tResp.status_code
    else:
        # not valid
        print("\tdarn - update resource failed...")
        print(tResp)
        return tResp.status_code


def updateResourceDefUsingSession(url, session, resourceName, resJson):
    """
    update a setting in an existing resource

    returns rc=200 (valid) & other rc's from the put
            resourceDef (json)

    """

    print("\tupdating resource for catalog:-" + url + " resource=" + resourceName)
    # print("\t" + json.dumps(resJson))
    apiURL = url + "/access/1/catalog/resources/" + resourceName
    # print("\turl=" + apiURL)
    header = {"Accept": "application/json", "Content-Type": "application/json"}
    tResp = session.put(
        apiURL,
        data=json.dumps(resJson),
        headers=header,
    )
    # print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        print(f"\tresource successfully updated, rc={tResp.status_code}")
        # print(tResp)
        return tResp.status_code
    else:
        # not valid
        print("\tupdate resource failed... rc={tResp.status_code}")
        # print(tResp)
        return tResp.status_code


def createResource(url, user, pWd, resourceName, resourceJson):
    """
    create a new resource based on the provided JSON

    returns rc=200 (valid) & other rc's from the put
            resourceDef (json)

    """
    # create a new resource
    apiURL = url + "/access/1/catalog/resources/"
    header = {"content-type": "application/json"}
    print("\tcreating resource: " + resourceName)
    newResourceResp = requests.post(
        apiURL,
        data=json.dumps(resourceJson),
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\trc=" + str(newResourceResp.status_code))
    print("\tbody=" + str(newResourceResp.text))

    return newResourceResp.status_code


def createResourceUsingSession(url, session, resourceName, resourceJson):
    """
    create a new resource based on the provided JSON
    using a session that already has the auth (credentials)

    returns rc=200 (valid) & other rc's from the put
            resourceDef (json)

    """
    # create a new resource
    apiURL = url + "/access/1/catalog/resources/"
    header = {"content-type": "application/json"}
    print("\tcreating resource: " + resourceName)
    newResourceResp = session.post(
        apiURL, data=json.dumps(resourceJson), headers=header
    )
    print("\trc=" + str(newResourceResp.status_code))
    # print("\tbody=" + str(newResourceResp.text))

    return newResourceResp.status_code


def uploadResourceFileUsingSession(
    url, session, resourceName, fileName, fullPath, scannerId
):
    """
    upload a file for the resource - e.g. a custom lineage csv file
    works with either csv for zip files  (.csv|.zip)

    returns rc=200 (valid) & other rc's from the post

    """
    print("uploading file for resource " + url + " resource=" + resourceName)
    apiURL = url + "/access/1/catalog/resources/" + resourceName + "/files"
    print("\turl=" + apiURL)
    # header = {"accept": "*/*", }
    params = {"scannerid": scannerId, "filename": fileName, "optionid": "File"}
    print("\t" + str(params))
    #     files = {'file': fullPath}
    mimeType = "text/csv"
    readMode = "rt"
    if fileName.endswith(".zip"):
        mimeType = "application/zip"
        readMode = "rb"

    if fileName.endswith(".dsx"):
        mimeType = "text/plain"

    file = {"file": (fileName, open(fullPath, readMode), mimeType)}
    # file = {"file": (fileName, open(fullPath, readMode), )}
    print(f"\t{file}")
    # print(f"session header:{session.headers}")
    uploadResp = session.post(
        apiURL,
        data=params,
        files=file,
    )
    print("\tresponse=" + str(uploadResp.status_code))
    if uploadResp.status_code == 200:
        # valid - return the json
        return uploadResp.status_code
    else:
        # not valid
        print("\tupload file failed")
        print("\t" + str(uploadResp))
        print("\t" + str(uploadResp.text))
        return uploadResp.status_code


def uploadResourceFile(url, user, pWd, resourceName, fileName, fullPath, scannerId):
    """
    upload a file for the resource - e.g. a custom lineage csv file
    works with either csv for zip files  (.csv|.zip)

    returns rc=200 (valid) & other rc's from the post

    """
    print(
        "uploading file for resource "
        + url
        + " resource="
        + resourceName
        + " user="
        + user
    )
    apiURL = url + "/access/1/catalog/resources/" + resourceName + "/files"
    print("\turl=" + apiURL)
    # header = {"accept": "*/*", "Content-Type" : "multipart/form-data"}
    header = {"accept": "*/*"}
    print("\t" + str(header))
    # params={"scannerid": "LineageScanner", "filename": fileName, "optionid": "File"}
    params = {"scannerid": scannerId, "filename": fileName, "optionid": "File"}
    print("\t" + str(params))
    #     files = {'file': fullPath}
    mimeType = "text/csv"
    readMode = "rt"
    if fileName.endswith(".zip"):
        mimeType = "application/zip"
        readMode = "rb"

    if fileName.endswith(".dsx"):
        mimeType = "text/plain"

    file = {"file": (fileName, open(fullPath, readMode), mimeType)}
    print("\t" + str(file))
    uploadResp = requests.post(
        apiURL,
        data=params,
        files=file,
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\tresponse=" + str(uploadResp.status_code))
    if uploadResp.status_code == 200:
        # valid - return the jsom
        return uploadResp.status_code
    else:
        # not valid
        print("\tupload file failed")
        print("\t" + str(uploadResp))
        print("\t" + str(uploadResp.text))
        return uploadResp.status_code


def executeResourceLoadUsingSession(url, session, resourceName):
    """
    start a resource load

    returns rc=200 (valid) & other rc's from the get
            json with the job details

    """

    print("starting scan resource " + url + " resource=" + resourceName)
    apiURL = url + "/access/2/catalog/resources/jobs/loads"
    print("\turl=" + apiURL)
    header = {"accept": "application/json", "Content-Type": "application/json"}
    print("\t" + str(header))
    params = {"resourceName": resourceName}
    print("\t" + str(params))
    uploadResp = session.post(apiURL, data=json.dumps(params), headers=header)
    print("\tresponse=" + str(uploadResp.status_code))
    if uploadResp.status_code == 200:
        # valid - return the jsom
        return uploadResp.status_code, json.loads(uploadResp.text)
    else:
        # not valid
        print("\tdarn - resource start failed")
        print("\t" + str(uploadResp))
        print("\t" + str(uploadResp.text))
        return uploadResp.status_code, None


def executeResourceLoad(url, user, pWd, resourceName):
    """
    start a resource load

    returns rc=200 (valid) & other rc's from the get
            json with the job details

    """

    print(
        "starting scan resource " + url + " resource=" + resourceName + " user=" + user
    )
    apiURL = url + "/access/2/catalog/resources/jobs/loads"
    print("\turl=" + apiURL)
    header = {"accept": "application/json", "Content-Type": "application/json"}
    print("\t" + str(header))
    params = {"resourceName": resourceName}
    print("\t" + str(params))
    uploadResp = requests.post(
        apiURL,
        data=json.dumps(params),
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\tresponse=" + str(uploadResp.status_code))
    if uploadResp.status_code == 200:
        # valid - return the jsom
        return uploadResp.status_code, json.loads(uploadResp.text)
    else:
        # not valid
        print("\tdarn - resource start failed")
        print("\t" + str(uploadResp))
        print("\t" + str(uploadResp.text))
        return uploadResp.status_code, None


def createOrUpdateAndExecuteResourceUsingSession(
    url,
    session,
    resourceName,
    templateFileName,
    fileName,
    inputFileFullPath,
    waitForComplete,
    scannerId,
):
    """
    create or update resourceName  (new way with sessions)
    upload a file
    execute the scan
    optionally wait for the scan to complete

    assumption - from the template, we are only changing the resource name,
                 and filename options - all else is already in the template

    @todo:  add a diff process to determine if the input file is different to last time
            - assume last file ins in what folder???
    """
    # check if the file to be uploaded exists
    if os.path.isfile(inputFileFullPath):

        # check if the file is different from /prev
        # if /prev/<file> either does not exist, or is different
        #   proceed
        #
        # else  (file content is the same)
        #   do nothing

        # get existing resource (so we know to create it or update it)
        validResource = False
        rc, rj = getResourceDefUsingSession(url, session, resourceName)

        if rc == 200:
            validResource = True
            # valid resource
            print("\tresource is valid: " + resourceName)
            print("\tchecking for file name change...")
            # print(rj)

            # check the file name in the json results
            isResChanged = False
            # check if the resource file name is the same as the file we are uploading
            for config in rj["scannerConfigurations"]:
                for opt in config["configOptions"]:
                    optId = opt.get("optionId")
                    optVals = opt.get("optionValues")
                    # print (opt)
                    if optId == "File":
                        print("\t     file=" + str(optVals))
                        print("\tcheckiung:" + fileName)
                        if fileName in optVals:
                            print("\t\tfile name is same...")
                        else:
                            print("\t\tfile name different")
                            isResChanged = True
                            # replace the optionValues content (the file name)
                            opt["optionValues"] = [fileName]

            # if the properties of the resource changed, update it
            if isResChanged:
                # save the resource def
                print("saving resource def...")
                updRc = updateResourceDefUsingSession(url, session, resourceName, rj)
                print(updRc)
                if updRc == 200:
                    print("update succeeded")
                else:
                    print("update failed")
                    print("resource could be out of sync - load might fail")
            else:
                print("\tno changes to resource def...")

        else:
            print("\tneed to create resource: %s" % resourceName)
            # check the template file exists
            if os.path.isfile(templateFileName):
                # create resource using this template
                # newResourceJson = json.load(lineageResourceTemplate)
                with open(templateFileName) as json_data:
                    templateJson = json.load(json_data)

                # print(templateJson)
                # set the resource name
                templateJson["resourceIdentifier"]["resourceName"] = resourceName

                # print(templateJson)
                # set the File property (in configOptions)
                for config in templateJson["scannerConfigurations"]:
                    for opt in config["configOptions"]:
                        optId = opt.get("optionId")
                        optVals = opt.get("optionValues")
                        if optId == "File":
                            opt["optionValues"] = [fileName]

                # print(templateJson)
                createRc = createResourceUsingSession(
                    url, session, resourceName, templateJson
                )
                if createRc == 200:
                    validResource = True
                else:
                    print("error creating resource: cannot upload file and scan")

            else:
                print("lineage template file does not exist: " + templateFileName)

        # if the resource is valid
        # (either created as new, or updated with new file name)
        if validResource:
            # upload the new file
            print(
                "uploading file"
                + " "
                + inputFileFullPath
                + " to resource: "
                + resourceName
            )
            uploadRc = uploadResourceFileUsingSession(
                url, session, resourceName, fileName, inputFileFullPath, scannerId
            )
            # print(uploadRc)

            # if the file was uploaded - start the resource load
            if uploadRc == 200:
                print("starting resource load: " + resourceName)
                loadRc, loadJson = executeResourceLoadUsingSession(
                    url, session, resourceName
                )
                if loadRc == 200:
                    # print(loadJson)
                    print("\tJob Queued: " + loadJson.get("jobId"))
                    print("\tJob def: " + str(loadJson))

                    if waitForComplete:
                        print("waiting for job completion is not implemented yet")
                else:
                    print("\tjob not started " + str(loadRc))
            else:
                print("file not uploaded - resource/scan will not be started")

    else:
        # file does not exist
        print(
            "resource input file: "
            + inputFileFullPath
            + " invalid or does not exist, exiting"
        )


# end


def createOrUpdateAndExecuteResource(
    url,
    user,
    pwd,
    resourceName,
    templateFileName,
    fileName,
    inputFileFullPath,
    waitForComplete,
    scannerId,
):
    """
    create or update resourceName
    (note: old way - consider moving to sessions (better for id/pwd/ssl validation))
    upload a file
    execute the scan
    optionally wait for the scan to complete

    assumption - from the template, we are only changing the resource name,
                 and filename options - all else is already in the template

    @todo:  add a diff process to determine if the input file is different to last time
            - assume last file ins in what folder???
    """
    # check if the file to be uploaded exists
    if os.path.isfile(inputFileFullPath):

        # check if the file is different from /prev
        # if /prev/<file> either does not exist, or is different
        #   proceed
        #
        # else  (file content is the same)
        #   do nothing

        # get existing resource (so we know to create it or update it)
        validResource = False
        rc, rj = getResourceDef(url, user, pwd, resourceName)

        if rc == 200:
            validResource = True
            # valid resource
            print("\tresource is valid: " + resourceName)
            print("\tchecking for file name change...")
            # print(rj)

            # check the file name in the json results
            isResChanged = False
            # check if the resource file name is the same as the file we are uploading
            for config in rj["scannerConfigurations"]:
                for opt in config["configOptions"]:
                    optId = opt.get("optionId")
                    optVals = opt.get("optionValues")
                    # print (opt)
                    if optId == "File":
                        print("\t     file=" + str(optVals))
                        print("\tcheckiung:" + fileName)
                        if fileName in optVals:
                            print("\t\tfile name is same...")
                        else:
                            print("\t\tfile name different")
                            isResChanged = True
                            # replace the optionValues content (the file name)
                            opt["optionValues"] = [fileName]

            # if the properties of the resource changed, update it
            if isResChanged:
                # save the resource def
                print("saving resource def...")
                updRc = updateResourceDef(url, user, pwd, resourceName, rj)
                print(updRc)
                if updRc == 200:
                    print("update succeeded")
                else:
                    print("update failed")
                    print("resource could be out of sync - load might fail")
            else:
                print("\tno changes to resource def...")

        else:
            print("\tneed to create resource: %s" % resourceName)
            # check the template file exists
            if os.path.isfile(templateFileName):
                # create resource using this template
                # newResourceJson = json.load(lineageResourceTemplate)
                with open(templateFileName) as json_data:
                    templateJson = json.load(json_data)

                # print(templateJson)
                # set the resource name
                templateJson["resourceIdentifier"]["resourceName"] = resourceName

                # print(templateJson)
                # set the File property (in configOptions)
                for config in templateJson["scannerConfigurations"]:
                    for opt in config["configOptions"]:
                        optId = opt.get("optionId")
                        optVals = opt.get("optionValues")
                        if optId == "File":
                            opt["optionValues"] = [fileName]

                # print(templateJson)
                createRc = createResource(url, user, pwd, resourceName, templateJson)
                if createRc == 200:
                    validResource = True
                else:
                    print("error creating resource: cannot upload file and scan")

            else:
                print("lineage template file does not exist: " + templateFileName)

        # if the resource is valid
        # (either created as new, or updated with new file name)
        if validResource:
            # upload the new file
            print(
                "uploading file"
                + " "
                + inputFileFullPath
                + " to resource: "
                + resourceName
            )
            uploadRc = uploadResourceFile(
                url, user, pwd, resourceName, fileName, inputFileFullPath, scannerId
            )
            # print(uploadRc)

            # if the file was uploaded - start the resource load
            if uploadRc == 200:
                print("starting resource load: " + resourceName)
                loadRc, loadJson = executeResourceLoad(url, user, pwd, resourceName)
                if loadRc == 200:
                    # print(loadJson)
                    print("\tJob Queued: " + loadJson.get("jobId"))
                    print("\tJob def: " + str(loadJson))

                    if waitForComplete:
                        print("waiting for job completion is not implemented yet")
                else:
                    print("\tjob not started " + str(loadRc))
            else:
                print("file not uploaded - resource/scan will not be started")

    else:
        # file does not exist
        print(
            "resource input file: "
            + inputFileFullPath
            + " invalid or does not exist, exiting"
        )


def callGETRestEndpoint(apiURL, user, pWd):
    """
    this function call the URL  with a GET method and return the status code
    as well as the response body
    returns rc=200 (valid) & other rc's from the get
            resourceDef (json)
    """
    header = {"Accept": "application/json"}
    tResp = requests.get(
        apiURL, params={}, headers=header, auth=HTTPBasicAuth(user, pWd), verify=False
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        return tResp.status_code, json.loads(tResp.text)
    else:
        # not valid
        return tResp.status_code, None


def getResourceObjectCount(url, user, pWd, resourceName):
    """
    get the resource object count - given a resource name (and catalog url)
    """

    apiURL = url + "/access/2/catalog/data/objects?q=core.resourceName:" + resourceName
    print(
        "getting object count for catalog resource:-"
        + apiURL
        + " resource="
        + resourceName
        + " user="
        + user
    )
    return callGETRestEndpoint(apiURL, user, pWd)


def getCatalogObjectCount(url, user, pWd):
    """
    get the resource object count - given a catalog url
    """

    print("getting object count for catalog resource:-" + url + " user=" + user)
    apiURL = url + "/access/2/catalog/data/objects"
    return callGETRestEndpoint(apiURL, user, pWd)


def getCatalogResourceCount(url, user, pWd):
    """
    get the resource count - given a catalog url
    """

    apiURL = url + "/access/2/catalog/data/objects?q=core.allclassTypes:core.Resource"
    print("getting object count for catalog resource:-" + apiURL + " user=" + user)
    return callGETRestEndpoint(apiURL, user, pWd)


def getReusableScannerConfig(url, user, pWd):
    """
    get the reusable configuration - given a catalog url
    """

    apiURL = url + "/access/1/catalog/resources/reusablescannerconfigs"
    print("getting the reusable configuration:-" + apiURL + " user=" + user)
    return callGETRestEndpoint(apiURL, user, pWd)


def getCatalogCustomAttr(url, user, pWd):
    """
    call GET /access/2/catalog/models/attributes
    this returns all attributes (system + custom)
    filter for only the custom attributes (id startswith "com.infa.appmodels.ldm."
    """

    resturl = url + "/access/2/catalog/models/attributes"
    header = {"Accept": "application/json"}

    # set initial value for total to > 0 - is over-written by the count of objects found
    total = 1000
    offset = 0
    page = 0
    pageSize = 200

    customAttrs = []

    while offset < total:
        page += 1
        parameters = {"offset": offset, "pageSize": pageSize}

        # execute catalog rest call, for a page of results
        resp = requests.get(
            resturl,
            params=parameters,
            headers=header,
            auth=HTTPBasicAuth(user, pWd),
            verify=False,
        )
        status = resp.status_code
        if status != 200:
            # some error - e.g. catalog not running, or bad credentials
            print("error! " + str(status) + str(resp.json()))
            break

        resultJson = resp.json()
        total = resultJson["metadata"]["totalCount"]
        print(
            "objects found: "
            + str(total)
            + " processing:"
            + str(offset + 1)
            + "-"
            + str(offset + pageSize)
            + " pagesize="
            + str(pageSize)
            + " currentPage="
            + str(page)
        )

        # for next iteration
        offset += pageSize

        # for each attribute found...
        for attrDef in resultJson["items"]:
            # attrCount+=1
            attrId = attrDef["id"]
            if attrId.startswith("com.infa.appmodels.ldm."):
                customAttrs.append(attrDef)

    # end of while loop
    # print("")
    # print("Finished - run time = %s seconds ---" % (time.time() - start_time))
    return customAttrs


def createAttribute(url, user, pWd, attrJson):
    """
    create a new attribute
    attrJSON must be in the form
        {
          "items": [
            {
              "analyzer": "INT",
              "boost": "LOWEST",
              "classes": [
                {
                  "id": "string"
                }
              ],
              "dataTypeId": "string",
              "description": "string",
              "facetable": false,
              "multivalued": false,
              "name": "string",
              "searchable": false,
              "sortable": false,
              "suggestable": false
            }
          ]
        }

    """
    apiURL = url + "/access/2/catalog/models/attributes/"
    header = {"content-type": "application/json"}
    print("\tcreating custom attribute: " + attrJson["items"][0]["name"])
    newAttrResp = requests.post(
        apiURL,
        data=json.dumps(attrJson),
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\trc=" + str(newAttrResp.status_code))
    print("\tbody=" + str(newAttrResp.text))
    print(attrJson)

    return newAttrResp.status_code
