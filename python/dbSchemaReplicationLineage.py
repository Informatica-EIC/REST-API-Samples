'''
Created on Jun 26, 2018

@author: dwrigley
**************************************************************************************************
DB schema replication custom lineage generator

process:-
    scenario:  the tables in 2 different schemas (perhaps in different databases/resources) are replicated
    in EDC - we have no way to automatically know that there is lineage between the schema contents (tables & columns)
    this utility will generate the custom lineage import to create the links
    
    given two schemas - leftSchema and rightSchema
    find the 2 schemas objects in the catalog (GET /2/catalog/data/objects)
    
    for each schema
        execute /2/catalog/data/relationships (2 levels schema->table->column)
            for each table & column - store the id & name (names converted to lower-case for case-insensitive match)
            
    for the stored objects (tables/columns) left side...
        find the same table/column in the right side
        if found - write a custom lineage link to csv
        
    Note:  the custom lineage format used is:-
        Association,From Connection,To Connection,From Object,To Object
        
        where:  From Connection and To Connection will be empty
                Assocition will be either core.DirectionalDataFlow or core.DataSetDataFlow
                the From and To Object will be the full object id
                
        when importing - there is no need for auto connection assignment, since the full id's are provided this happens automatically
        this is possible using v10.2.0 with a patch, and works native in v10.2.1+
        
'''

import platform
import json
import requests
from requests.auth import HTTPBasicAuth
import csv
import edcutils
import time

#***********************************************************************
# change these settings
#***********************************************************************
leftResource = 'ot_oracle'
leftSchema = 'OT'
leftSchemaType = 'com.infa.ldm.relational.Schema'

rightSchema = 'landing'
rightResource = 'landing_hive'
rightSchemaType = 'com.infa.ldm.relational.Schema'
# rightTablePrefix - for cases where the replicated tables have a prefix added (e.g. employee on left ot_employee on right)
rightTablePrefix = ''
#Note: for SAP Hana DB - schema type is different:  com.infa.ldm.extended.saphanadatabase.Schema

catalogServer='http://napslxapp01:9085'
uid='Administrator'
pwd='Administrator'
'''
# if using base64 encoded credentials - you don't need uid/pwd
# you can just use authCredentials- with the correct value
# comment lines with uid/pwd and uncomment the same line using only header (references to authCredentials)
'''
#authCredentials="Basic QWRtaW5pc3RyYXRvcjpBZG1pbmlzdHJhdG9y"

# format the csv file name for custom lineage
csvPrefix="schemaLineage"
csvFolder="out"
#***********************************************************************
# end of settings that should be changed 
#***********************************************************************

# set the csv fileName
csvFileName = csvFolder + "/" + csvPrefix + "_" + leftSchema.lower() + "_" + rightSchema.lower()  + ".csv"


# function - process a single schema - get columns into memory (dict)
def getSchemaContents(schemaName, schemaType, resourceName):
    """
    given a schema name, schema class type (e.g. hanadb is different) and resource name
    find the schema object
    then
        execute a relationships call to get the schema tables & columns (parent/child links)
        note:  some models separate primary key columns from regular columns
        note:  some models have different relationships (e.g. sap hana db)
        
    returns a dictionary of all tables & columns for the schema & the id of the schema object
    key=table  val=tableid
    key=table.column  val=columnid
    """
    print('\tgetSchemaContents for:' + schemaName+ " resource=" + resourceName)
    # schemaDict returned  key=TABLE.COLUMN value=column id
    schemaDict = {}
    tableNames = {}
    
    url = catalogServer + '/access/2/catalog/data/objects'
    query = "core.resourceName:" + resourceName + " and core.classType:" + schemaType + " and core.name_lc_exact:" + schemaName
    parameters = {'q': query, 'offset': 0, 'pageSize': 1}
    # header using uid/pwd (no Authorization)
    header = {"Accept": "application/json"}
    # header using Authorization - no need to use uid/pwd in the get call
    #header = {"Accept": "application/json", "Authorization": authCredentials}
    
    print("\tquery=" + query)
    #print("\theader=" + str(header))
    
    schemaId = None
    tableCount = 0
    columnCount = 0
    # make the call to find the schema object
    response = requests.get(url,params=parameters,headers=header, auth=HTTPBasicAuth(uid,pwd))
    #response = requests.get(url,params=parameters,headers=header)
    rc = response.status_code
    if rc!=200:
        print ("error reading object: rc=" + str(rc) + " response:" + str(response.json) ) 
        if rc==401:
            print("\t401:Possible Missing/bad credentials - or server not found/responding")
            print(str(response))
        return

    # get the total # of objects returned (first part of the json resultset)
    totalObjects=response.json()['metadata']['totalCount'] 
    print("\tobjects returned: " + str(totalObjects) )
    
    for item in response.json()['items']:
        schemaId=item["id"]
        schemaName = edcutils.getFactValue(item, 'core.name')
        # get the tables & columns
        print ("\tfound schema: " + schemaName + " id=" + schemaId)
        
        lineageURL=catalogServer + '/access/2/catalog/data/relationships'
        lineageParms={"seed": schemaId
              , "association": "core.ParentChild"
              , "depth": "2"
              , "direction": "OUT"
              , "includeAttribute": {'core.name', 'core.classType'}
              , "includeTerms": "false"
              , "removeDuplicateAggregateLinks": "false"
              }
        print("\tGET child rels for schema: " + lineageURL + " parms=" + str(lineageParms) )
        # get using uid/pwd
        lineageResp = requests.get(lineageURL,params=lineageParms,headers=header, auth=HTTPBasicAuth(uid,pwd))
        # credentials are in the header
        #lineageResp = requests.get(lineageURL,params=lineageParms,headers=header)
        lineageStatus = lineageResp.status_code
        print("\tlineage resp=" + str(lineageStatus))
        if lineageStatus != 200:
            print ("error getting schema contents (tables) rc=" + str(rc) + " response:" + str(response.json) ) 
            if rc==401:
                print("\t401:Possible Missing/bad credentials - or server not found/responding")
                print(str(response))
            return

        if lineageResp.text.startswith('{items:'):
            # bug (10.2.0 & 10.2.1) - the items collection should be "items"
            lineageJson = lineageResp.text.replace('items', '"items"', 1)
        else:
            lineageJson = lineageResp.text
        #relsJson = json.loads(lineageJson.replace('items', '"items"'))
        relsJson = json.loads(lineageJson)
        #print(len(relsJson))
        
        for lineageItem in relsJson["items"]:
            #print('\t\t' + str(lineageItem))
            inId = lineageItem.get("inId")
            outId = lineageItem.get("outId")
            
            #print('new inId===' + inId + " outId=" + outId)
            #print(edcutils.getFactValue(lineageItem["inEmbedded"], "core.name"))
            assocId = lineageItem.get("associationId")
            #print("\t\t" + inId + " assoc=" + assocId)
            #if assocId=='com.infa.ldm.relational.SchemaTable':
            if assocId.endswith('.SchemaTable'):
                # note - custom lineage does not need table and column - count the tables & store table names
                tableCount += 1
                #tableName = inId.split('/')[-1]
                tableName = edcutils.getFactValue(lineageItem["inEmbedded"], "core.name").lower()
                # store the table name (for lookup when processing the columns) key-id, val=name
                tableNames[inId] = tableName
                schemaDict[tableName]=inId
            #if assocId=='com.infa.ldm.relational.TableColumn':
            if assocId.endswith('.TableColumn') or assocId.endswith(".TablePrimaryKeyColumn"):
                #columnName = inId.split('/')[-1]
                columnCount += 1
                columnName = edcutils.getFactValue(lineageItem["inEmbedded"], "core.name").lower()
                tableName = tableNames[outId].lower()
                #print("column=" + tableName + "." + columnName)
                schemaDict[tableName+"."+columnName] = inId
    
    print("\tgetSchema: returning " + str(columnCount) + " columns, in " + str(tableCount) + " tables")  
    return schemaDict, schemaId


def main():
    """ 
    initialise the csv file(s) to write
    call getSchemaContents for both left and right schema objects
    match the tables/columns from the left schema to the right
    when matched
        write a lineage link - table and column level
    
    Note:  this script generates the newer lineage format using complete object id's and relationship types
           connection assignment will not be necessary
           works with v10.2.1+
           
    """
    start_time = time.time()
    print ("dbSchemaReplicationLineage:start")
    print ("Catalog=" + catalogServer)
    print ("left:  resource=" + leftResource)
    print ("left:    schema=" + leftSchema)
    print ("right: resource=" + rightResource)
    print ("right:   schema=" + rightSchema)
        
    # initialize csv output file
    columnHeader=["Association","From Connection","To Connection","From Object","To Object"]

    # python 3 & 2.7 use different methods
    print("initializing file: " + csvFileName)
    if str(platform.python_version()).startswith("2.7"):
        fCSVFile = open(csvFileName,"w")
    else:
        fCSVFile = open(csvFileName,"w", newline='', encoding='utf-8')
    colWriter=csv.writer(fCSVFile)
    colWriter.writerow(columnHeader)   
   
    # get the objects from the left schema into memory
    print("get left schema: name=" + leftSchema + " resource=" + leftResource + " type=" + leftSchemaType)
    leftObjects, leftSchemaId = getSchemaContents(leftSchema, leftSchemaType, leftResource)
    
    # get the objects from the right schema into memory
    print("get left schema: name=" + rightSchema + " resource=" + rightResource + " type=" + rightSchemaType)
    rightObjects, rightSchemaId = getSchemaContents(rightSchema, rightSchemaType, rightResource)

    matches=0
    missing=0

    if len(leftObjects) > 0 and len(rightObjects) > 0:        
        # create the lineage file    
        colWriter.writerow(["core.DataSourceDataFlow","","",leftSchemaId,rightSchemaId])
        # iterate over all left objects - looking for matching right ones
        print("\nprocessing: " + str(len(leftObjects)) + " objects (left side)")
        for leftName, leftVal in leftObjects.items():
            # if the target is using a prefix - add it to leftName
            if len(rightTablePrefix)>0:
                leftName = rightTablePrefix.lower() + leftName

            #print("key=" + leftName + " " + leftVal + " " + str(leftName.count('.')))
            if (leftName in rightObjects.keys()):
                # match
                rightVal = rightObjects.get(leftName)
                matches += 1
                #print("\t" + rightVal)
                # check if it is formatted as table.column or just table
                if leftName.count('.') == 1:
                    # column lineage - using DirectionalDataFlow
                    colWriter.writerow(["core.DirectionalDataFlow","","",leftVal,rightVal])
                else:
                    # table level - using core.DataSetDataFlow
                    colWriter.writerow(["core.DataSetDataFlow","","",leftVal,rightVal])

                # write a line to the custom lineage csv file (connection assignment)
                #colWriter.writerow([leftResource,rightResource,leftRef,rightRef])
            else:
                missing += 1
                print("\t no match on right side for key=" + leftName)

    else:
        print("error getting schema info... - no linking/lineage created")

    print ("dbSchemaLineageGen:finished. " + str(matches) + " links created, " + str(missing) + " missing (found in left, no match on right)")
    print("run time = %s seconds ---" % (time.time() - start_time))

    fCSVFile.close()


# call main - if not already called or used by another script 
if __name__== "__main__":
    main()            
