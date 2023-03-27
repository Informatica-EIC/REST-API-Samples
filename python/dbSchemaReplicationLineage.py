"""
Created on Jun 26, 2018

@author: dwrigley
***************************************************************************************
DB schema replication custom lineage generator

process:-
    scenario:  the tables in 2 different schemas
    (perhaps in different databases/resources) are replicated
    in EDC - we have no way to automatically know that there is lineage
    between the schema contents (tables & columns)
    this utility will generate the custom lineage import to create the links
    it will create
    - core.DataSourceDataFlow links from schema to schema (or equivalent)
    - core.DataSetDataFlow links from table to table (or equivalent)
    - core.DirectionalDataFlow links from column to column (or equivalent)

    given two schemas (datasource instances) - leftSchema and rightSchema
    find the 2 schemas objects in the catalog (GET /2/catalog/data/objects)

    for each schema
        execute /2/catalog/data/relationships (2 levels schema->table->column)
            for each table & column - store the id & name (names converted to
            lower-case for case-insensitive match)

    for the stored objects (tables/columns) left side...
        find the same table/column in the right side
        if found - write a custom lineage link to csv

    Note:  the custom lineage format used is:-
        Association,From Connection,To Connection,From Object,To Object

        where:  From Connection and To Connection will be empty
                Assocition will be either core.DirectionalDataFlow
                or core.DataSetDataFlow
                the From and To Object will be the full object id

        when importing - there is no need for auto connection assignment,
        since the full id's are provided this happens automatically
        this is possible using v10.2.0 with a patch,
        and works native in v10.2.1+
"""

import argparse
import json

# import requests
# from requests.auth import HTTPBasicAuth
import urllib3
import csv
import edcutils
import time
import edcSessionHelper

# set edc helper session + variables (easy/re-useable connection to edc api)
edcHelper = edcSessionHelper.EDCSession()

# disable ssl cert warnings, when using self-signed certificate
urllib3.disable_warnings()

# define script command-line parameters (in global scope for gooey/wooey)
parser = argparse.ArgumentParser(parents=[edcHelper.argparser])
# add args specific to this utility (left/right resource, schema, classtype...)
parser.add_argument(
    "-lr",
    "--leftresource",
    required=False,
    help="name of the left resource to find objects",
)
parser.add_argument(
    "-ls",
    "--leftschema",
    required=False,
    help="name of the left schema/container object",
)
parser.add_argument(
    "-lt",
    "--lefttype",
    required=False,
    default="com.infa.ldm.relational.Schema",
    help="class type for the schema level object",
)
parser.add_argument(
    "-rr",
    "--rightresource",
    required=False,
    help="name of the right resource to find objects",
)
parser.add_argument(
    "-rs",
    "--rightschema",
    required=False,
    help="name of the right schema/container object",
)
parser.add_argument(
    "-rt",
    "--righttype",
    required=False,
    default="com.infa.ldm.relational.Schema",
    help="class type for the right schema level object",
)
parser.add_argument(
    "-pfx",
    "--csvprefix",
    required=False,
    default="schemaLineage",
    help="prefix to use when creating the output csv file",
)
parser.add_argument(
    "-rtp",
    "--righttableprefix",
    required=False,
    default="",
    help="table prefix for right datasets",
)
parser.add_argument(
    "-sub",
    "--substitute",
    required=False,
    help="characters to replace, from/to split by '/' and comma seperated for multiple substitutions - e.g. _a/_b,#/_h  replace any _a string to _b, and any # to _h in the columns to link",
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


def getSchemaContents(schemaName, schemaType, resourceName, substitute_chars):
    """
    given a schema name, schema class type (e.g. hanadb is different)
    and resource name, find the schema object
    then
        execute a relationships call to get the schema tables & columns
        (parent/child links)
        note:  some models separate primary key columns from regular columns
        note:  some models have different relationships (e.g. sap hana db)

    returns a dictionary of all tables & columns for the schema & the id of
    the schema object
    key=table  val=tableid
    key=table.column  val=columnid
    """
    print("\tgetSchemaContents for:" + schemaName + " resource=" + resourceName)
    # schemaDict returned  key=TABLE.COLUMN value=column id
    schemaDict = {}

    # url = catalogServer + "/access/2/catalog/data/objects"
    url = edcHelper.baseUrl + "/access/2/catalog/data/objects"
    query = (
        f'+core.resourceName:"{resourceName}"'
        + f' +core.classType:"{schemaType}"'
        + f' +core.name:"{schemaName}"'
    )
    parameters = {"q": query, "offset": 0, "pageSize": 1}
    print("\tquery=" + query)

    schemaId = None
    tableCount = 0
    columnCount = 0
    # make the call to find the schema object
    response = edcHelper.session.get(url, params=parameters, timeout=3)
    print(f"session get finished: {response.status_code}")
    rc = response.status_code

    if rc != 200:
        print("error reading object: rc=" + str(rc) + " response:" + str(response.json))
        if rc == 401:
            print("\t401:Possible Missing/bad credentials")
            print(str(response))
        return

    # get the total # of objects returned (first part of the json resultset)
    totalObjects = response.json()["metadata"]["totalCount"]
    print("\tobjects returned: " + str(totalObjects))

    for item in response.json()["items"]:
        schemaId = item["id"]
        schemaNameFound = edcutils.getFactValue(item, "core.name")
        # get the tables & columns

        # check to see if schemaName found is an exact match
        # (for situations where there are other schemas with the same prefix)
        # e.g. search for "PUBLIC" will also return "PUBLIC_TEST"
        print(
            "\tfound schema: "
            + schemaNameFound
            + " id="
            + schemaId
            + " type="
            + schemaType
        )
        if schemaNameFound != schemaName:
            print(
                f"schema {schemaNameFound} does not exactly match {schemaName}, skipping"
            )
            continue

        lineageURL = edcHelper.baseUrl + "/access/2/catalog/data/relationships"
        depth = 2
        # for SAP Hana Calculation views - the package might contain sub-packages
        # set the depth to >2
        if schemaType == "com.infa.ldm.relational.SAPHanaPackage":
            depth = 10
            print(
                f"\tNote:  SAP Hana Package used for datasource (schema)\n\t\tsetting depth={depth} for relationships query"
            )
        lineageParms = {
            "seed": schemaId,
            "association": "core.ParentChild",
            "depth": depth,
            "direction": "OUT",
            "includeAttribute": {"core.name", "core.classType"},
            "includeTerms": "false",
            "removeDuplicateAggregateLinks": "false",
        }

        print(
            "\tGET child rels for schema: " + lineageURL + " parms=" + str(lineageParms)
        )
        # get using uid/pwd
        lineageResp = edcHelper.session.get(
            lineageURL,
            params=lineageParms,
        )
        lineageStatus = lineageResp.status_code
        print("\tlineage resp=" + str(lineageStatus))
        if lineageStatus != 200:
            print(
                f"error getting schema contents (tables) rc={rc}"
                f" response:{response.json}"
            )
            if rc == 401:
                print("\t401:Possible Missing/bad credentials")
                print(str(response))
            return

        if lineageResp.text.startswith("{items:"):
            # bug (10.2.0 & 10.2.1) - the items collection should be "items"
            lineageJson = lineageResp.text.replace("items", '"items"', 1)
        else:
            lineageJson = lineageResp.text
        # relsJson = json.loads(lineageJson.replace('items', '"items"'))
        relsJson = json.loads(lineageJson)
        # print(len(relsJson))

        for lineageItem in relsJson["items"]:
            # print('\t\t' + str(lineageItem))
            inId = lineageItem.get("inId")
            outId = lineageItem.get("outId")

            # print('new inId===' + inId + " outId=" + outId)
            # print(edcutils.getFactValue(lineageItem["inEmbedded"], "core.name"))
            assocId = lineageItem.get("associationId")
            # print("\t\t" + inId + " assoc=" + assocId)
            # if assocId=='com.infa.ldm.relational.SchemaTable':
            if (
                assocId.endswith(".SchemaTable")
                or assocId == "com.infa.adapter.snowflake.PackageFlatRecord_table"
                or assocId == "com.infa.ldm.relational.SAPHanaPackageCalculationView"
                or assocId == "core.DataSourceDataSets"
            ):
                # note - custom lineage does not need table and column
                # count the tables & store table names
                tableCount += 1
                # tableName = inId.split('/')[-1]
                # tableName = edcutils.getFactValue(
                #     lineageItem["inEmbedded"], "core.name"
                # ).lower()
                # store the table name (for lookup when processing the columns)
                # key=id, val=name
                # tableNames[inId] = tableName
                # schemaDict[tableName] = inId
            # if assocId=='com.infa.ldm.relational.TableColumn':
            if (
                assocId.endswith(".TableColumn")
                or assocId.endswith(".TablePrimaryKeyColumn")
                or assocId == "com.infa.adapter.snowflake.FlatRecord_tableField"
                or assocId == "com.infa.ldm.relational.CalculationViewAttribute"
                or assocId.startswith("com.infa.ldm.mdm.")
            ):
                # columnName = inId.split('/')[-1]
                columnCount += 1
                columnName = edcutils.getFactValue(
                    lineageItem["inEmbedded"], "core.name"
                ).lower()
                # check if key exists??  possible bug or different order from relationships

                # check for substitutions
                columnName = (substitute_name(columnName, substitute_chars)).lower()
                # get table name from id (split and get the parent - won't work if a table / in the name)
                tableName = outId.split("/")[-1].lower()
                # print(f"table::: {outId}")
                # print(outId.split("/"))
                # tableName = tableNames[outId].lower()
                # print("column=" + tableName + "." + columnName)
                schemaDict[tableName + "." + columnName] = inId
                schemaDict[tableName] = outId

    print(
        "\tgetSchema: returning "
        + str(columnCount)
        + " columns, in "
        + str(tableCount)
        + " tables"
    )
    return schemaDict, schemaId


def substitute_name(from_name: str, subst: str) -> str:
    # split the substituions by ,
    new_str = from_name
    if subst is None:
        return from_name
    if "/" not in subst:
        return from_name

    substutions = subst.split(",")
    for subst_instance in substutions:
        # split the subst string into from/to
        from_str = subst_instance.strip().split("/")[0]
        if from_str in from_name:
            to_str = subst_instance.strip().split("/")[1]
            new_str = from_name.replace(from_str, to_str)
            print(
                f"substituting {from_str} with {to_str} in {from_name} - new value is {new_str}"
            )
    return new_str


def main():
    """
    initialise the csv file(s) to write
    call getSchemaContents for both left and right schema objects
    match the tables/columns from the left schema to the right
    when matched
        write a lineage link - table and column level

    Note:  this script generates the newer lineage format using complete
           object id's and relationship types
           connection assignment will not be necessary
           works with v10.2.1+

    """
    args = args, unknown = parser.parse_known_args()
    # setup edc session and catalog url - with auth in the session header,
    # by using system vars or command-line args
    edcHelper.initUrlAndSessionFromEDCSettings()

    print(f"command-line args parsed = {args} ")

    start_time = time.time()

    print("dbSchemaReplicationLineage:start")
    print(f"Catalog={edcHelper.baseUrl}")
    print(f"left:  resource={args.leftresource}")
    print(f"left:    schema={args.leftschema}")
    print(f"left:      type={args.lefttype}")
    print(f"right:  resource={args.rightresource}")
    print(f"right:    schema={args.rightschema}")
    print(f"right:      type={args.righttype}")
    print(f"output folder:{args.outDir}")
    print(f"output file prefix:{args.csvprefix}")
    print(f"right table prefix:{args.righttableprefix}")
    if args.substitute is not None:
        print(f"substition from,to: {args.substitute}")

    # initialize csv output file
    columnHeader = [
        "Association",
        "From Connection",
        "To Connection",
        "From Object",
        "To Object",
    ]

    # set the csv fileName
    csvFileName = (
        f"{args.outDir}/{args.csvprefix}_{args.leftschema.lower()}"
        f"_{args.rightschema.lower()}.csv"
    )
    # python 3 & 2.7 use different methods
    print("initializing file: " + csvFileName)
    fCSVFile = open(csvFileName, "w", newline="", encoding="utf-8")
    colWriter = csv.writer(fCSVFile)
    colWriter.writerow(columnHeader)

    # get the objects from the left schema into memory
    print(
        f"get left schema: name={args.leftschema}"
        f" resource={args.leftresource}"
        f" type={args.lefttype}"
    )
    leftObjects, leftSchemaId = getSchemaContents(
        args.leftschema, args.lefttype, args.leftresource, args.substitute
    )

    # get the objects from the right schema into memory
    print(
        f"get right schema: name={args.rightschema}"
        f" resource={args.rightresource}"
        f" type={args.righttype}"
    )
    rightObjects, rightSchemaId = getSchemaContents(
        args.rightschema, args.righttype, args.rightresource, args.substitute
    )

    matches = 0
    missing = 0

    if len(leftObjects) > 0 and len(rightObjects) > 0:
        # create the lineage file
        colWriter.writerow(
            ["core.DataSourceDataFlow", "", "", leftSchemaId, rightSchemaId]
        )
        # iterate over all left objects - looking for matching right ones
        print("\nprocessing: " + str(len(leftObjects)) + " objects (left side)")
        for leftName, leftVal in leftObjects.items():
            # if the target is using a prefix - add it to leftName
            if len(args.righttableprefix) > 0:
                leftName = args.righttableprefix.lower() + leftName

            # print("key=" + leftName + " " + leftVal + " " + str(leftName.count('.')))
            if leftName in rightObjects.keys():
                # match
                rightVal = rightObjects.get(leftName)
                matches += 1
                # print("\t" + rightVal)
                # check if it is formatted as table.column or just table
                if leftName.count(".") == 1:
                    # column lineage - using DirectionalDataFlow
                    colWriter.writerow(
                        ["core.DirectionalDataFlow", "", "", leftVal, rightVal]
                    )
                else:
                    # table level - using core.DataSetDataFlow
                    colWriter.writerow(
                        ["core.DataSetDataFlow", "", "", leftVal, rightVal]
                    )

                # write a line to the custom lineage csv file (connection assignment)
                # colWriter.writerow([leftResource,rightResource,leftRef,rightRef])
            else:
                missing += 1
                print("\t no match on right side for key=" + leftName)

    else:
        print("error getting schema info... - no linking/lineage created")

    print(
        f"dbSchemaLineageGen:finished. {matches} links created, "
        f"{missing} missing (found in left, no match on right)"
    )
    print("run time = %s seconds ---" % (time.time() - start_time))

    fCSVFile.close()


# call main - if not already called or used by another script
if __name__ == "__main__":
    main()
