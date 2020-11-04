import sys
sys.path.append("../")
import EDCTools

if __name__ == "__main__":
    """Examples for most methods"""

    EDCToolsObj = EDCTools.EDCTools(verbose=True)

    # Searching for Object
    objectName = "" # TODO: Fill in your objectName here - Search for a table Object
    objectType = "com.infa.ldm.relational.Table"
    objectID = EDCToolsObj.searchObject(objectName, objectType)
    print("Search results:\n\t", objectName, " : ", objectID)

    # Extracting Code from an object
    objectID = "" # TODO Fill in objectID - Probably either a Stored Procedure or View
    code = EDCToolsObj.extractCode(objectID)
    print("Extracted Code: ", code)

    # Extract columns from a table
    objectID = "" # TODO Fill in objectID - Search for a table Object
    columns = EDCToolsObj.extractTableColumns(objectID)
    print("Columns: ", columns)

    # Extracting Lineage for an Object ID
    objectID = "" # TODO: Fill in your objectID here.
    df_lineage = EDCToolsObj.extractLineage(objectID)
    print("First five records of lineage:\n", df_lineage.head())

    # Extracting Impact for an Object ID
    objectID = "" # TODO: Fill in your objectID here.
    df_impact = EDCToolsObj.extractImpact(objectID)
    print("First five records of impact:\n", df_impact.head())

    # Extracting Transformation Logic for a particular Object ID
    # NOTE: Extracting Lineage here. Switch impact to True if you want impact
    objectID = "" # TODO Fill in objectID
    df_transformations = EDCToolsObj.extractTransformationLogic(objectID, impact=False)
    print("Extracted Transformation Logic: ", df_transformations)

    # Extracting Complete Dataflow
    # NOTE: Extracting Lineage here. Switch impact to True if you want impact
    objectID = ""
    df_transformations, df_dataflow, df_attributes = EDCToolsObj.extractCompleteDataFlow(objectID, impact=True)

    # Extracting all details for a particular object
    objectID = "" # TODO Fill in objectID
    details = EDCToolsObj.extractDetail(objectID)
    print("Details: ", details)

    # Extract all details for a particular object and write to excel file
    objectID = "" # TODO Fill in objectID
    EDCToolsObj.extractEverything(objectID, impact=False)