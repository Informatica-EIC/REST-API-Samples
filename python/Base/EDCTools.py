"""
    File name: EDCTools.py
    Author: Cameron Wonchoba
    Date created: 10/6/2020
    Date last modified: 11/4/2020
    Python Version: 3.6+
"""


from InformaticaAPI import InformaticaAPI

import pandas as pd
import os

class EDCTools(InformaticaAPI):
    """
    A class used to extract a variety of information from EDC
    ...
    
    Attributes
    ----------
    directory - String
        Holds the name of a directory we may write to
            
    Methods
    -------
    searchObject(objectName, objectType, maxHits=250)
        Search for the ID of the objectName
    extractCode(objectID)
        Extract the code for a particular object
    extractDataFlow(objectID, associationID, impact)
        Extract the lineage or impact for a particular data assest. Return a DataFrame object
    extractTransformationLogic(objectID, impact)
        Extract the transformation logic for a particular objectID. Return a DataFrame object
    extractLineage(objectID)
        Extract the Lineage for a particular objectID. Return a DataFrame object
    extractImpact(objectID)
        Extract the Impact for a particular objectID. Return a DataFrame object
    extractCompleteDataFlow(objectID, impact)
        Extract DataFlow and Transformations into one report. Return a Dataframe object
    extractDetails(df_DataFlow)
        Extract the underlying details for a particular dataflow
    extractFarLeftTables(df_dataflow, df_attributes)
        Extract the far left tables (first dependency) from a dataflow
    extractTables(self, df_attributes)
        Extract all table dependencies
    extractColumns(df_attributes)
        Extract column dependenncies
    extractDetail(ID, prefix="")
        Extract attributes for a particular ID
    extractEverything(objectID, impact)
        Extracts All dataflow, transformation logic, table dependencies, and column dependencies
    extractTableColumns(objectID)
        Extract columns from a table
    """

    def __init__(self, securityDomain=None, userName=None, password=None, catalogService=None, verbose=False):
        super().__init__(securityDomain, userName, password, catalogService, verbose)
        self.directory = None

    def searchObject(self, objectName, objectType, maxHits=250):
        """Search for the ID of an object
        
        Parameters
        ----------
        objectName: String
            The name of the object to search for
        objectType : String
            The type of the object to search for
        maxHits : int (Default = 250)
            The maximum number of hits to search through
        """
        offsetCounter = 0
        hits = self.search(objectName, offset=offsetCounter)['hits']
        # Check name is same and we are dealing with the correct type.
        while len(hits) > 0 and offsetCounter <= maxHits: # If we can't find it after 250 hits, stop looking.
            for hit in hits:
                offsetCounter+=1
                nameCheck=False
                classTypeCheck=False
                objectID = hit['id']

                values = hit['values']
                for value in values:
                    if value['attributeId'] == 'core.name' and value['value'].lower() == objectName.lower():
                        nameCheck = True
                    elif value['attributeId'] == 'core.classType' and value['value'] == objectType:
                        classTypeCheck = True

                if nameCheck and classTypeCheck and self.verbose:
                    print("Match found!")
                    print(objectName, " : ", objectID)
                    return objectID # Return objectID

            hits = self.search(objectName, offset=offsetCounter)['hits']

    def extractCode(self, objectID):
        """
        Extract the code for a particular Data Object
        
        Parameters
        ----------
        objectID: String
            The Object ID to extract the code for
        """
        jsonObject = self.getObject(objectID)

        for fact in jsonObject['facts']:
            if fact['attributeId'] == 'com.infa.ldm.relational.Code':
                return fact['value']
        
        print("Code was not found...")
        return None

    def extractDataFlow(self, objectID, associationID, impact):
        """
        Extract the lineage or impact for a particular data assest. Return a DataFrame object
            Parameters
            ----------
            objectID : String 
                ObjectID for a particular data asset. This can be found in the URL when viewing a particular object
            associationID: String
                The association to traverse
            impact : Boolean 
                True if we are extracting impact. False if extracting lineage
        """
        responseJSON = self.getRelationships([objectID], [associationID], depth=0, direction='OUT' if impact else 'IN')

        dataflow = []
        for item in responseJSON['items']:
                dataflow += [[item['outId'], item['inId']]]
            
        return pd.DataFrame(dataflow, columns= ['OUT', 'IN'])

    def extractTransformationLogic(self, objectID, impact):
        """
        Extract the transformation logic for a particular objectID. Return a DataFrame object
        
        Parameters
        ----------
        objectID : String 
            ObjectID for a particular data asset. This can be found in the URL when viewing a particular object
        impact: Boolean
            Whether to extract the impact or Lineage
        """
        return self.extractDataFlow(objectID, "com.infa.ldm.etl.DetailedDataFlow", impact)

    def extractLineage(self, objectID):
        """
        Extract the Lineage for a particular objectID. Return a DataFrame object
        
        Parameters
        ----------
        objectID : String 
            ObjectID for a particular data asset. This can be found in the URL when viewing a particular object
        """
        return self.extractDataFlow(objectID, "core.DataFlow", False)

    def extractImpact(self, objectID):
        """
        Extract the Impact for a particular objectID. Return a DataFrame object
        
        Parameters
        ----------
        objectID : String 
            ObjectID for a particular data asset. This can be found in the URL when viewing a particular object
        """
        return self.extractDataFlow(objectID, "core.DataFlow", True)

    def extractCompleteDataFlow(self, objectID, impact):
        """Extract DataFlow and Transformations into one report. Return a Dataframe object
        
        Parameters
        ----------
        objectID : String 
            ObjectID for a particular data asset. This can be found in the URL when viewing a particular object
        impact: Boolean
            Whether to extract the impact or Lineage
        """
        # Extract DataFlow from a particular data asset
        df_dataflow = self.extractDataFlow(objectID, "core.DataFlow", impact)

        # Extract transformation Logic for each data assest
        df_transformations = pd.DataFrame(columns=['OUT','IN'])

        # When extracing Impact, we want to extract everything that has an "OUT" arrow
        # When extacting Lineage, we want to extract everything that has an "IN" arrow
        uniqueIDs = df_dataflow['OUT'].unique() if impact else df_dataflow['IN'].unique()

        # Transformation Logic extraction only allows for extracting up to another core.DataFlow object.
        # This means that we have to extract transformation logic for each dataflow object.
        for ID in uniqueIDs:
            df_transformations = df_transformations.append(self.extractTransformationLogic(ID, impact), ignore_index=True)

        # Define whether the link is a DATAFLOW or TRANSFORMATION
        df_dataflow['TYPE'] = "DATAFLOW"
        df_transformations['TYPE'] = "TRANSFORMATION"

        # Stitch together the DataFlow and Transformation
        # NOTE: Transformation logic will replace any link that has transformation logic under the hood.
        for idx, row in df_dataflow.iterrows():
            if row['OUT'] not in df_transformations['OUT'].unique(): # check case ID doesn't have any transformation logic
                df_transformations = df_transformations.append(row)
            elif df_dataflow[(df_dataflow['OUT'] == row['OUT']) & (df_dataflow['IN'] == row['OUT'])].shape[0] > 0: # check case when item goes back to itself
                if df_transformations[(df_transformations['OUT'] == row['OUT']) & (df_transformations['IN'] == row['OUT'])].shape[0] <= 0:# Make sure it wasn't already added or accounted for by transformations
                    if df_transformations[(df_transformations['OUT'] == row['OUT']) | (df_transformations['IN'] == row['OUT'])].shape[0] < 2:# check that this isn't accounted for by transformation logic already
                        df_transformations = df_transformations.append(row)
            elif row['OUT'] in df_transformations['OUT'].unique() and row['IN'] not in df_transformations['IN'].unique(): # Check if we are still missing the item.
                df_transformations = df_transformations.append(row)
        
        # Extract details about each 
        df_attributes = self.extractDetails(df_transformations)
            
        return (df_transformations, df_dataflow, df_attributes)
    
    def extractDetail(self, ID, prefix=""):
        """
        Extract attributes for a particular ID
        
        Parameters
        ----------
        ID : String
            The ID of the attribute to parse
        prefix : String (Default = "")
            The prefix to add to the column of the attribute
        """
        rowAttributes = {}
        json_object = self.getObject(ID)
        for fact in json_object['facts']:
            rowAttributes[prefix+fact['attributeId']] = fact['value']
        
        return rowAttributes

    def extractDetails(self, df_DataFlow):
        """
        Extract the underlying details for a particular dataflow
        
        Parameters
        ----------
        df_DataFlow : DataFrame
            This is a DataFrame that tracks the lineage/impact for a particular data object
        """
        # TODO: Make sure the transformation parses the relevant path. It may traverse a bunch of other transformations that aren't neccessary
        # Get unique IDs
        uniqueIDs = pd.unique(df_DataFlow[['OUT','IN']].values.ravel('K')).tolist()

        # Set up link to be embedded in Excel sheet.
        formattedLink=self.catalogService+"/ldmcatalog/main/ldmObjectView/('$obj':'{}','$type':{},'$where':ldm.ThreeSixtyView)"

        # Iterate over all Objects and obtain their attribute
        # Store in a list of dictionaries
        attributes = []
        allAttributes = {}

        for ID in uniqueIDs:
            # Initialize Dictionary to store all facts about the ID.
            rowAttributes = {"ID":ID}
            
            if not ID.startswith("PowerCenter://"): # Parse non-PowerCenter things
                # Extract Column Level info
                if ID not in allAttributes:
                    details = self.extractDetail(ID, "level_2_")
                    rowAttributes.update(details)
                    allAttributes[ID] = details
                else:
                    print("Already Present! Yay", ID)
                    rowAttributes.update(allAttributes[ID])
                
                # Extract Table Level info
                ID = ID.rsplit("/", 1)[0]
                if ID not in allAttributes:
                    details = self.extractDetail(ID, "level_1_")
                    rowAttributes.update(details)
                    allAttributes[ID] = details
                else:
                    print("Already Present! Yay", ID)
                    rowAttributes.update(allAttributes[ID])
                
                # Add a clickable link to view the object
                rowAttributes['Link'] = formattedLink.format(rowAttributes['ID'], rowAttributes['level_2_core.classType'])

            else: # Parse PowerCenter things
                # Extract Top Level info from PowerCenter
                if ID not in allAttributes:
                    details = self.extractDetail(ID, "PowerCenter_4_")
                    rowAttributes.update(details)
                    allAttributes[ID] = details
                else:
                    print("Already Present! Yay", ID)
                    rowAttributes.update(allAttributes[ID])
                
                # Add a clickable link to view the object
                rowAttributes['Link'] = formattedLink.format(rowAttributes['ID'], rowAttributes['PowerCenter_4_core.classType'])

            attributes += [rowAttributes]
        
        return pd.DataFrame(attributes)

    def extractFarLeftTables(self, df_dataflow, df_attributes):
        """Extract the far left tables (first dependency) from a dataflow
        
        Parameters
        ----------
        df_dataflow: DataFrame
            Dataflow mappings that contain IN and OUT information. This is within the context of extracting Lineage and Impact
        df_attributes: DataFrame
            DataFrame that contains detaisl about each Data Object used within the df_dataflow dataframe
        """
        # Search for objects that have an Out arrow, but not an In arrow
        df_farLeftTables = df_dataflow[~df_dataflow['OUT'].isin(df_dataflow['IN'].tolist())]['OUT'].to_frame()
        
        # Merge of extract details
        df_merged = df_farLeftTables.merge(df_attributes, left_on='OUT', right_on='ID')
        
        # Ensure the object is a Table
        # TODO: Will we want to add views here?
        df_merged = df_merged[df_merged['level_1_core.classType'] == "com.infa.ldm.relational.Table"][['level_1_core.name', 'ID']] # Potentially add VIEWS?
        df_merged.columns = ['Table', 'Path']

        # Create a path to the table
        # The definitions are currently column level, but we only need the Table
        for idx, row in df_merged.iterrows():
            df_merged.at[idx, "Path"] = row["Path"].rsplit("/",1)[0]

        # Tables might have had more than one column on the far left, so only keep the far left.
        df_merged = df_merged.drop_duplicates(subset='Path', keep='last')
        return df_merged[['Table', 'Path']]

    def extractTables(self, df_attributes):
        """Extract all table dependencies
        
        Parameters
        ----------
        df_attributes: DataFrame
            DataFrame the contains details about each Data Object within a Lineage or Impact
        """
        # Only actual tables. Not views or temporary tables 
        df = df_attributes[df_attributes['level_1_core.classType'] == "com.infa.ldm.relational.Table"][['level_1_core.name', 'ID']]
        df.columns = ["Table", "Path"]
        for idx, row in df.iterrows():
            df.at[idx, "Path"] = row["Path"].rsplit("/",1)[0]
        
        df = df.drop_duplicates(subset='Path', keep='last')
        return df

    def extractColumns(self, df_attributes):
        """Extract column dependenncies
        
        Parameters
        ----------
        df_attributes: DataFrame
            DataFrame the contains details about each Data Object within a Lineage or Impact
        """
        # Only extracting columns from tables. Not views or temp
        df = df_attributes[df_attributes['level_2_core.classType'] == "com.infa.ldm.relational.Column"][['level_1_core.name', 'level_2_core.name', 'ID']]
        df.columns = ['Table','Column','Path']
        return df

    def extractEverything(self, objectID, impact):
        """
        Extracts All dataflow, transformation logic, table dependencies, and column dependencies
        
        Parameters
        ----------
        objectID : String
            Object ID of object we want to extract all informations for
        impact : Boolean
            True if we are extracting impact. False if extracting lineage
        """
        df_transformations, df_dataflow, df_attributes = self.extractCompleteDataFlow(objectID, impact=impact)
        df_farLeftTables = pd.DataFrame()
        df_tableDependencies = pd.DataFrame()
        df_columnDependencies = pd.DataFrame()
        if df_attributes.shape[0] > 0: 
            df_farLeftTables = self.extractFarLeftTables(df_dataflow, df_attributes)
            df_tableDependencies = self.extractTables(df_attributes)
            df_columnDependencies = self.extractColumns(df_attributes)

        # Name directory
        self.directory = "ExtractedResults"

        # Create directory if it doesn't exist
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        
        # Clear the folder
        filelist = os.listdir(self.directory)
        for f in filelist:
            os.remove(os.path.join(self.directory, f))
            print(f"Removed {os.path.join(self.directory, f)}")

        filename = "Results_" + objectID.replace("/","_").replace(":","__")
        print(f"Writing to {filename}")
        # Write data to Excel Spreadsheet
        with pd.ExcelWriter(self.directory+f"/{filename}.xlsx", options={'strings_to_urls':False}) as writer:
            df_dataflow.to_excel(writer, "Dataflow", index=False)
            df_transformations.to_excel(writer, "Dataflow and Transformations", index=False)
            df_attributes.to_excel(writer, "Details", index=False)
            df_farLeftTables.to_excel(writer, "Far Left Tables", index=False)
            df_tableDependencies.to_excel(writer, "Table Dependencies", index=False)
            df_columnDependencies.to_excel(writer, "Column Dependencies", index=False)
        
        print(f"Written to {self.directory}/{filename}.xlsx")

    def extractTableColumns(self, objectID):
        """
        Extract columns from a table
        
        Parameters
        ----------
        objectID : String
            Object ID of object we want to extract columns for.
        """
        # TODO: We might want to pass in a database..
        response = self.getObject(objectID)

        verifyTable = False
        # Ensure that the object was a table
        if 'facts' in response:
            for fact in response['facts']:
                if fact['attributeId'] == 'core.classType':
                    if fact['value'] == 'com.infa.ldm.relational.Table' or fact['value'] == 'com.infa.ldm.relational.View':
                        verifyTable = True
                    break

            if verifyTable:
                columns = []
                # Extract columns
                for fact in response['dstLinks']:
                    if fact['classType'] == 'com.infa.ldm.relational.Column' or fact['classType'] == 'com.infa.ldm.relational.ViewColumn':
                        columns += [fact['name']]

                return sorted(columns)
        print("[ERROR] - Passed in ID of Object that isn't a table: ", objectID)
        return []

if __name__ == "__main__":
    EDCToolsObj = EDCTools(verbose=True)
    print("View examples folder for examples")  