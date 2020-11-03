"""
    File name: InformaticaAPI.py
    Author: Cameron Wonchoba
    Date created: 10/6/2020/
    Date last modified: 11/3/2020
    Python Version: 3.6+
"""

import copy
import pandas as  pd
import os
import re

from Connection import Connection

class InformaticaAPI(Connection):
    """
    A class used to run common API requests on EDC. Extends the Connection class

    ...
    
    Attributes
    ----------
    None

    Methods
    -------
    encodeID(id, tilde=True)
        Encode an ID to be safe. Return String
    getAssociations(associationIdLst=[])
        Get all associations. Return a JSON object
    getObject(objectID)
        Get details about a specific object. Return a JSON object
    getRelationships(self, objectIdLst, associationIdLst, depth=0, direction="BOTH")
        Get relationships that traverse a set of associationID(s). Return a JSON object
    search(query, offset)
        Search for objects based on query.
    """
    def __init__(self, securityDomain=None, userName=None, password=None, catalogService=None, verbose=False):
        """Constructor - Set-up connection using Parent Class"""
        # Call super class
        super().__init__(securityDomain, userName, password, catalogService, verbose)

    def encodeID(self, id, tilde=False):
        """
        Encode an ID to be safe. Return String.
        
        Parameters
        ----------
        id : String 
            ID of object
        tilde : Boolean, optional (default=True)
            Whether to encode with a tilde or percent sign. 
        """
        # Replace three underscores with two backslashes
        if ":___" in id:
           id = id.replace(":___", "://")
        
        # Get REGEX set-up
        regex = re.compile('([^a-zA-Z0-9-_])')
        match_obj = regex.search(id)

        # Extract indicies of unsafe chars
        indicies = match_obj.span()

        # Intialize a few variables
        id_lst = list(id)
        idx = 0

        # Replace each unsafe char with "~Hex(Byte(unsafe char))~"
        while regex.search(id, idx) is not None:
            idx = regex.search(id,idx).span()[1]
            if tilde:
                id_lst[idx-1] = "~" + str(bytes(id_lst[idx-1], 'utf-8').hex()) + "~"
            else:
                id_lst[idx-1] = "%" + str(bytes(id_lst[idx-1], 'utf-8').hex())
                
        return "".join(id_lst)
    
    def getAssociations(self, associationIdLst=[]):
        """
        Get all associations. Return a JSON object
        
        Parameters
        ----------
        associationIdLst : List, optional (default=[])
            List of associationIDs
        """
        url = f"{self.catalogService}/access/2/catalog/models/associations?{id}offset=0&pageSize=100000".format(
            id="id="+"&id=".join(associationIdLst) + "&" if associationIdLst else ""
        )
        return Connection.getResponseJSON(self, url)
    
    def getObject(self, objectID):
        """
        Get details about a specific object. Return a JSON object
        
        Parameters
        ----------
        objectID : String 
            The objectID. This can be found in the URL when viewing a particular object
            e.g.: <catalogService>/ldmcatalog/main/ldmObjectView/('$obj':'<objectID>','$type':com.infa.ldm.relational.Table,'$where':ldm.ThreeSixtyView)
            The objectID <objectID>
        """
        # Convert objectID to safe encoding
        objectID = self.encodeID(objectID, tilde=True)

        url = f"{self.catalogService}/access/2/catalog/data/objects/{objectID}?includeRefObjects=true"
        return Connection.getResponseJSON(self, url)

    def search(self, query, offset=0):
        """Search for objects based on query
        
        Parameters
        ----------
        query: String
            The query to pass the search functionality
        offset: int, optional (default=0)
            How many items to offset the results by
        """
        url = f"{self.catalogService}/access/2/catalog/data/search?q={query}&facet=false&defaultFacets=true&highlight=false&offset={offset}&pageSize=5&enableLegacySearch=false&disableSemanticSearch=false&includeRefObjects=false"
        return Connection.getResponseJSON(self, url)
  
    def getRelationships(self, objectIdLst, associationIdLst, depth=0, direction="BOTH"):
        """
        Get relationships that traverse a set of associationID(s). Return a JSON object
        
        Parameters
        ----------
        objectIdLst : List 
            List of ObjectIDs. This can be found in the URL when viewing a particular object
            e.g.: <catalogService>/ldmcatalog/main/ldmObjectView/('$obj':'<objectID>','$type':com.infa.ldm.relational.Table,'$where':ldm.ThreeSixtyView)
            The objectID <objectID>
        associationIdLst : List
            The association to traverse. Can be either an association or association kind
        depth : int, optional (default=0)
            Depth of traversal. 0 means traverse fully
        direction : String, optional (default="BOTH")
            Specify the direction of data flow be it IN or OUT
        """
        # Convert objectID to safe encoding
        objectIdLst = [self.encodeID(objectID, False) for objectID in objectIdLst]
        url = "{catService}/access/2/catalog/data/relationships?seed={o}&association={a}&depth={d}&direction={dir}&removeDuplicateAggregateLinks=true&includeTerms=true&includeRefObjects=true".format(
            catService=self.catalogService,
            o="&seed=".join(objectIdLst), 
            a="&association=".join(associationIdLst), 
            d=depth,
            dir=direction)
        return Connection.getResponseJSON(self, url)

    def updateObject(self, objectID, data):
        """
        Updates an object with the data

        Parameters
        ----------

        objectID : String
            ObjectID to modify
        data : dictionary
            Data to put into the object
        """
        if isinstance(data, dict):
            objectID = self.encodeID(objectID, tilde=True)

            url = f"{self.catalogService}/access/2/catalog/data/objects/{objectID}"
            Connection.putRequest(self, url, data)
        else:
            print("[ERROR] - Unable to put data. Data must be in a dictionary format")
    
if __name__ == "__main__":
    """Testing APIRequest - View Examples folder for more examples"""
    InformaticaAPIObj = InformaticaAPI(verbose=True)
    print("View Examples folder for examples")
