"""
    File name: Connection.py
    Author: Cameron Wonchoba
    Date created: 10/6/2020
    Date last modified: 11/3/2020
    Python Version: 3.6+
"""

import requests
import re
import base64
import getpass
import json
import sys

import credentials

class Connection:
    """
    A class used to represent and store Connection information and helper functions

    ...
    
    Attributes
    ----------
    session : Request Object
        A session object with the neccessary credentials to access our Catalog service
    verbose : Boolean
        States whether to show details of what is happening behind the scenes
    catalogService : String
        The URL and port of the catalog service

    Methods
    -------
    promptCredentials(securityDomain, userName, password)
        Prompt for credentials. Return tuple
    testConnection()
        Test the connection to EDC
    updateAuthorization(b64AuthStr, authType="Basic")
        Update the authorization header for the session
    getRepsonseJSON(url)
        For a particular URL request, return a reponse JSON object.
    writeJSON(jsonObj, filename)
        Write a JSON document to file
    """

    def __init__(self, securityDomain=None, userName=None, password=None, catalogService=None, verbose=False):
        """
        Constructor - Set-up connection. Return Connection Object
        
        Parameters
        ----------
        securityDomain : String, optional (default=None)
            The Security Domain you use to login into EDC with
        userName : String, optional (default=None)
            User Name for EDC, optional
        password : String (default=None)
            Password for EDC, optional
        """
        self.verbose = verbose
        #Prompt if necessary        
        securityDomain, userName, password = self.promptCredentials(securityDomain, userName, password, catalogService)

        # Encode username and password 
        authorizationStr = f"{securityDomain}\\{userName}:{password}"
        b64AuthStr = base64.b64encode(bytes(authorizationStr, "utf-8"))

        self.session = requests.Session()
        self.session.verify = False
        self.updateAuthorization(b64AuthStr, authType="Basic")
        self.testConnection()
    
    def promptCredentials(self, securityDomain, userName, password, catalogService):
        """
        Prompt for credentials. Return tuple.

        Parameters
        ----------
        securityDomain : String
            The Security Domain you use to login into EDC with
        userName : String
            User Name for EDC
        password : String
            password for EDC
        """
        # Prompt if necessary
        if securityDomain is None and credentials.securityDomain is None:
            securityDomain = input("Security Domain: ")
        else:
            securityDomain = securityDomain or credentials.securityDomain
        if userName is None and credentials.userName is None:
            userName = input("User Name: ")
        else:
            userName = userName or credentials.userName
        if password is None and credentials.password is None:
            password = getpass.getpass(prompt="Password: ")
        else:
            password = password or credentials.password
        if catalogService is None and credentials.catalogService is None:
            self.catalogService = input("Catalog Service: ")
        else:
            self.catalogService = catalogService or credentials.catalogService

        return (securityDomain, userName, password)

    def testConnection(self):
        """Test the connection to EDC"""
        url = f"{self.catalogService}/access"
        print("Testing connection...")
        if self.verbose:
            print("Running Request:\n\t", url)
        try:
            response = self.session.get(url, timeout=5)
        except:
            print("[ERROR] - Something went wrong. Perhaps incorrect Catalog Service?")
            sys.exit()
        print("Response Code: ", response.status_code)
        if response.status_code != 200:
            print("[ERROR]: Invalid Credentials. Try again.")
            self.__init__(verbose=self.verbose)
        else:
            print("Connection Successful!!")

    def updateAuthorization(self, b64AuthStr, authType="Basic"):
        """
        Update the authorization header for the session.

        Parameters
        ----------
        b64AuthStr : String
            Base64 authorization string.The Security Domain you use to login into EDC with.
        authType : String, optional (default="Basic")
            Authorization Type for header.
        """
        self.session.headers.update({"Authorization": f"{authType} {b64AuthStr.decode('utf-8')}"})

    def getResponseJSON(self, url):
        """
        For a particular URL request, return a reponse JSON object.
        
        Parameters
        ----------
        url : String
            Running a get request for the given URL.
        """
        if self.verbose:
            print("Running Request:\n\t", url)
        try:
            response = self.session.get(url, timeout=100)
            return response.json()
        except Exception as e:
            print("[ERROR] - Request failed. Error message: ", e)
            return 
        
    def putRequest(self, url, data):
        """
        Run a "PUT" request for the particular url using the data provided

        Parameters
        ----------
        url : String
            Running a put request for the given URL.
        data : Dictionary
            Data to put into the specfied URL.
        """
        if self.verbose:
            print("Running Request:\n\t", url)
        try:
            response = self.session.put(url, data=data)
            if response.status_code == 200 and self.verbose:
                print("Successfully Put!!!")
            elif response.status_code != 200:
                print("[ERROR] - Put request failed.")
        except Exception as e:
            print("[ERROR] - Request failed. Error message: ", e)
            return 
            
    def writeJSON(self, jsonObj, filename):
        """
        Write a JSON document to file

        Parameters
        ----------
        jsonObj : JSON Object 
            JSON Object that contains data.
        filename : String
            Name of file where the JSON object should be written to.
        """
        if self.verbose:
            print("Writing JSON object to: ", filename)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(jsonObj, f, ensure_ascii=False, indent=4)
        if self.verbose:
            print("Successfully written")

if __name__ == "__main__":
    """Testing code"""
    connectionObj = Connection(verbose=True)
    print("View examples folder for examples")