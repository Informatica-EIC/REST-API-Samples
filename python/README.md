# EDC rest api samples/utilities using python

contains examples for connecting to and querying EDC via python

Requirements
------------
* python 3.6+ - legacy python will not be actively tested
  * why? support for legacy python stops 1/1/2020, fstrings, unicode, async, many other 3rd party libraries like pandas, numpy, django are also not supporting legacy python 
* Note:  some scipts will work with legacy python (v2.7) but these are not maintained or heavily tested
  * any script with a suffix of 27 should work on legacy python systems
  * if you can't easily install python 3.x (e.g 3.7) - you could use docker to run the code in a python3 container
* Python editors (ide/environments)
  * VS Code - good support for python - free an runs on all platforms  https://code.visualstudio.com/
  * pycharm - https://www.jetbrains.com/pycharm/
  * anaconda - for JupterLab/Notebooks https://www.anaconda.com/ (includes vscode)
  * Eclipse - ide for java/python (python using/installing pydev)
    * Download: http://www.eclipse.org/downloads/eclipse-packages/
* other useful tools
  * rest api clients - for testing syntax/api calls + the good ones generate code for many languages
    * postman - https://www.getpostman.com/
    * insomnia - https://insomnia.rest/

 
Getting Started
---------------
* verify that python is installed - v3.6+
* Create a new VSCode/pycharm/Eclipse Project and import/use the files in the python folder (not the java folder)
* Ensure EDC is running while executing the samples - try/except code will catch & immediately exit
* you may need to install python pakages like requests if you get a message like `ImportError: No module named requests`
  * ```
    sudo pip install requests
    ```
* TODO: create a setup document for pyenv and venv


REST API Authentication
-----------------------
* the EDC rest api supports Basic Authentication only - see https://yourcatalogserver:port/access for details
* we use the python requests module for all http(s) rest calls (very easy to use)
* when making a rest api call - you can pass either the id/password - or a http header with an encoded password
* for all examples here, we initially used the id/pwd method - but have switched to use http headers
* if you are using LDAP authentication - the user must have the security domain and a '\' character prefixed to the user id
  * e.g. `COMPANY_LDAP\user_a`
* use the encodeUser.py script - to create the basic auth encoding for your user, and store in a variable named `INFA_EDC_AUTH`
  * you can set the variable for each session, so it is not stored anywhere
  * if using docker - you can add this variable to an .env file to pass to docker at runtime
  * if using VS Code - you can add and "env" setting for individual environment variables used in the debugger (launch.json)
    * e.g  "envFile": "${workspaceFolder}/.env",        // and add any settings to .env (preferred - also works with docker)
    * e.g. "env" : {"INFA_EDC_AUTH" : "Basic dXNlcjE6YUNvbXBsIWNAdGVkUGEkM3cwcmQ="}, (works but prefer .env file)
  * Note:  any files inside of .vscode (e.g. launch.json) will be excluded from the git repo (each user has their own local version)
* TODO:  create a seperate document & recording disucssing authorization techniques (http header, .netrc, auth=)


HTTPS/TLS/SSL Connections and certificates
------------------------------------------
* assuming your catalog service is https enabled (it should be, if not so your passwords are send in clear text & set verify=False)
* you will either need to download/copy the certificate (.pem format, not .jks) locally
  * or set flags to disable certificate authentication (not recommended, but possible) 
* if your ssl certificate is self signed (also not recommended), an additional warning will need to be suppressed
  * more information about SSL authentication can be found [https://3.python-requests.org/user/advanced/#ssl-cert-verification](https://3.python-requests.org/user/advanced/#ssl-cert-verification)

 
Sample Programs in the Project
------------------------------

* `encodeUser.py`: simple program to prompt for a userid/pwd and optionally a security domain and create a base64 encoded string that can be used for authentication in the http header.  e.g. ```"Basic dXNlcjE6YUNvbXBsIWNAdGVkUGEkM3cwcmQ="```
  * use this script before you call use the other scripts, to get the right format for authenticating & not storing passwords in the .py files
    * an alternate is to prompt for a password within your script & encode the id:password
  * use `encodeUser27.py` for legacy python
* `EDCQuery_template.py`:  a template/skeleton that shows how to connect to the catalog and execute a search using python.  the result-set processing includes handling the paging model.  It also uses the `getFactValue` method in `edcutils.py` to extract the item name from the facts array
* `edcutils.py`:  utility/helper methods for common tasks - like get an attribute value `getFactValue(item, attrName)`
* `listCustomAttributes.py`: simple script to print all custom attributes (name, id, type, sortable, facetable)
  * this script will list both regular custom attributes `/2/catalog/models/attributes` and reference 'classification' attributes `/2/catalog/models/referenceAttributes`
* `similarityReport.py`: v10.2.1+ utility to find & export all columns/fields with similar links
* `dbSchemaReplicationLineage.py`: provides the ability to link tables/columns in a database schema that are replicated to other schemas/databases & no scanner exists to automatcially document these relationships.  (e.g. sqoop, scripts/code, goldengate ...)
  * see [dbSchemaReplicationLineage.md](dbSchemaReplicationLineage.md) for more
* `externalDBLinker.py`: script to generate custom lineage for any tables/columns created within an ExternalDatabase/ExternalSchema (often happens with Oracle (dblink) and SQLServer databases (references to databases in views)
  * see [externalDBLinker.md](externalDBLinker.md) for more



