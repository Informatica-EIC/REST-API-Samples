# EDC rest api samples/utilities using python

contains examples for connecting to and querying EDC via python

Requirements
------------
* python 2.7 (current version on most linux/mac)
* Eclipse - ide for java/python (python using/installing pydev)
  * Download: http://www.eclipse.org/downloads/eclipse-packages/
  
Getting Started
---------------
* verify that python is installed - v2.7
* Create a new Eclipse Project and import/use the files in the python folder (not the java folder)
* Ensure EIC is running while executing the samples
* you may need to install python pakages like requests if you get a message like `ImportError: No module named requests`
  * ```
    sudo pip install requests
    ```

Sample Programs in the Project
------------------------------

* EDCQuery_template.py:  a template/skeleton that shows how to connect to the catalog and execute a search using python.  the result-set processing includes handling the paging model.  It also uses the `getFactValue` method in `edcutils.py` to extract the item name from the facts array
* edcutils.py:  utility/helper methods for common tasks - like get an attribute value `getFactValue(item, attrName)`
* listCustomAttributes: simple script to print all custom attributes (name, id, type, sortable, facetable)
* similarityReport.py: v10.2.1+ utility to find & export all columns/fields with similar links
* encodeUser.py: simple program to prompt for a userid/pwd and optionally a security domain and create a base64 encoded string that can be used for authentication in the http header.  e.g. ```"Authorization": "Basic QWRtaW5pc3RyYXRvcjpBZG1pbmlzdHJhdG9y"```


