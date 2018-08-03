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

Sample Programs in the Project
------------------------------

* EDCQuery_template.py:  this progam is a template/skeleton that shows how to connect to the catlog and execute a search in python.  the result-set processing includes handling the paging model.  It also uses the `getFactValue` method in `edcutils.py` to extract the item name from the facts array
* edcutils.py:  utility/helper methods for common tasks - like get an attribute value `getFactValue(item, attrName)`

