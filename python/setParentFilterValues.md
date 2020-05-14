# Bulk update custom attribute to filter by schema

use this script to update in bulk relational objects with a custom attribute containing the value of the schema the object belongs to. This will allow faceting by schema name in search results, as well as creating custom tab pointing to specific database schema within a resource

* create a custom attribute:

![Custom attribute definition](https://github.com/Informatica-EIC/REST-API-Samples/blob/master/img/custom_attribute_parent_definition.png?raw=true)

* execute the script `setParentFilterValues.py`
setup your environment by creating an .env file, with 2 environment variables

  * INFA_EDC_AUTH=Basic {your value} - see `encodeUser.py` to generate the value
  * INFA_EDC_URL=http://{your server}:9085

to execute the script:
```
setParentFilterValues.py -t "Parent Filter" -r sql_dw
```




* configure application for the custom attribute

![Custom attribute app configuration](https://github.com/Informatica-EIC/REST-API-Samples/blob/master/img/custom_attribute_parent_appConfig.png?raw=true)

* configure search tab to use the custom attribute

![search tab](https://github.com/Informatica-EIC/REST-API-Samples/blob/master/img/custom_attribute_parent_searchTab.png?raw=true)

* Search and navigate to the search tab

![Search results](https://github.com/Informatica-EIC/REST-API-Samples/blob/master/img/custom_attribute_parent_searchResults.png?raw=true)
