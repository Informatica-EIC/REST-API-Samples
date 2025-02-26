# notes for implemenation of db sceham custom attribute populateion

purpose

provide a way to populate a custom attribute for tables/viiews/columns with the owning schema name.
since EDC does not provide a search via hierarchy )like find tables in schema x), request as come in to enable a searchable/facteable property to score schema name.
it would be a large change for all relational scanners to add this & a script could be easily crated and run periodically.



# initial search

get a count of all objects that could have custom attibute contents.

Note - all edc search/activity can be done using search api (index), which should provide fast results

- search (q property)  `core.classType:(com.infa.ldm.relational.Table com.infa.ldm.relational.View com.infa.ldm.relational.Column com.infa.ldm.relational.ViewColumn)`
    - fq - `-com.infa.appmodels.ldm.LDM_cc7ace0e_b460_475f_882d_104aa88665ab:[* TO *]`
- facet on  (to get counts for script start)
  - core.resourceType
  - core.resourceName
  - core.classType (coount for individual objects that should be updated)

# step 2 - search each resoure

- for each resource found in search above
  - search for schema objects (get id/name and store in dict).   may be storing more objects than needed, but should be fast (all index)
  - search for objects in the resource that do not have custom attribute value
    - for each object
      - get the id of the schema (first n / characters) & lookup schema name 
      - write a line to bulk import file, with id, name, custom attr value for schema

    batch processing - after n rows - call bulk import & initialize next file to import

# bulk import

call bulk import (in stages) with populated objects
file(s) stored in folder, can be used for reference for what was updated


if process ends/terminated - can always be re-started because query will find objects remaining


# parameters

- parameter file should be able to store values used for search
  - classtypes  space seperated list of classtypes to add/search for custom attribute values
  - custom_attribute_id id for the custom attribute (we need to id & name for bulk import & id for search)
  - exclude_resources any resources to bypass (will add an fq with -core.resourceName:(`<items>`))
  - include_resources filter for initial search (may be useful for testing, or if there are a lot of large resources)
  - folder to write .csv files (keep a record of bulk upload activities)
- command-line switch for test mode?   show what it would do, without actually doing anything

python dot_env already in use, so should be able to leverage that & provide different configurations options for flexibility


sample initial search

```
https://asvdwsndboxrh01.informatica.com:9085/access/2/catalog/data/search?defaultFacets=false&disableSemanticSearch=false&enableLegacySearch=false&facet=true&facetId=core.resourceType&facetId=core.resourceName&facetId=core.classType&fq=-com.infa.appmodels.ldm.LDM_cc7ace0e_b460_475f_882d_104aa88665ab%3A%5B*%20TO%20*%5D&highlight=false&includeRefObjects=false&offset=0&pageSize=1&q=core.classType%3A(com.infa.ldm.relational.Table%20com.infa.ldm.relational.View%20com.infa.ldm.relational.Column%20com.infa.ldm.relational.ViewColumn)
```

https://asvdwsndboxrh01.informatica.com:9085/access/2/catalog/data/search?
  
- defaultFacets=false 
- disableSemanticSearch=false
- enableLegacySearch=false
- facet=true
- facetId=core.resourceType
- facetId=core.resourceName
- facetId=core.classType
- fq=-com.infa.appmodels.ldm.LDM_cc7ace0e_b460_475f_882d_104aa88665ab:[* TO *]
- highlight=false
- includeRefObjects=false
- offset=0
- pageSize=1
- q=core.classType:(com.infa.ldm.relational.Table com.infa.ldm.relational.View com.infa.ldm.relational.Column com.infa.ldm.relational.ViewColumn)


search for schema objects

```
https://asvdwsndboxrh01.informatica.com:9085/access/2/catalog/data/search?defaultFacets=true&disableSemanticSearch=false&enableLegacySearch=false&facet=true&fl=core.name&highlight=false&includeRefObjects=false&offset=0&pageSize=1000&q=core.classType_lc_exact%3A(com.infa.ldm.relational.Schema)%20AND%20core.resourceName%3A(%22SQLServer_TestDB%22%20%22WideWorldImporters_SQLServer%22%20%22Oracle23_ai_test%22%20)
```

https://asvdwsndboxrh01.informatica.com:9085/access/2/catalog/data/search?
- defaultFacets=true
- disableSemanticSearch=false
- enableLegacySearch=false
- facet=true
- fl=core.name
- highlight=false
- includeRefObjects=false
- offset=0
- pageSize=1000
- q=core.classType_lc_exact%3A(com.infa.ldm.relational.Schema)%20AND%20core.resourceName%3A(%22SQLServer_TestDB%22%20%22WideWorldImporters_SQLServer%22%20%22Oracle23_ai_test%22%20)
