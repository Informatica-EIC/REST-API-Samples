# DB Schema custom attribute population

# Purpose

provide a way to populate a custom attribute for tables/views/columns with the owning schema name.<br/>
since EDC does not provide a search via hierarchy (like find tables in schema x), a request as come in to enable a searchable/facteable property to store the schema name.
it would be a large change for all relational scanners to add this & a script could be easily crated and run periodically.

# out of scope / known limitations

- columns belonging to public synonyms that are contained directly within the database object will show that the schema cannot be found (since there is no schema)

# parameters command-line

- parameter file should be able to store values used for search
  - `--parms_file` (-pf) location of the parameter file used to specify search conditions (see parameter file section)
  - `--outDir` (-o) folder to write .csv files (keep a record of bulk upload activities), default ./bulk
  - `--pagesize` size of the search resultset to process, default 1000 (means 1000 objects will be written to csv for bulk upload)
  - `--queuesize` the number of bulk import jobs to be queued,  process will pause for 2 seconds if the queuesize is reached
  - `--edcimport` (-i) executes the bulk import process, if not specified, will only write csv files (like a test mode)

# parameter file via --parms_file

this process requires the use of a file with additional parameters, via --parms_file command-line setting.
settigns within the file are

- `schema_custom_attr_id`  the id of the custom attribute used to store schema names
- `class_types`  a list (space separated) of the classtypes to add custom attribute values
- `query_filter1` `query_filter2` ... `query_filter5` additional filter(s) for finding objects, can be any valid fq expression, up to 5 additional filters can be used.
  example of usage could be to include/exclude resources (by name or type)


# resource types to use

- any scanner using the standard relaional model 
- bigquery v1 (`com.infa.ldm.google.bigquery.View` `com.infa.ldm.google.bigquery.Table` `com.infa.ldm.google.bigquery.Field`) (v2 uses standard relational model)
- metadex based scanners that support external tables, columns (`com.infa.ldm.AdvancedScanners.PLSQL.ExternalTable` `com.infa.ldm.AdvancedScanners.PLSQL.ExternalColumn`)

# Process Outline

- validate custom attribute passed via `schema_custom_attr_id`, process will also get the Name used for the attribute
- execute a initial search to count objects & resources to process
  - search will use facets searching for `class_types` that do not have `schema_custom_attr_id` values stored in EDC, since facets are used, only 1 object is actually returned (makes the count very fast)
- for each resource from initial search
  - get a list of schema objects (id and names) - the name will be used as the custom attribute value
  - search for objects in the resource (paginated, using `--pagesize`)
    - for each resultset (page of results)
      - create a csv file to store the bulk import values (store file names to submit to bulk import)
        - csv file created is named as `<resource_name>_<timestamp>.csv`
      - iterate over objects found, lookup the schema name and write a line to the csv bulk import
        - export the schema custom attribute for the object
  - when a resource is finished, submit all csv files for bulk import
    - the `--queuesize` setting will be used to limit the jobs in submitted state (to not overload the import process)




# Implementation Notes

to help understand how the process works, the following section includes details about how the edc search is structured.

Note - all edc search/activity can be done using search api (using solr index), which should provide fast results

## step 1 -initial search

get a count of all objects that could have custom attibute contents.

- search (q property) taken from `class_types` value in parameter file  `core.classType:(com.infa.ldm.relational.Table com.infa.ldm.relational.View com.infa.ldm.relational.Column com.infa.ldm.relational.ViewColumn com.infa.ldm.google.bigquery.View com.infa.ldm.google.bigquery.Table com.infa.ldm.google.bigquery.Field com.infa.ldm.AdvancedScanners.PLSQL.ExternalTable com.infa.ldm.AdvancedScanners.PLSQL.ExternalColumn)`
 
    - fq - `-<custom_attribute_id>:[* TO *]`  taken from `schema_custom_attr_id` property in parameter file
    - fq - any additional filters added via parameter file via `query_filter1` ... `query_filter5`  (if filter values are provided)
- facet on  (to get counts for script start)
  - core.resourceType - returns list of allresource types found (with count of objects)
  - core.resourceName - returns list of all resources found (with count of objects)
  - core.classType - returls list of classtypes that should be updated - also includes core.DataSet and core.DataElement supertypes)
- offset=0, pageSize=1 - no need to return actual objects, as we just need the facet counts

a list of resource names is stored, and used in the following steps (processing occurs a resource at a time)

## 

# step 2 - search each resource for objects to update

- for each resource found in search above
  - search for schema objects (get id/name and store in dict)
  - search for objects in the resource that do not have custom attribute value
    - uses a similar approach taken for the initial search, however filters on resource id :*.<br/>   Example.  if a resource is named `WideWorldImporters_SQLServer`  then the q property for search will be formatted as `id:WideWorldImporters_SQLServer\:*`.
      - the reason for this, is in case there are other resources with a suffix (like WideWorldImporters_SQLServer_Test), <br/>
        we want to ensure that only the resource found is searched.<br/> 
        the : character needs to be escaped.<br/>
        this will search for all objects within the resource and other query filters are added to filter on class types and the absense of the custom attribute value
      - `fl` (field list) parameter is used to only return  `core.name` and `core.classType` properties (reducing the search resultset size to minimum)

    - for each object
      - get the id of the schema (first n / characters) & lookup schema name 
        - if schema name is not found, there are 2 possible causes
          - table or column belong to an external schema, so the id's are skewed (this should not happen starting with EDC v10.5.8).  in that case the script will search through all schema id's to see if a match can be found
          - column belongs to a public schema, and therefore has no parent schema (so no atribute value to populate)
            - possible extra parameter could be added here to provide a value for this case like 'None' or 'N/A'
      - if schema name is found
        - write a line to bulk import file, with id, name, custom attr value for schema

when all objects in the resource (across all pages), then submit all .csv files created for bulk import

# bulk import process

before calling the bulk import process, we first need to check the current import jobs to see how many are currently queued.  
to get a list of queued bulk imports, use the following:-

- GET 2/catalog/jobs/objectImports
- passing `jobStatus=SUBMITTED` as the parameter

if the totalCount returned < `--queuesize` parameter (defaut 10), then the process pauses for 2seconds and tries again until the number of SUBMITTED jobs is less than queuesize

call bulk import with populated objects, using POST 2/catalog/jobs/objectImports, passing the csv file for the import

if process ends/terminated - can always be re-started because query will find objects remaining


# How do you run this script?

## Enrionment Setup (one time process)
- get code from git repository (into any folder)
  - git clone https://github.com/Informatica-EIC/REST-API-Samples.git
- setup python environment (will work on any platform)
  - linux 8.x/macos - python3.12 -m venv .venv
  - windows - python -m venv .venv  (use py --list to list available python versions)
- activate python environment
  - source .venv/bin/activate   (for linux & macos)
  - .venv/scripts/activate.pst      (for windows)
- download python 3rd party libraries  (one time setup per environment, installs requests, pythondotenv)
  - python -m pip install -r requirements.txt  
- setup connection to EDC (creates .env file, or other name of your choice)
  - python setupConnection.py      (alternative - python db_schema_customattr.py --setup)
    - you will be promted for edc url, user id, pwd and have the option to write to .env file.  default file is named .env (in current folder)

## Parameter file Setup

- use or clone ./config/db_schema_vars.txt
- add your custom attribute id to the schema_custom_attr_id setting.<br/>
you can get the value by exporting a sample from EDC, or use the API to get custom attributes
- check the setting for class_types.  default values are  (change as needed):-<br/>
com.infa.ldm.relational.Table com.infa.ldm.relational.View com.infa.ldm.relational.Column com.infa.ldm.relational.ViewColumn com.infa.ldm.google.bigquery.View com.infa.ldm.google.bigquery.Table com.infa.ldm.google.bigquery.Field com.infa.ldm.AdvancedScanners.PLSQL.ExternalTable com.infa.ldm.AdvancedScanners.PLSQL.ExternalColumn
- add any query filters to restrict the scope of the process (e.g. by resource name or type)
  

# Example

for this example, we will run the process to populate custom attributes for 2 different resource types (Oracle and Snowflake)



