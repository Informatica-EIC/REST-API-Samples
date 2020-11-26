# externalDBLinker utility

this utility is used to link any tables/columns that belong to ExternalDatabase & ExternalSchema objects.

this happens when other databases are queried, usually in views, via dblinks (oracle) or direct database references in sql (sqlserver & sybase).

### Note: for SQLServer databases, this utility is no longer necessary (v10.4.0+) - since connection assignments and reference objects are created with the scan (Auto Assign Connections=true)

## process outline
- for each ExternalDatabase (found via query - using the parameters query `core.classType:com.infa.ldm.relational.ExternalDatabase`)
    - execute the rest api call `/access/2/catalog/data/relationships` to find all child object to a depth of 2 outward, including the `core.name` and `core.classType`
        - for each table found - we need to find the corresponding table that was read in the external database
        - format a query to find all tables/views with the same name, in the same resource tyoe (e.g. oracle/sqlserver/sybase) using this query `core.classType:(com.infa.ldm.relational.Table or com.infa.ldm.relational.View)  and core.name_lc_exact:<tableName>`
            - for each candidate table/view found
                - if the table names match (case insensitive) AND the schema name matches (or if the external schema name is empty and the candiate schema is dbo)
                    - create a lineage link at the table level using `core.DataSetDataFlow`
                    - for all matching columns
                        - create a lineage link at the column level using `core.DirectionalDataFlow`

## output
a file named `out/externalDBLinks.csv` is created by this process, using the custom lineage format (including association name & id of the source/target objects) - no connection assignment necessary

## Usage
syntax: `usage: externalDBLinker.py [-h] [-c EDCURL] [-v ENVFILE] [-a AUTH | -u USER] [-s SSLCERT] [-f CSVFILENAME] [-o OUTDIR] [-dl DBLINSFILE] [-i] [-rn LINEAGERESOURCENAME] [-rt LINEAGERESOURCETEMPLATE]`


to start the utility
`python externalDBLinker.py <options>`

by default - all log messages are written to the console (stdout) to re-direct to a file, use this syntax.

`python externalDBLinker.py > externalLinker.log &`

then you can `tail -f externalLinker.log` to monitor progress

## Import to EDC
create an instance of the "Custom Lineage" resource type and import/drop externalDBLinker.csv

to create/update the resource directly in the catalog, upload the lineage file then execute the resource load, use the following settings:-
- `executeEDCImport=True` - set to True to execute the import
- `lineageResourceName=`  resource name to create/update
- `lineageResourceTemplate="template/custom_lineage_template.json"` - template to use to create the resource

when setting executeEDTImport=True - it will call `edcutils.createOrUpdateAndExecuteResource`

## Other Notes
should work on all platforms (linux/mac/windows) using python 3.6+
for best performance, run on the catalog service server

## Oracle DBlinks

for the case where an oracle dblinks is used - a parameter file has been created to map the dblink name to the actual schema and database (target of the link)
this makes it possible to generate the correct external links.

previously - if the schema name was not used when referring to a dblink - the id of the external schema object was formatted like this `informatica_[783]@oradockerdaa_[791]`  (linkingdb_[nnn]@dblinkname_[nnn])

the format of the dblink lookup file has 3 columns:-
- dblink name of the dblink (without the @)
- database database that the dblink is referring to (if empyty, will look for any database)
- schema used for the dblink (needed where no schema is referenced)

example:-
```
dblink,database,schema
oradockerdaa,ORCLPDB133,DAA
```
