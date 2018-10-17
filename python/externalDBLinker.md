# externalDBLinker utility

this utility is used to link any tables/columns that belong to ExternalDatabase & ExternalSchema objects.

this happens when other databases are queried, usually in views, via dblinks (oracle) or direct database references in sql (sqlserver & sybase).

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
edit externalDBlinker.py - changing the following settings
- catalogServer - e.g. `http://napslxapp01:9085`
- uid & pwd for your catalog

to start the utility
`python externalDBLinker.py`

by default - all log messages are written to the console (stdout) to re-direct to a file, use this syntax.

`python externalDBLinker.py > externalLinker.log &`

then you can `tail -f externalLinker.log` to monitor progress

## Import to EDC
create an instance of the "Custom Lineage" resource type and import/drop externalDBLinker.csv

## Other Notes
should work on all platforms (linux/mac/windows) using either python 2.7 or 3.x

for best performance, run on the catalog service server
