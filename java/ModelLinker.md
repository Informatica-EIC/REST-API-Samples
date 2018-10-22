# Model Linker

## Purpose

Create links from data model objects E.g. entities/attributes to the corresponding database objects

## Usage Notes

Works only for models that have physical names defined.  E.g. from Ewrin - the models should be logical/physical or physical only
if the physical name cannot be found, the catalog object name is used

## Implementation

a .properties file is used to configure the ModelLinker process.  the default name is `catalog_utils.properties` - but can be any name

settings

- `modelLinker.entityQuery`  query to find all entities/tables created to be processed.  filters on resource type or name can be added
   example:-
   ```
	core.classType:(com.infa.ldm.erwin.TableEntity \
                           or com.infa.ldm.erwin.Entity \
                           or com.infa.ldm.erwin.Table \
                           or com.infa.ldm.extended.sappowerdesigner.Entity \
                           or com.infa.ldm.extended.sappowerdesigner.Table) \
    AND  core.resourceName:acme_erwin
	```
- `modelLinker.entityToAttrLink` comma seperated list of links used for Entity|Table to attribute/pk 
- `modelLinker.physicalNameAttr` comma seperated list of attribute id's to use to find the physical name (default for erwin & powerdesigner is)
   `com.infa.ldm.erwin.PhysicalName,com.infa.ldm.extended.sappowerdesigner.PhysicalName`
- `modelLinker.tableQuery` - query to find table objects in the relational model - should not need to be changed
- `modelLinker.tableToColLink` - query to find the columns belongin to a table in the relational model - should not need to be changed
- `modelLinker.logfile` - log file to write results to (console and log get same content) - default modelLinker.log
- `modelLinker.lineageFile` - custom lineage file generated, default=model_lineage.csv
- `modelLinker.testOnly` - if true, will not make any updates via REST api - custom lineage and log will still be created
   - note:  if only using custom lineage, you should not need to set this to true, unless you want to write directly via API call
- `modelLinker.deleteLinks` - delete links, if they were created by setting testOnly to false (cleanup links) for testing the process
   - note: - if only using custom lineage- you should never need to deleteLinks

## downloading executable version of this utility

if you do not want to compile/run from source - an executable version of this utility is available for download.

- from linux:  `wget -O  wget -O catalog_utils.zip https://d398h.app.goo.gl/edcUtils` 
- or download directly from <https://d398h.app.goo.gl/edcUtils>
  - unzip catalog_utils.zip
  - chmod +x *.sh
  
to run the utility:-
- edit catalog_utils.properties to suit your environment (connection settings `rest_service`,`user`, `password` - if password is empty, you will be prompted)
- `./modelLinker.sh catalog_utils.properties`
- or modify the .sh to run on windows




