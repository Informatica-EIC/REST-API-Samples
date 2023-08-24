# Model Linker

## Purpose

Create links from data model objects E.g. entities/attributes to the corresponding database objects

## Usage Notes

Works only for models that have physical names defined.  E.g. from Ewrin - the models should be logical/physical or physical only
if the physical name cannot be found, the catalog object name is used

Note:  this utility is not optimized for performance (e.g. does not use async http calls or multiple threads)

## Implementation

a .properties file is used to configure the ModelLinker process.  the default name is `catalog_utils.properties` - but can be any name

settings

- `modelLinker.entityQuery`  query to find all entities/tables created to be processed.  filters on resource type or name can be added
   example:-
   ```
	core.classType:(com.infa.ldm.erwin.TableEntity \
                           OR com.infa.ldm.erwin.Entity \
                           OR com.infa.ldm.erwin.Table \
                           OR com.infa.ldm.extended.sappowerdesigner.Entity \
                           OR com.infa.ldm.extended.sappowerdesigner.Table) \
    AND  core.resourceName:acme_erwin
	```
- `modelLinker.entityFQ` apply a query filter to the entityQuery, example: to filter based on resource name.   Recent testing shows this works better than trying to add a resourceName filter to the entityQuery.  example: `core.resourceName:acme_erwin`
- `modelLinker.entityToAttrLink` comma seperated list of links used for Entity|Table to attribute/pk 
- `modelLinker.physicalNameAttr` comma seperated list of attribute id's to use to find the physical name (default for erwin & powerdesigner is)
   `com.infa.ldm.erwin.PhysicalName,com.infa.ldm.extended.sappowerdesigner.PhysicalName`
- `modelLinker.tableQuery` - query to find table objects in the relational model - should not need to be changed
- `modelLinker.tableToColLink` - query to find the columns belongin to a table in the relational model - should not need to be changed
- `modelLinker.logfile` - log file to write results to (console and log get same content) - default modelLinker.log
- `modelLinker.useOwnerSchema` - boolean, use the schema name from the model for lookup (will search by core.autoSuggestMatchId - uppercase) - default is false
- `modelLinker.ownerSchemaAttr` - attribute that contains the db schema e.g. for Erwin `com.infa.ldm.erwin.OwnerSchema`
- `modelLinker.lineageFile` - custom lineage file generated, default=model_lineage.csv
- `modelLinker.testOnly` - if true, will not make any updates via REST api - custom lineage and log will still be created
   - note:  if only using custom lineage, you should not need to set this to true, unless you want to write directly via API call
- `modelLinker.deleteLinks` - delete links, if they were created by setting testOnly to false (cleanup links) for testing the process
   - note: - if only using custom lineage- you should never need to deleteLinks
- `modelLinker.attributePropagation` - boolean flag to enable attribute propagation (copies attributes from model to dbms object)  value=true|false.   
    - note:  Setting this to true can propagate attributes without actually linking them (e.g. if testOnly=false)
- `modelLinker.attributesToPropagate` - list of attributes to propagate.  comma separated for each column paring and : seperated for the from/to attribute.
    - e.g. `modelLinker.attributesToPropagate=core.name:com.infa.ldm.ootb.enrichments.displayName,com.infa.ldm.erwin.Definition:com.infa.ldm.ootb.enrichments.businessDescription`


## downloading executable version of this utility

if you do not want to compile/run from source - an executable version of this utility is available for download from the informatica TSFTP server
- location: /updates/Catalog_Solutions/utilities folder
- file: edc_catalog_utils_java_{version}.zip, unzip to any folder (requires Java)
  
to run the utility:-
- unzip the catalog utils package
- edit (or copy to new file and edit) the .properties file to suit your environment (connection settings `rest_service`,`user`, `password` - if password is empty, you will be prompted)
- if you catalog services uses certificates, modify modelLinker.cmd/modelLinker.sh to reference your truststore (e.g. infa_truststore.jks)
- `./modelLinker.sh catalog_utils.properties`
- Model Linker properties are set after the header

        ```
        #*****************************************************************
        # Model Linker
        #*****************************************************************
        ```


## sample output - console/log small model (Ewrin)

```
napslxapp01.infaaws.com:/data/utils_new#./modelLinker.sh catalog_utils.properties

ModelLinker 1.1 initializing properties from: catalog_utils.properties
reading properties from: catalog_utils.properties
   EDC rest url: http://napslxapp01:9085/access/2
    entityQuery: core.classType:(com.infa.ldm.erwin.TableEntity or com.infa.ldm.erwin.Entity or com.infa.ldm.erwin.Table or com.infa.ldm.extended.sappowerdesigner.Entity or com.infa.ldm.extended.sappowerdesigner.Table) AND  core.resourceName:acme_erwin
     tableQuery: core.classType:com.infa.ldm.relational.Table
   physNameAttr: com.infa.ldm.erwin.PhysicalName,com.infa.ldm.extended.sappowerdesigner.PhysicalName
  physNameAttrs: [com.infa.ldm.erwin.PhysicalName, com.infa.ldm.extended.sappowerdesigner.PhysicalName]
 entityAttrLink: com.infa.ldm.erwin.TableEntityColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TableColumn,com.infa.ldm.erwin.TableEntityPrimaryKeyColumnAttribute,com.infa.ldm.erwin.TablePrimaryKeyColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TablePrimaryKey
entityAttrLinks: [com.infa.ldm.erwin.TableEntityColumnAttribute, com.infa.ldm.extended.sappowerdesigner.TableColumn, com.infa.ldm.erwin.TableEntityPrimaryKeyColumnAttribute, com.infa.ldm.erwin.TablePrimaryKeyColumnAttribute, com.infa.ldm.extended.sappowerdesigner.TablePrimaryKey]
 tableToColLink: com.infa.ldm.relational.TableColumn
   delete links: false
       log file: modelLinker.log
      test mode: true
        initializing logFile:modelLinker.log
        initializing lineageFile:model_lineage.csv
entity objects.. using query= core.classType:(com.infa.ldm.erwin.TableEntity or com.infa.ldm.erwin.Entity or com.infa.ldm.erwin.Table or com.infa.ldm.extended.sappowerdesigner.Entity or com.infa.ldm.extended.sappowerdesigner.Table) AND  core.resourceName:acme_erwin
Entities found: 4
Entity: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics name=Demographics physicalName=CRM_CUSTOMER_DEMOGRAPHICS
        finding table (exact name match): core.classType:com.infa.ldm.relational.Table AND core.name_lc_exact:"CRM_CUSTOMER_DEMOGRAPHICS"
        Tables found=1
        physical table id: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS
                linking objects.... acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics -> acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS
        get attrs for entity: using link attribute: com.infa.ldm.erwin.TableEntityColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TableColumn,com.infa.ldm.erwin.TableEntityPrimaryKeyColumnAttribute,com.infa.ldm.erwin.TablePrimaryKeyColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TablePrimaryKey
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/CustomerId attr Name=CustomerId physicalName=CUST_ID
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Category Code attr Name=Category Code physicalName=CAMEO_CATEGORY_CD
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Preferred Language attr Name=Preferred Language physicalName=CUST_LANGUAGE
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Category Description attr Name=Category Description physicalName=CAMEO_CATEGORY_DESC
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Income Profile attr Name=Income Profile physicalName=CUST_INCOME_PROFILE
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Education Level attr Name=Education Level physicalName=CUST_EDUCATION_LEVEL
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Group Code attr Name=Group Code physicalName=CAMEO_GROUP_CD
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/International Code attr Name=International Code physicalName=CAMEO_INTERNATIONAL_CD
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/International Description attr Name=International Description physicalName=CAMEO_INTERNATIONAL_DESC
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Group Description attr Name=Group Description physicalName=CAMEO_GROUP_DESC
        attrMapKeys=[CAMEO_CATEGORY_CD, CUST_EDUCATION_LEVEL, CAMEO_GROUP_CD, CUST_ID, CAMEO_INTERNATIONAL_CD, CAMEO_CATEGORY_DESC, CUST_LANGUAGE, CAMEO_GROUP_DESC, CAMEO_INTERNATIONAL_DESC, CUST_INCOME_PROFILE]
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_CATEGORY_CD attr Name=CAMEO_CATEGORY_CD match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_CATEGORY_DESC attr Name=CAMEO_CATEGORY_DESC match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CUST_EDUCATION_LEVEL attr Name=CUST_EDUCATION_LEVEL match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CUST_ID attr Name=CUST_ID match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_GROUP_DESC attr Name=CAMEO_GROUP_DESC match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_INTERNATIONAL_CD attr Name=CAMEO_INTERNATIONAL_CD match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_GROUP_CD attr Name=CAMEO_GROUP_CD match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_INTERNATIONAL_DESC attr Name=CAMEO_INTERNATIONAL_DESC match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CUST_INCOME_PROFILE attr Name=CUST_INCOME_PROFILE match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CUST_LANGUAGE attr Name=CUST_LANGUAGE match?true
        finished with: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics
Entity: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address name=Customer Address physicalName=CRM_CUSTOMER_ADDRESS
        finding table (exact name match): core.classType:com.infa.ldm.relational.Table AND core.name_lc_exact:"CRM_CUSTOMER_ADDRESS"
        Tables found=1
        physical table id: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS
                linking objects.... acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address -> acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS
        get attrs for entity: using link attribute: com.infa.ldm.erwin.TableEntityColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TableColumn,com.infa.ldm.erwin.TableEntityPrimaryKeyColumnAttribute,com.infa.ldm.erwin.TablePrimaryKeyColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TablePrimaryKey
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/CustomerId attr Name=CustomerId physicalName=CUST_ID
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/House Name attr Name=House Name physicalName=CUST_HOUSENAME
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/Street attr Name=Street physicalName=CUST_STREET
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/ZipCode attr Name=ZipCode physicalName=CUST_POSTCODE
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/Address attr Name=Address physicalName=CUST_ADDRESS
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/House Number attr Name=House Number physicalName=CUST_HOUSENUMBER
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/State attr Name=State physicalName=CUST_PROVINCE
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/Country attr Name=Country physicalName=CUST_COUNTRY
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/City attr Name=City physicalName=CUST_CITY
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/CountryISO attr Name=CountryISO physicalName=CUST_COUNTRY_ISO
        attrMapKeys=[CUST_ID, CUST_CITY, CUST_STREET, CUST_HOUSENUMBER, CUST_PROVINCE, CUST_ADDRESS, CUST_HOUSENAME, CUST_COUNTRY, CUST_COUNTRY_ISO, CUST_POSTCODE]
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_HOUSENAME attr Name=CUST_HOUSENAME match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_STREET attr Name=CUST_STREET match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_ADDRESS attr Name=CUST_ADDRESS match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_PROVINCE attr Name=CUST_PROVINCE match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_CITY attr Name=CUST_CITY match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_HOUSENUMBER attr Name=CUST_HOUSENUMBER match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_COUNTRY attr Name=CUST_COUNTRY match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_ID attr Name=CUST_ID match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_COUNTRY_ISO attr Name=CUST_COUNTRY_ISO match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_POSTCODE attr Name=CUST_POSTCODE match?true
        finished with: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address
Entity: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main name=Customer Main physicalName=CRM_CUSTOMER_MAIN
        finding table (exact name match): core.classType:com.infa.ldm.relational.Table AND core.name_lc_exact:"CRM_CUSTOMER_MAIN"
        Tables found=1
        physical table id: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN
                linking objects.... acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main -> acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN
        get attrs for entity: using link attribute: com.infa.ldm.erwin.TableEntityColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TableColumn,com.infa.ldm.erwin.TableEntityPrimaryKeyColumnAttribute,com.infa.ldm.erwin.TablePrimaryKeyColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TablePrimaryKey
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/CustomerId attr Name=CustomerId physicalName=CUST_ID
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/First Name attr Name=First Name physicalName=CUST_FIRSTNAME
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Date of Birth attr Name=Date of Birth physicalName=CUST_DOB
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Tier attr Name=Tier physicalName=CUST_TIER
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Customer Code attr Name=Customer Code physicalName=CUST_CODE
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Gender attr Name=Gender physicalName=CUST_GENDER
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Last Name attr Name=Last Name physicalName=CUST_LASTNAME
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Middle Name attr Name=Middle Name physicalName=CUST_MIDDLENAME
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Country attr Name=Country physicalName=CUST_COUNTRY
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Name attr Name=Name physicalName=CUST_NAME
        attrMapKeys=[CUST_ID, CUST_LASTNAME, CUST_DOB, CUST_TIER, CUST_GENDER, CUST_CODE, CUST_MIDDLENAME, CUST_FIRSTNAME, CUST_COUNTRY, CUST_NAME]
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_FIRSTNAME attr Name=CUST_FIRSTNAME match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_LASTNAME attr Name=CUST_LASTNAME match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_ID attr Name=CUST_ID match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_TIER attr Name=CUST_TIER match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_COUNTRY attr Name=CUST_COUNTRY match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_CODE attr Name=CUST_CODE match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_MIDDLENAME attr Name=CUST_MIDDLENAME match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_NAME attr Name=CUST_NAME match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_GENDER attr Name=CUST_GENDER match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_DOB attr Name=CUST_DOB match?true
        finished with: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main
Entity: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications name=Communications physicalName=CRM_CUSTOMER_COMMUNICATION
        finding table (exact name match): core.classType:com.infa.ldm.relational.Table AND core.name_lc_exact:"CRM_CUSTOMER_COMMUNICATION"
        Tables found=1
        physical table id: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION
                linking objects.... acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications -> acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION
        get attrs for entity: using link attribute: com.infa.ldm.erwin.TableEntityColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TableColumn,com.infa.ldm.erwin.TableEntityPrimaryKeyColumnAttribute,com.infa.ldm.erwin.TablePrimaryKeyColumnAttribute,com.infa.ldm.extended.sappowerdesigner.TablePrimaryKey
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/CustomerId attr Name=CustomerId physicalName=CUST_ID
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/Email Address attr Name=Email Address physicalName=CUST_EMAIL
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/Twitter Id attr Name=Twitter Id physicalName=CUST_TWITTER
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/Cell Phone attr Name=Cell Phone physicalName=CUST_MOBILE
                Attribute: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/Phone Number attr Name=Phone Number physicalName=CUST_PHONE
        attrMapKeys=[CUST_ID, CUST_MOBILE, CUST_PHONE, CUST_TWITTER, CUST_EMAIL]
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_PHONE attr Name=CUST_PHONE match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_TWITTER attr Name=CUST_TWITTER match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_MOBILE attr Name=CUST_MOBILE match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_ID attr Name=CUST_ID match?true
                Column: acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_EMAIL attr Name=CUST_EMAIL match?true
        finished with: acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications
finished... links written=0 links skipped(existing)=0 linksDeleted=0
errors:  0
Finished

```

## linege file generated - small erwin model

modelLineage.csv

```
Association,From Connection,To Connection,From Object,To Object
core.DataSetDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Category Code,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_CATEGORY_CD
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Category Description,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_CATEGORY_DESC
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Education Level,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CUST_EDUCATION_LEVEL
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/CustomerId,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CUST_ID
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Group Description,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_GROUP_DESC
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/International Code,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_INTERNATIONAL_CD
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Group Code,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_GROUP_CD
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/International Description,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CAMEO_INTERNATIONAL_DESC
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Income Profile,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CUST_INCOME_PROFILE
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Demographics/Columns - Attributes/Preferred Language,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_DEMOGRAPHICS/CUST_LANGUAGE
core.DataSetDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/House Name,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_HOUSENAME
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/Street,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_STREET
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/Address,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_ADDRESS
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/State,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_PROVINCE
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/City,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_CITY
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/House Number,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_HOUSENUMBER
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/Country,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_COUNTRY
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/CustomerId,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_ID
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/CountryISO,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_COUNTRY_ISO
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Address/Columns - Attributes/ZipCode,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_ADDRESS/CUST_POSTCODE
core.DataSetDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/First Name,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_FIRSTNAME
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Last Name,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_LASTNAME
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/CustomerId,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_ID
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Tier,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_TIER
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Country,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_COUNTRY
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Customer Code,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_CODE
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Middle Name,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_MIDDLENAME
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Name,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_NAME
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Gender,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_GENDER
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Customer Main/Columns - Attributes/Date of Birth,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_MAIN/CUST_DOB
core.DataSetDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/Phone Number,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_PHONE
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/Twitter Id,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_TWITTER
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/Cell Phone,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_MOBILE
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/CustomerId,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_ID
core.DirectionalDataFlow,,,acme_erwin://CRM Model/Schemas/ACME_CRM/Tables - Entities/Communications/Columns - Attributes/Email Address,acme_crm://informatica/ACME_CRM/CRM_CUSTOMER_COMMUNICATION/CUST_EMAIL
core.DataSetDataFlow,,,erwin_prodigy://Model_4/Tables - Entities/Customer,hermes://informatica/HERMES/CUSTOMER
core.DataSetDataFlow,,,sh_erwin://Model_1/Tables - Entities/Employee,cstmr://informatica/CSTMR/EMPLOYEE

```
