# Schema Replication Lineage Script

## Purpose
provides the ability to link tables/columns in a database schema that are replicated to other schemas/databases & no scanner exists to automatcially document these relationships.  (e.g. sqoop, scripts/code, goldengate ...)

## Usage
Note:  usage has changed - no settings are hard coded, all can be provided via command-line arguments

`python dbSchemaReplicationLineage.py <options>

- `-lr` or `--leftresource` - name of the resource containing the schema on the left
- `-ls` or `--leftschema` - name of the schema on the left
- `-lt` or `--leftschematype` - classtype of the left schema (e.g. 'com.infa.ldm.relational.Schema') - some scanners (e.g. hana db have different class types)

- `-rr` or `--rightresource` - name of the resource containing the schema on the right
- `-rs` or `--rightschema` - name of the schema on the left
- `-rt` or `--rightschemaType` - classtype of the right schema
- `-rtp` or `--righttableprefix` - prefix to use for target tables (remove the prefix to match on left table)

catalog connection settings (can/should be passed via .env file)

- `-c` or `--edcurl` host:port for the catalog service.  e.g. 'http://napslxapp01:9085'
- `-v` or `--envfile` reference to an environment file with settings for catalog and user id/pwd (encoded)'
- `-a` pr `--auth` - user id/password, encoded - see `encodeUser.py` or `setupConnection.py` (but just use a .env file)


csv file output settings

- `-pfx` or `--csvprefix` - prefix for the generated csv file - e.g. "schemaLineage"
- `-out` or `--outDir` - folder to write the csv file, default/example "out"

## Lineage File Generated
the file generated uses the direct lineage format, supported in EDC v10.2.1+ and available for 10.2.0 via patch, using object id's and specific relationships vs connection assignment.

when configuring the CustomLineage resource - you do not need to check "Auto Assign Connections" in the Metadata Load Settings tab

## Example

We want to create linage from:-
- from oracle Schema named PIM - in EDC resource named PIM_Oracle
- to SQL Server Schema named dbo  - in EDC resource named PIM_SQLServer

there are 5 tables in each schema

Command-line syntax (assuming .env file has already been configured with INFA_EDC_URL and INFA_EDC_AUTH settings)

```python dbSchemaReplicationLineage.py -lr PIM_Oracle -ls PIM -rr PIM_SQLServer -rs dbo```

the following output is created on the console

```
python dbSchemaReplicationLineage.py -lr PIM_Oracle -ls PIM -rr PIM_SQLServer -rs dbo
        reading common env/env file/cmd settings
ready to check .env file .env
                loading from .env file .env
                read edc url from .env value=https://napslxapp01:9554
                replacing edc url with value from .env
                replacing edc auth with INFA_EDC_AUTH value from {args.envfile}
        finished reading common env/.env/cmd parameters
command-line args parsed = Namespace(auth=None, csvprefix='schemaLineage', edcurl=None, envfile='.env', leftresource='PIM_Oracle', leftschema='PIM', lefttype='com.infa.ldm.relational.Schema', outDir='out', rightresource='PIM_SQLServer', rightschema='dbo', righttableprefix='', righttype='com.infa.ldm.relational.Schema', sslcert=None, user=None)
dbSchemaReplicationLineage:start
Catalog=https://napslxapp01:9554
left:  resource=PIM_Oracle
left:    schema=PIM
left:      type=com.infa.ldm.relational.Schema
right:  resource=PIM_SQLServer
right:    schema=dbo
right:      type=com.infa.ldm.relational.Schema
output folder:out
output file prefix:schemaLineage
right table prefix:
initializing file: out/schemaLineage_pim_dbo.csv
get left schema: name=PIM resource=PIM_Oracle type=com.infa.ldm.relational.Schema
        getSchemaContents for:PIM resource=PIM_Oracle
        query=+core.resourceName:"PIM_Oracle" +core.classType:"com.infa.ldm.relational.Schema" +core.name:"PIM"
session get finished: 200
        objects returned: 1
        found schema: PIM id=PIM_Oracle://informatica/PIM
        GET child rels for schema: https://napslxapp01:9554/access/2/catalog/data/relationships parms={'seed': 'PIM_Oracle://informatica/PIM', 'association': 'core.ParentChild', 'depth': '2', 'direction': 'OUT', 'includeAttribute': {'core.name', 'core.classType'}, 'includeTerms': 'false', 'removeDuplicateAggregateLinks': 'false'}
        lineage resp=200
        getSchema: returning 29 columns, in 5 tables
get right schema: name=dbo resource=PIM_SQLServer type=com.infa.ldm.relational.Schema
        getSchemaContents for:dbo resource=PIM_SQLServer
        query=+core.resourceName:"PIM_SQLServer" +core.classType:"com.infa.ldm.relational.Schema" +core.name:"dbo"
session get finished: 200
        objects returned: 1
        found schema: dbo id=PIM_SQLServer://PIM/dbo
        GET child rels for schema: https://napslxapp01:9554/access/2/catalog/data/relationships parms={'seed': 'PIM_SQLServer://PIM/dbo', 'association': 'core.ParentChild', 'depth': '2', 'direction': 'OUT', 'includeAttribute': {'core.name', 'core.classType'}, 'includeTerms': 'false', 'removeDuplicateAggregateLinks': 'false'}
        lineage resp=200
        getSchema: returning 29 columns, in 5 tables

processing: 34 objects (left side)
dbSchemaLineageGen:finished. 34 links created, 0 missing (found in left, no match on right)
run time = 0.7643930912017822 seconds ---

```


after running the script - the following lineage file was generated `schemaLineage_pim_dbo.csv`:-

Note:  there is need for From Connection or To Connection

actual csv generated

```
Association,From Connection,To Connection,From Object,To Object
core.DataSourceDataFlow,,,PIM_Oracle://informatica/PIM,PIM_SQLServer://PIM/dbo
core.DataSetDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS,PIM_SQLServer://PIM/dbo/Product_Descriptions
core.DataSetDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES,PIM_SQLServer://PIM/dbo/Product_Categories
core.DataSetDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC,PIM_SQLServer://PIM/dbo/Product_Generic
core.DataSetDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_PRICES,PIM_SQLServer://PIM/dbo/Product_Prices
core.DataSetDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_BRANDS,PIM_SQLServer://PIM/dbo/Product_Brands
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_PRICES/VAT_PERC,PIM_SQLServer://PIM/dbo/Product_Prices/VAT_Perc
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_PRICES/DATE_PRICE_START,PIM_SQLServer://PIM/dbo/Product_Prices/Date_Price_Start
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_PRICES/DATE_PRICE_END,PIM_SQLServer://PIM/dbo/Product_Prices/Date_Price_End
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_PRICES/PRODUCT_ID,PIM_SQLServer://PIM/dbo/Product_Prices/Product_Id
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_PRICES/SALES_PRICE,PIM_SQLServer://PIM/dbo/Product_Prices/Sales_Price
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_PRICES/COST_PRICE,PIM_SQLServer://PIM/dbo/Product_Prices/Cost_Price
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_PRICES/VAT_AMOUNT,PIM_SQLServer://PIM/dbo/Product_Prices/VAT_Amount
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES/CATEGORY_ID,PIM_SQLServer://PIM/dbo/Product_Categories/Category_Id
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES/PARENT_ID,PIM_SQLServer://PIM/dbo/Product_Categories/Parent_Id
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES/CATEGORY_CODE,PIM_SQLServer://PIM/dbo/Product_Categories/Category_Code
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES/CATEGORY_DESCRIPTION,PIM_SQLServer://PIM/dbo/Product_Categories/Category_Description
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_BRANDS/BRAND_CODE,PIM_SQLServer://PIM/dbo/Product_Brands/Brand_Code
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_BRANDS/DATE_FIRST_LOADED,PIM_SQLServer://PIM/dbo/Product_Brands/Date_First_Loaded
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_BRANDS/BRAND_ID,PIM_SQLServer://PIM/dbo/Product_Brands/Brand_Id
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_BRANDS/BRAND_DESCRIPTION,PIM_SQLServer://PIM/dbo/Product_Brands/Brand_Description
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS/PRODUCT_ID,PIM_SQLServer://PIM/dbo/Product_Descriptions/Product_Id
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS/PRODUCT_DESC_SHORT,PIM_SQLServer://PIM/dbo/Product_Descriptions/Product_Desc_Short
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS/LANGUAGE,PIM_SQLServer://PIM/dbo/Product_Descriptions/Language
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS/PRODUCT_DESC_LONG,PIM_SQLServer://PIM/dbo/Product_Descriptions/Product_Desc_Long
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_DIM_L,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Dim_L
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_ID,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Id
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_START_DATE,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Start_Date
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_WEIGHT,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Weight
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_CATEGORY,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Category
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_END_DATE,PIM_SQLServer://PIM/dbo/Product_Generic/Product_End_Date
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_BRAND,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Brand
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_DIM_W,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Dim_W
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_CODE,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Code
core.DirectionalDataFlow,,,PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_DIM_H,PIM_SQLServer://PIM/dbo/Product_Generic/Product_Dim_H
```

Table format

Association|From Connection|To Connection|From Object|To Object
---|---|---|---|---
core.DataSourceDataFlow|||PIM_Oracle://informatica/PIM|PIM_SQLServer://PIM/dbo
core.DataSetDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS|PIM_SQLServer://PIM/dbo/Product_Descriptions
core.DataSetDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES|PIM_SQLServer://PIM/dbo/Product_Categories
core.DataSetDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC|PIM_SQLServer://PIM/dbo/Product_Generic
core.DataSetDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_PRICES|PIM_SQLServer://PIM/dbo/Product_Prices
core.DataSetDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_BRANDS|PIM_SQLServer://PIM/dbo/Product_Brands
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_PRICES/VAT_PERC|PIM_SQLServer://PIM/dbo/Product_Prices/VAT_Perc
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_PRICES/DATE_PRICE_START|PIM_SQLServer://PIM/dbo/Product_Prices/Date_Price_Start
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_PRICES/DATE_PRICE_END|PIM_SQLServer://PIM/dbo/Product_Prices/Date_Price_End
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_PRICES/PRODUCT_ID|PIM_SQLServer://PIM/dbo/Product_Prices/Product_Id
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_PRICES/SALES_PRICE|PIM_SQLServer://PIM/dbo/Product_Prices/Sales_Price
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_PRICES/COST_PRICE|PIM_SQLServer://PIM/dbo/Product_Prices/Cost_Price
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_PRICES/VAT_AMOUNT|PIM_SQLServer://PIM/dbo/Product_Prices/VAT_Amount
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES/CATEGORY_ID|PIM_SQLServer://PIM/dbo/Product_Categories/Category_Id
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES/PARENT_ID|PIM_SQLServer://PIM/dbo/Product_Categories/Parent_Id
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES/CATEGORY_CODE|PIM_SQLServer://PIM/dbo/Product_Categories/Category_Code
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_CATEGORIES/CATEGORY_DESCRIPTION|PIM_SQLServer://PIM/dbo/Product_Categories/Category_Description
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_BRANDS/BRAND_CODE|PIM_SQLServer://PIM/dbo/Product_Brands/Brand_Code
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_BRANDS/DATE_FIRST_LOADED|PIM_SQLServer://PIM/dbo/Product_Brands/Date_First_Loaded
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_BRANDS/BRAND_ID|PIM_SQLServer://PIM/dbo/Product_Brands/Brand_Id
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_BRANDS/BRAND_DESCRIPTION|PIM_SQLServer://PIM/dbo/Product_Brands/Brand_Description
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS/PRODUCT_ID|PIM_SQLServer://PIM/dbo/Product_Descriptions/Product_Id
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS/PRODUCT_DESC_SHORT|PIM_SQLServer://PIM/dbo/Product_Descriptions/Product_Desc_Short
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS/LANGUAGE|PIM_SQLServer://PIM/dbo/Product_Descriptions/Language
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_DESCRIPTIONS/PRODUCT_DESC_LONG|PIM_SQLServer://PIM/dbo/Product_Descriptions/Product_Desc_Long
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_DIM_L|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Dim_L
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_ID|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Id
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_START_DATE|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Start_Date
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_WEIGHT|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Weight
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_CATEGORY|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Category
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_END_DATE|PIM_SQLServer://PIM/dbo/Product_Generic/Product_End_Date
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_BRAND|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Brand
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_DIM_W|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Dim_W
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_CODE|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Code
core.DirectionalDataFlow|||PIM_Oracle://informatica/PIM/PRODUCT_GENERIC/PRODUCT_DIM_H|PIM_SQLServer://PIM/dbo/Product_Generic/Product_Dim_H

