# BDM Scanner Controlflow Script

## Problem Statement

the Current BDM scanner for EDC, does not support control flow relationshios.  
This is clearly documented in the PAM.

While some scanners support control flows (treated separately to dataflows), like the PC scanner.
the BDM scanner currently does not.

For EDC users that need control flow relationships - this would require a feature request 
that would need proper analysis/planning/implementation/testing etc.

this utility script is intented to bridge the gap, to generate controlflows for Lookup 
Transformations.

## Implementation

This script is run for any BDM (Informatica Platform) resource & does the following

- For each Mapping
    - For each Lookup Transformation (in the mapping)
        - extract the lookup expression(s)
        - extract the lookup fields compared (splitting the expression into master, lookup columns)
        - get upstream lineage for the fields compared in the lookup tx.  
          - Note: upstream lineage will end for any field that is not an instance of com.infa.ldm.bdm.platform.Field
        - get downstream lineage for all lookup fields (to find where the data is mapped)
          - Note: downstream lineage ends the lineage traversal downstream when a class type other than com.infa.ldm.bdm.platform.Field is found
        - for each compared field (both master and lookup field)
            - generate a `core.DirectionalControlFlow` link to the fields found in downstream lineage
            - also generate a `core.DataSetControlFlow` for the parents of these fields (table level)
- an the end of the process, if the --edcimport flag is used
    - create or update a custom lineage resource, with same name as BDM scanner resource with _controlflow_lineage
    - upload the lineage file created by this script
    - execute the resource import


Note:  the custom lineage file imported will use direct id's - not connection assignment

## Pre-requisites

EDC v10.5.3+.  this version has support to enable lineage filtering to include control flow relationships

## Installation

download this script from TSFTP folder `/updates/Catalog_Solutions/utilities/bdm_scanner_lookup_controlflow`

file to download is named: bdm_scanner_controlflow_gen_v1.zip and contains the binary executable and a PDF version of this readme

the script was created in python, and compiled to a linux binary, embedding the python runtime
so it is not necessary to install python or any 3rd party packages
(note:  source code is available on github)

unzip the package from TSFTP to any linux machine

to prepare the script to connect to EDC, the best way is to execute the packaged binary with --setup option

e.g. 
./bdm_scanner_ctrlflow_gen --setup

- you will be prompted for:-
  - EDC URL - enter the host & port - e.g. https://asvdwsndboxrh01.informatica.com:9085
  - EDC user id
  - EDC password

the script will give you the option to create a .env file.  this file is the default used to connect to EDC
other files can be generated, making it possible to easily switch between EDC services (e.g. dev & prod)
the user id used must have api access (/1 and /2 api endpoints) and must have read access to the BDM scanner objects

## Script execution

the script can be stared with 2 controlling flags:-
- -rn (or --resourceName) <BDM Scanner Resource>
- --edcimport    (flag to create/update/run the custom lineage resource)

for other flags - pass -h - see following example

```
./bdm_scanner_ctrlflow_gen -h
BDM Scanner control flow lineage generator: version 1.0 starting
using Python version: 3.9.2
usage: bdm_scanner_ctrlflow_gen [-h] [-c EDCURL] [-v ENVFILE] [-a AUTH | -u USER] [-s SSLCERT] -rn RESOURCENAME [-o OUTDIR] [-i] [--setup]

optional arguments:
  -h, --help            show this help message and exit
  -c EDCURL, --edcurl EDCURL
                        edc url - including http(s)://<server>:<port>, if not already configured via INFA_EDC_URL environment var
  -v ENVFILE, --envfile ENVFILE
                        .env file with config settings INFA_EDC_URL,INFA_EDC_AUTH etc will over-ride system environment variables. if not specified - '.env' file in current folder will be used
  -a AUTH, --auth AUTH  basic authorization encoded string (preferred over -u) if not already configured via INFA_EDC_AUTH environment var
  -u USER, --user USER  user name - will also prompt for password
  -s SSLCERT, --sslcert SSLCERT
                        ssl certificate (pem format), if not already configured via INFA_EDC_SSL_PEM environment var
  -rn RESOURCENAME, --resourceName RESOURCENAME
                        setup the .env file for connecting to EDC
  -o OUTDIR, --outDir OUTDIR
                        output folder to write results - default = ./out - will create folder if it does not exist
  -i, --edcimport       use the rest api to create the custom lineage resource and start the import process
  --setup               setup the connection to EDC by creating a .env file - same as running setupConnection.py
```


# files created by this script
- ./out/<bdm_resource_name>_controlflow_lineage.csv  (the csv file with controlflow links) - folder location can be modified using -o <OUTDIR> parameter
- ./log/bdm_scanner_controlflow_gen_<timestamp>.log

# Example

Simple developer mapping with a lookup - shows a source table (Orders) and a lookup table (Customers)
where CustomerID for each is used as the lookup expression.
there are 3 columns returned from the lookup that are linked to the final target tx.

<img src="img/m_order_with_customer.png">

dataset level, - showing (correct) dataflow links, no controlflows

<img src="img/customer_order_details_dataset_level_before.png">

element level - showing (correct) dataflow links, no controlflows

<img src="img/customer_order_details_dataelement_level_before.png">



after script execution and switching on control flows

<img src="img/customer_order_details_dataset_level_with_controlflow.png">

after script execution, showing the 2 control fields, with control flow lineage to the 3 fields used via the lookup transformation

<img src="img/customer_order_details_dataelement_level_with_controlflow.png">

