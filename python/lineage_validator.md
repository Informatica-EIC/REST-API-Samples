# Lineage Validator script

## Purpose
Validate EDC custom lineage file that uses direct-id method to link objects
for connection assignment, use the missing links report from ldmadmin

we need to do this, because the EDC custom lineage resource type does not validate
the links it is reading when object id's are used.  if an id (left/right or both) does not match, no error is reported
in the scanner log & no lineage is generated for that row.

## Release History
- 2022-12-12 - bugfix:  issue #40 - reference id's were not validated (always false)
- 2022-11-04 - minor update, compatible with python 3.6 and -lf flag set to optional (for --setup)
- 2022-11-02 - Initial Version (ready for testing)

## Comments/Suggestions/Errors to report

Raise an issue on github: https://github.com/Informatica-EIC/REST-API-Samples/issues


## Output Created
a new file is created, in the same location as the lineage csv file to check, with the suffix _validation.csv

additional columns added:-
- `From Valid`  possible values: True/False/Unknown
- `To Valid`    possible values: True/False/Unknown
- `Link Exists` possible values: True/False/Unknown
- `Comments`     - any comments to help troubleshoot - as mentioned in the [Possible output in message column](#possible-output-in-message-column) section


> **Note:**<br>Unknown will be returned if connection assignment is  used for either the from/to object.  Lineage will not be validated for connection assignments

## Process
for each from/to object reference
- check if the id matches an object in EDC (lookup is case sensitive)
- if found
  - store True in the validation column for the source/target
- if not found
  - if the resource is case-sensitive, then
    - format a search using core.autoSuggestMatchId - using the last 2/3 elements of the path & the resource name
       - if found
         - store True in the validation column for the source/target
         - add a message, stating that a the case insensitive version matched, but the id did not
       - if not found
         - store False in the validation column for the source/target


## Possible output in message column

- id has leading/trailing whitespace.
- case sensitive id did not match, cis approach matched - actual is=`<actual_edc_id>`
- left object uses connection assignment.
- right object uses connection assignment.
- object link does not exist in EDC - lineage should be imported again
- fromId:reference object id used.  any time a reference id was used in lineage


## Implementation Notes

this script is not optimized for performance (e.g. using concurrent lookups)

if a connection-assignment link is used, it will be ignored

lookup process

- find object by id:  access/2/catalog/data/objects  passing id=`<object_id>`
  - if result count = 1, then match found
- find object that did not match, in case-insensitive resource.  (could be common for dbms objects)
  - access/2/catalog/data/objects with parameters:
    - q=core.autoSuggestMatchId:`{last_2_pathentries from id}`
    - fq=core.resourceName:`{resourceName}`
  - if result count = 1, then match found


## Setup and Usage

follow guidelines for all python scripts, [README.md](./README.md)

to setup a re-useable connection (by default in a file named .env, but can be saved to other filename), start passing --setup

`python3 lineage_validator.py --setup`

the system will prompt you for catalog url/port, id and password, and give you the option to save the settings to file

to start the lineage validation process:-

`python lineage_validator.py -lf <lineage_file>`

or using long-form parameter switch

`python lineage_validator.py --lineage_file <lineage_file>`

to print the command-line options

`python lineage_validator.py --help`  or `python lineage_validator.py -h`

```
usage: lineage_validator.py [-h] [-c EDCURL] [-v ENVFILE] [-a AUTH | -u USER] [-s SSLCERT] -lf LINEAGE_FILE [--setup]

optional arguments:
  -h, --help            show this help message and exit
  -c EDCURL, --edcurl EDCURL
                        edc url - including http(s)://<server>:<port>, if not already configured via INFA_EDC_URL
                        environment var
  -v ENVFILE, --envfile ENVFILE
                        .env file with config settings INFA_EDC_URL,INFA_EDC_AUTH etc will over-ride system
                        environment variables. if not specified - '.env' file in current folder will be used
  -a AUTH, --auth AUTH  basic authorization encoded string (preferred over -u) if not already configured via
                        INFA_EDC_AUTH environment var
  -u USER, --user USER  user name - will also prompt for password
  -s SSLCERT, --sslcert SSLCERT
                        ssl certificate (pem format), if not already configured via INFA_EDC_SSL_PEM environment var
  -lf LINEAGE_FILE, --lineage_file LINEAGE_FILE
                        Lineage file to check - results written to same file with _validated.csv suffix
  --setup               setup the connection to EDC by creating a .env file - same as running setupConnection.py
```

