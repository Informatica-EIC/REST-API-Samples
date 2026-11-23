# Find Duplicate Domains

utility script to find and fix domain inferences that were created via API with the wrong provider-id

# Setup

for setup notes - refer to README.MD

TLDR

- requires python 3.6+
- requirements.txt lists dependent python packages (requests & python-dotenv)
- best practice:  create virtual environment
    - python3 -m venv .venv
    - .venv/scripts/activate.ps1    (for windows, powershell)
    - source .venv/bin/activate     (linux & macox)
- to setup edc connectivity 
    python findDuplicateDomains.py --setup
        you will be prompted for edc url:port, userid and pwd - default will save to .env file 

# usage

python findDuplicateDomains.py <options>   (use -h to print all options)

```
findDuplicateDomains.py starting in C:\dev\REST-API-Samples\python
usage: findDuplicateDomains.py [-h] [-c EDCURL] [-v ENVFILE] [-a AUTH | -u USER] [-s SSLCERT] [--setup] [--list] [--remove]
                               [--maxobjects MAXOBJECTS]

optional arguments:
  -h, --help            show this help message and exit
  -c EDCURL, --edcurl EDCURL
                        edc url - including http(s)://<server>:<port>, if not already configured via INFA_EDC_URL
                        environment var
  -v ENVFILE, --envfile ENVFILE
                        .env file with config settings INFA_EDC_URL,INFA_EDC_AUTH etc will over-ride system environment
                        variables. if not specified - '.env' file in current folder will be used
  -a AUTH, --auth AUTH  basic authorization encoded string (preferred over -u) if not already configured via INFA_EDC_AUTH
                        environment var
  -u USER, --user USER  user name - will also prompt for password
  -s SSLCERT, --sslcert SSLCERT
                        ssl certificate (pem format), if not already configured via INFA_EDC_SSL_PEM environment var
  --setup               setup the connection to EDC by creating a .env file - same as running setupConnection.py
  --list                list all objects that have duplicated domains to file named items_with_duplicated_domains.txt
  --remove              remove the duplicated inferred domain, using PATCH api
  --maxobjects MAXOBJECTS
                        max number of objects to remove, for testing. only used when --remove is used
```

- use --list to generate a list of objects that need to be fixed
- use --remove to use the api to remove the duplicated domain inference

details will be written to log/domain_duplication_yyyy_mm_dd_hh-mm-ss.log

# other notes

the ./template folder contains 2 template files used by this utility script

- template/delete_inferred_domain_duplicate_accepted_template.json
- template/delete_inferred_domain_duplicate_rejected_template.json

when a domain is inferred and rejected, extra processing is required to check if there are any other dataelements in the same dataset that use the same data domain.  if there are none, then the link from dataset to domain will also be removed (the rejected template)

