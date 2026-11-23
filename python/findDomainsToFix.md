# Find Domain inferences that need to be fixed

utility script to look at all inferred data domains, and determin whether they were created with the right provider id and attribute.

Note:  the correct provider id is : `DDPScanner` and the `com.infa.ldm.profiling.infDataDomain` attribute should have a value

# Setup

for setup notes - refer to README.MD

TLDR

- requires python 3.6+
- requirements.txt lists dependent python packages (requests & python-dotenv)
- best practice:  create virtual environment
    - python3 -m venv .venv
    - .venv/scripts/activate.ps1    (for windows, powershell)
    - source .venv/bin/activate     (linux & macox)
- to setup edc connectivity   (one time effort for all scripts)
    python findDomainsToFix.py --setup
        you will be prompted for edc url:port, userid and pwd - default will save to .env file 

# usage

python findDomainsToFix.py (use -h to print all options)

```
findDomainsToFix.py starting in C:\dev\REST-API-Samples\python
usage: findDomainsToFix.py [-h] [-c EDCURL] [-v ENVFILE] [-a AUTH | -u USER] [-s SSLCERT] [--setup] [--pagesize PAGESIZE]

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
  --pagesize PAGESIZE   pageSize setting to use when executing an objects query, default 300
  ```

- use --pagesize to change the defult page size of 300

output written to items_with_domain_inf_errors.csv with the following columns:-
- id   object id where the domain inference was found
- domain  the domain that is inferred
- providerId  proividerId used to create the domain inference
- infDataDomain attr  contents of the `com.infa.ldm.profiling.infDataDomain` attribute
- modified_by  user-id that created the domain inference
- is_accepted  boolean - is the domain also accepted
- is_rejected  boolean - is the domain also rejected
- fix_required boolean - does the domain inference require fixing

