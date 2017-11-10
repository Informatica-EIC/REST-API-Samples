# REST-API-Samples
This repository contains Java samples for EIC REST API. For instructions on running the samples, see the Readme files in the java directory.

Official REST API documents can be found [here](https://kb.informatica.com/proddocs/Product%20Documentation/6/IN_102_EnterpriseInformationCatalog[REST-API]Reference_en.pdf)

Getting Started
---------------

* Clone this repository
* Try the sample against Informatica Enterprise Information Catalog
* Use 'Issues' to note any bugs or to request new samples
* Lets us know if you have samples that you would like to share here

Sample Programs in the Project
------------------------------
* FuzzyBGAssociater: This program uses the EIC REST API to match technical metadata (Column Names) against similar BG Terms. By using the Fuzzy Name Matching library, this program makes it easy for users to initialize BG term associations against large datasets.
* BGAssociationReport: This program uses the EIC REST API to generate a coverage report of BG terms against specified resources. Using this program, data stewards can quickly get a report on # of columns not associated with BG terms yet.
* BulkClassifier: This program uses the EIC REST API to add values to custom attributes in data assets.
* UnrulyLinker: This program uses the EIC REST API to add lineage links between tables of two resources having same names.

