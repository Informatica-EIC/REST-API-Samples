Requirements
------------
* Eclipse
  * Download: http://www.eclipse.org/downloads/eclipse-packages/
* Java SDK
  * Download: http://www.oracle.com/technetwork/java/javase/downloads/index.html
* Libraries
  * client.jar: Download from your EIC instance: http://cataloghost:port/access/2/files/client.jar
  * fuzzywuzzy.jar: https://github.com/xdrop/fuzzywuzzy/releases
  * Opencsv.jar: https://sourceforge.net/projects/opencsv/
  
Getting Started
---------------
* Install the tools in the Requirement Section
* Create a new Eclipse Project and import the Java samples in the project
* Add the Libraries mentioned in the Requirements section to the project
* Open APIUtils.java and provide values for EIC URL, UserName and Password for your EIC Instance
* Ensure EIC is running while executing the samples

Sample Programs in the Project
------------------------------
* FuzzyBGAssociater: This program uses the EIC REST API to match technical metadata (Column Names) against similar BG Terms. By using the Fuzzy Name Matching library, this program makes it easy for users to initialize BG term associations against large datasets.
* BGAssociationReport: This program uses the EIC REST API to generate a coverage report of BG terms against specified resources. Using this program, data stewards can quickly get a report on # of columns not associated with BG terms yet.
* BulkClassifier: This program uses the EIC REST API to add values to custom attributes in data assets.
* UnrulyLinker: This program uses the EIC REST API to add lineage links between tables of two resources having same names.

