Requirements
------------
* IDE's to use to edit/test/build
  * VSCode - https://code.visualstudio.com/
    * with java extension https://marketplace.visualstudio.com/items?itemName=redhat.java 
  * Eclipse
    * Download: http://www.eclipse.org/downloads/eclipse-packages/
* Java SDK  (jdk 1.8)
  * Download: https://openjdk.java.net/install/
  * or https://www.azul.com/downloads/?version=java-8-lts&package=jdk 

* maven
  * https://maven.apache.org/install.html
* Libraries (best to use maven and pom.xml for easiest setup)
  * client.jar: Download from your EIC instance: http://cataloghost:port/access/2/files/client.jar
  * to add to your local maven repository (for your edc version) - e.g. for v10.5.0.1
    * `mvn install:install-file -Dfile=client.jar -DgroupId="com.infa.products.ldm.core.rest.v2.client" -DartifactId=EDC-Rest-Client -Dversion="10.5.2.1" -Dpackaging=jar`
    * and edit pom.xml to use the same version - e.g. 10.5.1,10.5.2.1,10.5.3,10.5.4  (note: it is possible to use older api versions with newer edc, but best to keep them in sync)

  
Getting Started
---------------
* Install the tools in the Requirement Section
* Create a new Eclipse Project and import the Java samples in the project
* Add the Libraries mentioned in the Requirements section to the project
* Open APIUtils.java and provide values for EDC URL, UserName and Password for your EIC Instance
  * Note:  some samples use catalog_utils.properties for configuration (nothing hard-coded in APIUtils.java)
* Ensure EDC is running while executing the samples

SSL Connections
---------------
if your catalog uses TLS (https://<server>:port) - you will need to specify/use a truststore with the service certificate.

you can download and use infa_truststore.jks for this, or create your own truststore and import the certificate

the jvm settings you will need to add are:-
* `-Djavax.net.ssl.trustStore=infa_truststore.jks`   (or whatever your truststore file is, including full path if necessary)
* `-Djavax.net.ssl.trustStorePassword=<password>`
* `-Djavax.net.ssl.trustStoreType=JKS`

following is a sample using vs code & starting the column lineage summary script - infa_truststore.jks is copied to the java folder
```
       {
            "type": "java",
            "name": "Launch LineageSummaryColumns",
            "request": "launch",
            "mainClass": "com.infa.eic.sample.LineageSummaryColumns",
            "projectName": "edc_utils",
            "args": [
                "test_catalog_utils.properties"
            ],
            "vmArgs": [
                "-Djavax.net.ssl.trustStore=infa_truststore.jks",
                "-Djavax.net.ssl.trustStorePassword=<add password here>",
                "-Djavax.net.ssl.trustStoreType=JKS"
            ]
        },

```

Sample Programs in the Project
------------------------------
* FuzzyBGAssociater: This program uses the EIC REST API to match technical metadata (Column Names) against similar BG Terms. By using the Fuzzy Name Matching library, this program makes it easy for users to initialize BG term associations against large datasets.
* BGAssociationReport: This program uses the EIC REST API to generate a coverage report of BG terms against specified resources. Using this program, data stewards can quickly get a report on # of columns not associated with BG terms yet.
* BulkClassifier: This program uses the EIC REST API to add values to custom attributes in data assets.
* UnrulyLinker: This program uses the EIC REST API to add lineage links between tables of two resources having same names.
* UltimateColumnLineageReport: this program retrieves the relational columns which have lineage information along with the origin column of the object.
* CustomAttributeValuesCopier: this program copies a custom attribute value to another custom attribute value for each Columns, Tables and resources
* ObjectFilteredByCustomAttributeValueReport: this program extract all the objects filtered with a specified custom attribute value
* ModelLinker: creates links (either via REST API direct, or custom lineage) between data models (e.g. ERwin, PowerDesigner) and the corresponding DBMS object
* `LineageSummaryColumns` - create a csv file with element level lineage, filters can be used for specific resources