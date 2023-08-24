rem  call the db extractor process

rem -Djavax.net.ssl.trustStore=infa_truststore.jks 
rem -Djavax.net.ssl.trustStorePassword=<password>
rem -Djavax.net.ssl.trustStoreType=JKS

rem for ssl connection
set JAVA_OPTS=-Djavax.net.ssl.trustStore=infa_truststore.jks -Djavax.net.ssl.trustStorePassword=pass2038@infaSSL -Djavax.net.ssl.trustStoreType=JKS

rem set JAVA_OPTS=

rem - with SSL connection
java -cp "lib\*" %JAVA_OPTS% com.infa.eic.sample.LineageSummaryColumns %1





