@echo off
rem - this script assumes that JAVA_HOME is set, and java is in the system path "java -version"  command should return 1.8 or newer

rem set ssl jvm options (if needed)
rem assumption is $INFA_HOME is set - change settings to suit your environment
rem add a truststore password for the truststore you are using
rem you can use any truststore - like cacerts for your jvm - after importing the denodo certificate
SET SCANNER_TRUSTSTORE=./infa_truststore.jks
SET SCANNER_TRUSTSTORE_PWD=
SET JAVA_OPTS=-Djavax.net.ssl.trustStore=%SCANNER_TRUSTSTORE% -Djavax.net.ssl.trustStorePassword=%SCANNER_TRUSTSTORE_PWD% -Djavax.net.ssl.trustStoreType=JKS

call java %JAVA_OPTS% -cp "lib/*" com.infa.eic.sample.ModelLinker %1

echo Finished
