#!/bin/bash

if [ $# -eq 0 ]
then
   echo 
   echo "modelLinker.sh [propertyFile]"
   exit 0
fi

propfile=$1
# call the model linker 

#ssl if needed
export INFA_TRUSTSTORE=$INFA_HOME/services/shared/security/infa_truststore.jks
export INFA_TRUSTSTORE_PASSWORD=pass2038@infaSSL
 
export JAVA_OPTS="-Djavax.net.ssl.trustStore=$INFA_TRUSTSTORE -Djavax.net.ssl.trustStorePassword=$INFA_TRUSTSTORE_PASSWORD -Djavax.net.ssl.trustStoreType=JKS "
export MY_JAVA_OPTS
 
java ${JAVA_OPTS} -cp "edc_utils-1.0-SNAPSHOT.jar:lib/*" com.infa.eic.sample.ModelLinker ${propfile}


#complete
echo 'Finished'





