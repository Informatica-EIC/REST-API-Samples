# integration with EDC and CA PAM

## purpose
create a script that can update EDC resources and platform connctions (for profiling) with new passwords that are managed/stored within CA PAM

# setup
- download 
- set the following environment variables:-
  - for CA PAM
    - CSPM_CLIENT_HOME folder where cspmclient executable is installed
    - CSPM_CLIENT_BINARY name of the ca pam executable to call
  - for Enterprise Data Catalog
    - INFA_EDC_URL edc url e.g. https://napslxapp01:9085
    - INFA_EDC_AUTH  base64 encoded credentials for an EDC user with api and resource update privileges (use encodeUser.py to create)
  - for infa platform connections infacmd
     - INFA_DEFAULT_DOMAIN_USER  user id to connect and update connections
     - INFA_DEFAULT_DOMAIN_PASSWORD encrypted password for infa user - use pmpasswd to create  `$INFA_HOME//server/bin/pmpasswd <password>`

# control file

a file named control_file.csv will control the process, it will contain a list of the PAM aliases and the edc resource name or the platform connection name.
the control file as 3 columns:- 
- object_type - either edc_resource or infa_connection - determines what is updated
- object_name - name of the edc resource or infa connection
- pam_alias - alias to use to call CA PAM to get the password

# run the process
the `edc_ca_pam` binary file is called with no parameters it will use the environment variables and control_file.csv

# sample output

```
edc_ca_pam
checking CA PAM executable for platform=Linux
        using CSPM_CLIENT_HOME=/home/infa/ca_pam/cspm from environment var
        using CSPM_CLIENT_BINARY=cspmclient.sh from environment var
testing connection to EDC
        reading common env/env file/cmd settings
                using EDC URL=https://napslxapp01:9554 from INFA_EDC_URL env var
                using INFA_EDC_AUTH from environment
ready to check .env file .env
isfile False
        finished reading common env/.env/cmd parameters
validating connection to https://napslxapp01:9554
        api status code=200
        EDC Version=10.4.1.2 build:33 date:Fri Oct 16 14:53:51 UTC 2020

reading control file: control_file.csv

processing row: 1 from control file ----------------------------------------

processing resource edc_resource oracle_acme_crm using alias alias2
        executing call to CA PAM: /home/infa/ca_pam/cspm/cspmclient.sh alias2
        valid return code (400) - password can be updated
                password for alias alias2 has 12 characters

        starting process to update edc resource: oracle_acme_crm with new password
        getting resource for catalog:-https://napslxapp01:9554 resource=oracle_acme_crm
        response=200
                password replaced
                connection name is infaedw_jdbc_acme_crm
        updating resource oracle_acme_crm
        updating resource for catalog:-https://napslxapp01:9554 resource=oracle_acme_crm
        resource successfully updated, rc=200
        resource updated in in 1.00181961799899 seconds

        starting resource test connect
        response=200 in 6.23412064799777 seconds
        test connect succeeded:200 true

processing row: 2 from control file ----------------------------------------

processing resource infa_connection sqlserver_db_one using alias sqlserver_dev
        executing call to CA PAM: /home/infa/ca_pam/cspm/cspmclient.sh sqlserver_dev
        valid return code (400) - password can be updated
                password for alias sqlserver_dev has 9 characters
        infa connection pwd change... sqlserver_db_one
                updating connection sqlserver_db_one
                using INFA_HOME=/opt/infa/10.4 from environment var
        executing infacmd isp updateConnection -cn sqlserver_db_one
                return lines from pmpasswd 1 in 2.8030991410014394 seconds
                returned =['[ICMD_10033] Command [updateConnection] failed with error [[INFACMD_40000] [DTF_0020] The [UserManagementService] Service failed to authenticate the user with the error message [[UM_10205] Failed to authenticate the user [admin] that belongs to the security domain [Native]. For more information, see the domain logs.].].']
                        conmnection update failed :(

processing row: 3 from control file ----------------------------------------

processing resource edc_resource sqlserver_db_one using alias sqlserver_devxx
        executing call to CA PAM: /home/infa/ca_pam/cspm/cspmclient.sh sqlserver_devxx
        invalid return code 100 exiting
                password for alias sqlserver_devxx has 5 characters
        cannot update resource sqlserver_db_one - return from CA PEM rc=100 alias=sqlserver_devxx

Process finished: 1/3 updated
                   aliases read: 3 ['alias2', 'sqlserver_dev', 'sqlserver_devxx']
              updated resources: 1 ['oracle_acme_crm']
            updated connections: 0 []
              failed  resources: 0 []
        failed edc test connect: 0 []
               failed aliases  : 1 ['sqlserver_devxx']

```

