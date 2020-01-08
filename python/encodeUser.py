"""
Created on Aug 10, 2018

Note:  this is the python 3.x version

base64 encode - prompting for security domain, username and password
this is useful for generating the string used for API basic authentication

the user will be prompted for an id and password and optional security domain
& print the encoded result to the console

@author: dwrigley
"""

import base64
import getpass

# assume python 3.x (input vs raw_input)
print("")
print("Enter values for Security Domain (optional), User and Password")
secDomain = input("security domain <empty> for Native:")
u = input("User Name for catalog:")
p = getpass.getpass(prompt="password for user=" + u + ":")
if secDomain == "":
    auth_str = f"{u}:{p}"
else:
    auth_str = f"{secDomain}\\{u}:{p}"
b64_auth_str = base64.b64encode(bytes(auth_str, "utf-8"))

# print result of encoding to console
# print(b64_auth_str.decode("utf-8"))
print("header settings:")
print(f'\t"Authorization": Basic {b64_auth_str.decode("utf-8")}')

print("\nto set an env variable - linux/mac:")
print(f'\texport INFA_EDC_AUTH="Basic {b64_auth_str.decode("utf-8")}"')
print("")
print("for Powershell:")
print(f'\t$env:INFA_EDC_AUTH="Basic {b64_auth_str.decode("utf-8")}"')
print("")
print("for windows cmd:")
print(f'\tset INFA_EDC_AUTH=Basic {b64_auth_str.decode("utf-8")}')
print("")
print("or add to launch.json for vsvode debugging - in the configurations section")
print('\t"envFile": "${workspaceFolder}/.env",')
print(f'\tand add\n\tINFA_EDC_AUTH="Basic {b64_auth_str.decode("utf-8")}",')
print("\tto .env in the root folder of your project")
print("")
print("or add to launch.json for vscode debugging - in the configurations section")
print(f'\t"env" : {{"INFA_EDC_AUTH" : "Basic {b64_auth_str.decode("utf-8")}"}},')

print("finished")
