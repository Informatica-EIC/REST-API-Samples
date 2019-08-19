"""
Created on Aug 10, 2018

Note:  legacy python version - this is not being actively maintained or tested

base64 encode - prompting for security domain, username and password
this is useful for generating the string used for API basic authentication

the user will be prompted for id/password & write the result to the console

@author: dwrigley
"""


import base64
import getpass
import platform


print("python version=" + platform.python_version())
secDomain = raw_input("security domain <empty> for Native:")
u = raw_input("User Name for catalog:")
p = getpass.getpass(prompt="password for user=" + u + ":")
if secDomain == "":
    auth_str = "%s:%s" % (u, p)
else:
    auth_str = "%s\\%s:%s" % (secDomain, u, p)
b64_auth_str = base64.b64encode(bytes(auth_str))

# print(auth_str)
print(b64_auth_str.decode("utf-8"))
print("header settings:")
print('    "Authorization": "Basic ' + b64_auth_str.decode("utf-8") + '"')

print("to set an env variable:")
print(f'INFA_EDC_AUTH="Basic {b64_auth_str.decode("utf-8")}"')
