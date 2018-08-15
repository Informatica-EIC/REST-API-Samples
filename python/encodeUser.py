'''
Created on Aug 10, 2018

base64 encode - prompting for security domain, username and password
this is useful for generating the string used for basic authentication using the rest api

the user will be prompted for an id and password & write the encoded result to the console

@author: dwrigley
'''


import base64
import getpass
import platform


print("python version=" + platform.python_version())
if str(platform.python_version()).startswith("2.7"):
    secDomain = raw_input("security domain <empty> for Native:")
    u = raw_input("User Name for catalog:")
    p = getpass.getpass(prompt='password for user=' + u + ':')
    if secDomain == "":
        auth_str = '%s:%s' % (u, p)
    else:
        auth_str = '%s\%s:%s' % (secDomain, u, p)
    b64_auth_str = base64.b64encode(bytes(auth_str))
else:
    # assume 3.x
    secDomain = input("security domain <empty> for Native:")
    u = input("User Name for catalog:")
    p = getpass.getpass(prompt='password for user=' + u + ':')
    if secDomain == "":
        auth_str = '%s:%s' % (u, p)
    else:
        auth_str = '%s\%s:%s' % (secDomain, u, p)
    b64_auth_str = base64.b64encode(bytes(auth_str, 'utf-8'))
    
#print(auth_str)
print(b64_auth_str.decode("utf-8"))
print('header settings:')
print('    "Authorization": "Basic ' + b64_auth_str.decode("utf-8") + '"')
