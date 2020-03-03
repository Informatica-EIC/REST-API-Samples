"""
setup a connection environment file .env<string> to use for other edc utility scripts

no command-line input expected - will prompt for all values
"""
import getpass
import base64
import urllib3
import pathlib

from edcSessionHelper import EDCSession

urllib3.disable_warnings()

def main():
    # prompt for catalog url
    print(f"edc api configuration utility:\n")
    catalog_url = input("enter catalog url - http(s):\\<server>:port : ")
    catalog_user = input("enter user id: ")
    p = getpass.getpass(prompt=f"password for user={catalog_user}: ")
    b64_auth_str = base64.b64encode(bytes(f"{catalog_user}:{p}", "utf-8"))
    auth = f'Basic {b64_auth_str.decode("utf-8")}'

    edcSession: EDCSession = EDCSession()

    edcSession.initSession(catalog_url=catalog_url, catalog_auth=auth, verify=None)

    print("\nvalidating that the information you entered is valid...")

    rc, rs_json = edcSession.validateConnection()
    if rc == 200:
        print(f"valid connection\n\t{rs_json}\n")
        print("to make this a repeatable process - you can set the following enviroment variables, or an .env* file")

        print("\nto set an env variable - linux/mac:")
        print(f'\texport INFA_EDC_URL={catalog_url}')
        print(f'\texport INFA_EDC_AUTH="Basic {b64_auth_str.decode("utf-8")}"')
        print("")
        print("for Powershell:")
        print(f'\t$env:INFA_EDC_AUTH={catalog_url}')
        print(f'\t$env:INFA_EDC_AUTH="Basic {b64_auth_str.decode("utf-8")}"')
        print("")
        print("for windows cmd:")
        print(f'\tset INFA_EDC_URL={catalog_url}')
        print(f'\tset INFA_EDC_AUTH=Basic {b64_auth_str.decode("utf-8")}')
        print("")

        print("\nor - create a .env file with those settings.")
        print("\tNote:  if you create a file named '.env' - it will be automatically used by other scripts, or you can over-ride with the -v setting")

        if not pathlib.Path(".env").is_file():
            yes_or_not = input("\na .env file in the current folder does not exist, should i create it? (y or n)?:" )
            if yes_or_not.lower() == 'y':
                print('creating .env.....')
                write_env_file(".env", catalog_url, b64_auth_str)
        else:
            yes_or_not = input("\na .env file already exists, would you like to overwrite it with these settings?: Y or N :")
            if yes_or_not.lower() == 'y':
                print('overwriting .env.....')
                write_env_file(".env", catalog_url, b64_auth_str)
            else:
                yes_or_not = input("\na create/overwrite a different env file (suggest .env_<name>?: Y or N :")
                if yes_or_not.lower() == 'y':
                    env_name = input('env file name to create: e.g. .env_test :')
                    if env_name is not None or env_name != "":
                        write_env_file(env_name, catalog_url, b64_auth_str)
                    else:
                        print("no name entered - can't create file")
    else:
        print(f"connection test failed\n{rs_json}")


def write_env_file(filename, catalog_url, b64_auth_str):
    with open(filename, 'w') as the_file:
        the_file.write(f'INFA_EDC_URL={catalog_url}\n')
        the_file.write(f'INFA_EDC_AUTH="Basic {b64_auth_str.decode("utf-8")}"\n')
    print(f"file created  {filename}")


if __name__ == "__main__":
    main()