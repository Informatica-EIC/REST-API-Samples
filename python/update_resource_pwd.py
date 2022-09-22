"""
update_resource_pwd.py
    update the password for an edc resource, with command-line options

Created on Sept 22, 2022

@author: dwrigley

"""

import urllib3
import edcSessionHelper
import argparse
import edcutils
import getpass


urllib3.disable_warnings()


# create edc session helper (global for this script)
edcHelper = edcSessionHelper.EDCSession()

# setup command-line parser
argparser = argparse.ArgumentParser(
    parents=[edcHelper.argparser],
    description="EDC resource password update",
)
argparser.add_argument(
    "-rn",
    "--resource_name",
    required=True,
    help="resource to update the password",
)
argparser.add_argument(
    "-np",
    "--new_password",
    required=False,
    help=("new password to set, optional - if not provided, user will be prompted"),
    # default="",
    type=str,
)

argparser.add_argument(
    "-tc",
    "--test_connect",
    action="store_true",
    required=False,
    help=("execute a test-connect for the newly update resource"),
)


def main():
    # initialize the edc session and common connection parms
    print("update_resource_pwd - starting")
    edcHelper.initUrlAndSessionFromEDCSettings()
    # parse the script specific parameters
    args, unknown = argparser.parse_known_args()
    print(f"args: {args}")

    # check to see if password should be prompted from cmd-line
    if args.new_password is None:
        print(
            "\nno password entered via command-line, prompting for user entered password"
        )
        args.new_password = getpass.getpass(
            prompt=f"password for resource={args.resource_name}: "
        )
        print(f"password, with {len(args.new_password)} characters entered")

    edcutils.updateResourcePasswordUsingSession(
        edcHelper, args.resource_name, args.new_password, args.test_connect
    )


if __name__ == "__main__":
    main()
