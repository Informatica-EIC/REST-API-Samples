"""
create multiple dbms resources - using an input template
"""
import edcutils
import os

import argparse
import sys
import requests
from edcSessionHelper import EDCSession
import urllib3


# global var declaration (with type hinting)
edcSession: EDCSession = None
urllib3.disable_warnings()


def deleteResource(aResource: str, args):
    """
    delete the resource
    """
    print(f"delete resource {aResource}")

    resourceUrl = f"{edcSession.baseUrl}/access/1/catalog/resources/{aResource}"
    print(f"executing delete  {resourceUrl}")
    if args.simulation:
        print(f"\tsim mode - will not delete resource {aResource}, exiting")
        return

    try:
        resp = edcSession.session.delete(resourceUrl, params=(), timeout=10)
        if resp.status_code == 200:
            print(f"resource {aResource} delete added to queue")
            print(resp.text)
        elif resp.status_code == 401:
            # bad credentuals usually
            print(f"{resp.status_code} status code - probably bad credentials, exiting")
            sys.exit(1)
        elif resp.status_code == 500:
            print(f"rc=500, resource {aResource} does not exist")
            return
        else:
            print(f"response code {resp.status_code} - not sure what to do, returning")
            print(f"response=\n{resp.json()}")
            return
    except requests.exceptions.RequestException as e:
        print(f"Exception raised when executing edc query: {edcSession.baseUrl}")

        print(e)
        return


def readLocalArgs() -> argparse.Namespace:
    # check for args overriding the env vars
    # Note: adding parent arg parser here will also show/use common edc connection vars
    parser = argparse.ArgumentParser(parents=[edcSession.argparser])
    # add args specific to this utility (resourceName, resourceType, outDir, -sim)

    parser.add_argument(
        "-f",
        "--resourceFile",
        required=False,
        help=(
            "text file with a list of resoures to process,"
            "if 1st character is # it will be treated as comment"
        ),
    )

    parser.add_argument(
        "--all", action="store_true",
        help="find and delete all non-system resources"
    )

    parser.add_argument(
        "-sim",
        "--simulation",
        action="store_true",
        required=False,
        help=(
            "simulation mode - will tell you what it would do in non-sim mode."
            "but not acutally do it"
        ),
    )

    args, unknown = parser.parse_known_args()
    # print(f"Reading local args... {args} unknown={unknown}")

    return args


def main():
    """
    read command-line args, prompt for db user/pwd if needed
    for each entry in databaseFile (arg) - try to create/execute a resource
    """
    global edcSession
    edcSession = EDCSession()
    edcSession.initUrlAndSessionFromEDCSettings()
    print(edcSession.baseUrl)

    # read any command-line args
    args = readLocalArgs()
    print(f"args={args}")

    # check that both files exist (database file and resource template json file)
    if not os.path.isfile(args.resourceFile):
        print(f"resource file not found {args.resourceFile}, exiting")
        return

    # assuming all args are now read - let's do something
    for count, resourceName in enumerate(open(args.resourceFile)):
        # database name is read from file - only process if not starting with #
        if not resourceName.startswith("#") and len(resourceName.strip()) > 0:
            deleteResource(resourceName.strip(), args)


if __name__ == "__main__":
    main()
