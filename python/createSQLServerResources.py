"""
create multiple dbms resources - using an input template
"""
import json
import edcutils
import os
import urllib3

# import edc_parms
import argparse
import getpass
import sys
import requests
from edcSessionHelper import EDCSession

urllib3.disable_warnings()

# global var declaration (with type hinting)
edcSession: EDCSession = None


def processDatabase(aDatabase: str, args: argparse.Namespace):
    """
    read the template file, substituting values for resourceName, Database, userId, pwd
    assumption:  template has the db server and port numbers
    """
    print()
    print(f"create resource for database {aDatabase}")
    resourceName = args.resourcePrefix + aDatabase + args.resourceSuffix
    print(f"\tdatabase = {aDatabase} resource={resourceName}")

    # read the json template
    with open(args.resourceTemplate) as json_data:
        templateJson = json.load(json_data)

    # set the resource name
    templateJson["resourceIdentifier"]["resourceName"] = resourceName
    # set the scanner options (in configOptions)
    for config in templateJson["scannerConfigurations"]:
        for opt in config["configOptions"]:
            optId = opt.get("optionId")
            if optId == "Database":
                opt["optionValues"] = [aDatabase]
            if optId == "User":
                opt["optionValues"] = [args.databaseUser]
            if optId == "Password":
                opt["optionValues"] = [args.databasePassword]
            if optId == "Host" and args.sqlserver_host is not None:
                opt["optionValues"] = [args.sqlserver_host]
                print(f"\tadding host={args.sqlserver_host}")
            if optId == "Port" and args.sqlserver_port is not None:
                opt["optionValues"] = [args.sqlserver_port]
                print(f"\tadding port={args.sqlserver_port}")
            if optId == "Instance" and args.sqlserver_instance is not None:
                opt["optionValues"] = [args.sqlserver_instance]
                print(f"\tadding instance={args.sqlserver_instance}")

    # check/set the profiling connection???
    # also create a profiling connection using infacmd

    resourceUrl = f"{edcSession.baseUrl}/access/1/catalog/resources/{resourceName}"
    print(f"\texecuting get  {resourceUrl}")
    try:
        resp = edcSession.session.get(resourceUrl, params=(), timeout=10)
        if resp.status_code == 200:
            print(f"\tresource {resourceName} already exists, will not be created")
        elif resp.status_code == 401:
            # bad credentuals usually
            print(f"{resp.status_code} status code - probably bad credentials, exiting")
            sys.exit(1)
        elif resp.status_code == 400:
            # not found returns a 400 rc
            print(f"\tstatus_code={resp.status_code} resource does not exist")
            # 500 = resource does not exist
            if args.simulation:
                print(f"\tsim mode - not creating resource {resourceName}")
                return
            # create a new resource
            createRc = edcutils.createResourceUsingSession(
                edcSession.baseUrl, edcSession.session, resourceName, templateJson
            )
            if createRc == 200:
                loadRc, loadJson = edcutils.executeResourceLoadUsingSession(
                    edcSession.baseUrl, edcSession.session, resourceName
                )
                if loadRc == 200:
                    # print(loadJson)
                    print("\tJob Queued: " + loadJson.get("jobId"))
                    print("\tJob def: " + str(loadJson))
            else:
                print(f"error creating resource: rc={loadRc}")
        else:
            print(f"response code {resp.status_code} - not sure what to do, returning")
            return
    except requests.exceptions.RequestException as e:
        print("Exception raised when executing edc query: " + {edcSession.baseUrl})
        print(e)
        return


def readLocalArgs() -> argparse.Namespace:
    # check for args overriding the env vars
    # Note: adding parent arg parser here will also show/use common edc connection vars
    parser = argparse.ArgumentParser(parents=[edcSession.argparser], allow_abbrev=False)
    # add args specific to this utility (resourceName, resourceType, outDir, -sim)
    parser.add_argument(
        "-rt",
        "--resourceTemplate",
        required=True,
        help="resource template json file to use",
    )
    parser.add_argument(
        "-rp",
        "--resourcePrefix",
        default="",
        required=False,
        help=(
            "resource name prefix - <prefix>DatabaseName<suffix>"
            " will be used or the resourcename"
        ),
    )
    parser.add_argument(
        "-rs",
        "--resourceSuffix",
        default="",
        required=False,
        help=(
            "resource name suffix - <prefix>DatabaseName<suffix>"
            " will be used or the resourcename"
        ),
    )
    parser.add_argument(
        "-dbf",
        "--databaseFile",
        required=True,
        help=(
            "text file with a list of databases to process,"
            "if 1st character is # it will be treated as comment"
        ),
    )
    parser.add_argument(
        "-dbu",
        "--databaseUser",
        required=False,
        help=(
            "database username - used to connect to the database,"
            " will prompt for user name if not provided"
        ),
    )
    parser.add_argument(
        "-dbp",
        "--databasePassword",
        required=False,
        help=(
            "database password - used to connect to the database,"
            "will prompt for user name if not provided"
        ),
    )

    parser.add_argument(
        # "-sim",  # note - we can't use -sim shortcut,
        #            argparse will assume -s (for ssl)
        "--simulation",
        action="store_true",
        required=False,
        help=(
            "simulation mode - will tell you what it would do in non-sim mode."
            "but not acutally do it"
        ),
    )

    parser.add_argument(
        "--sqlserver_host",
        required=False,
        help=("sqlserver host name (ip address or hostname)"),
    )
    parser.add_argument(
        "--sqlserver_port",
        required=False,
        type=int,
        help=("sqlserver port (e.g. 1433) if different from template"),
    )
    parser.add_argument(
        "--sqlserver_instance",
        required=False,
        help=("sqlserver instance name (if needed) empty string if not used"),
    )

    args, unknown = parser.parse_known_args()
    # print(f"Reading local args... {args} unknown={unknown}")

    if args.databaseUser is None:
        print("no database user specified...")
        args.databaseUser = input("User Name for database server:")

    if args.databasePassword is None:
        print("\tno password specified... prompting user")
        args.databasePassword = getpass.getpass(
            prompt="password for user=" + args.databaseUser + ":"
        )
        print(f"\tpassword entered with {len(args.databasePassword)} characters")

    return args


def main():
    """
    read command-line args, prompt for db user/pwd if needed
    for each entry in databaseFile (arg) - try to create/execute a resource
    """
    global edcSession
    edcSession = EDCSession()
    edcSession.initUrlAndSessionFromEDCSettings()
    print(f"edc url={edcSession.baseUrl}")

    # read any command-line args
    args = readLocalArgs()
    # print(f"args={args}")
    if args.databaseUser is None or args.databaseUser == "":
        print("no datbase user specified - exiting")
        return

    if args.databasePassword is None or args.databasePassword == "":
        print("no datbase password specified - exiting")
        return

    # check that both files exist (database file and resource template json file)
    if not os.path.isfile(args.databaseFile):
        print(f"database file not found {args.databaseFile}, exiting")
        return
    if not os.path.isfile(args.resourceTemplate):
        print(
            f"resource template (json) file not found {args.resourceTemplate}, exiting"
        )
        return

    # assuming all args are now read - let's do something
    for count, databaseName in enumerate(open(args.databaseFile)):
        # database name is read from file - only process if not starting with #
        if not databaseName.startswith("#") and len(databaseName.strip()) > 0:
            processDatabase(databaseName.strip(), args)


if __name__ == "__main__":
    main()
