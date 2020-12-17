import os
import platform
import csv
from edcSessionHelper import EDCSession
import edcutils
import urllib3
from urllib.parse import urljoin
import json
from timeit import default_timer as timer
import subprocess


urllib3.disable_warnings()
edcSession: EDCSession = EDCSession()
CSPM_CLIENT_HOME = ""
CSPM_CLIENT_BINARY = ""


class mem:
    """ globals class pattern for shared vars """

    updated_list = list()
    failed_list = list()
    failed_aliases = list()
    failed_edc_test = list()
    actions = dict()
    alias_cache = dict()
    edc_actions = dict()
    conn_actions = dict()
    connections_updated = list()


def main():
    """
    processing starts here - assumptions
    - control_file.csv is read to determine what edc resources to read
      and what aliases to use
    """
    if not check_ca_pam_binaries():
        return

    # establish connection/session for EDC, otherwise exit
    if not get_edc_session():
        return

    # read the control file & process each entry
    resources_processed = 0

    control_file = "control_file.csv"
    print(f"\nreading control file: {control_file}")
    current_row = 0
    with open(control_file, "r") as theFile:
        reader = csv.DictReader(theFile)
        for line in reader:
            current_row += 1
            # print(line)
            object_type = line["object_type"]
            object_name = line["object_name"]
            alias_name = line["pam_alias"]
            resources_processed += 1
            print(
                f"\nprocessing row: {current_row} from control file ----------------------------------------"
            )
            process_entry(object_type, object_name, alias_name)

    print(
        f"\nProcess finished: {len(mem.updated_list)+len(mem.connections_updated)}/{resources_processed} updated"
    )
    print(
        f"\t           aliases read: {len(mem.alias_cache)} {list(mem.alias_cache.keys())}"
    )
    print(f"\t      updated resources: {len(mem.updated_list)} {mem.updated_list}")
    print(
        f"\t    updated connections: {len(mem.connections_updated)} {mem.connections_updated}"
    )
    print(f"\t      failed  resources: {len(mem.failed_list)} {mem.failed_list}")
    print(
        f"\tfailed edc test connect: {len(mem.failed_edc_test)} {mem.failed_edc_test}"
    )
    print(f"\t       failed aliases  : {len(mem.failed_aliases)} {mem.failed_aliases}")


def process_entry(object_type, object_name: str, alias_name: str):
    """
    process a control-file entry - to update an edc resource or an isp connection
    """
    cacheflag = ""
    optflag = ""

    print(
        f"\nprocessing resource {object_type} {object_name}"
        f" using alias {alias_name}"
    )
    # get the new password via the pam alias (or read from cache)
    if alias_name in mem.alias_cache:
        print(f"\tusing alias {alias_name} from cache (instead of calling vault again")
        rc = mem.alias_cache[alias_name]["rc"]
        id = mem.alias_cache[alias_name]["id"]
        pwd = mem.alias_cache[alias_name]["pwd"]
    else:
        rc, id, pwd = getCredential(alias_name, cacheflag, optflag)
        mem.alias_cache[alias_name] = {"rc": rc, "id": id, "pwd": pwd}
        print(f"\t\tpassword for alias {alias_name} has {len(pwd)} characters")

    if rc != "400":
        # mem.failed_list.append(object_name)
        if alias_name not in mem.failed_aliases:
            mem.failed_aliases.append(alias_name)
        print(
            f"\tcannot update resource {object_name} "
            f"- return from CA PEM rc={rc} alias={alias_name}"
        )
        return

    # we have a valid alias + password here
    if object_type == "edc_resource":
        if update_edc_resource_pwd(object_name, pwd):
            mem.updated_list.append(object_name)
        else:
            mem.failed_list.append(object_name)
    elif object_type == "infa_connection":
        print(f"\tinfa connection pwd change... {object_name}")
        # encrypt_pwd_infa(pwd)
        if update_isp_connection(object_name, pwd):
            print("\t\t\tConnection updated!")
            mem.connections_updated.append(object_name)
        else:
            print("\t\t\tconmnection update failed :(")
    else:
        print(f"unknown object type {object_type} can't do anything")


def update_isp_connection(connection_name: str, pwd_to_encrypt: str) -> str:
    print(f"\t\tupdating connection {connection_name}")
    if "INFA_HOME" in os.environ:
        INFA_HOME = os.environ["INFA_HOME"]
        print(f"\t\tusing INFA_HOME={INFA_HOME} from environment var")
    else:
        print("\t\tINFA_HOME not set - cannot call infacmd")
        return

    ## assume INFA_DEFAULT_DOMAIN_USER  and INFA_DEFAULT_DOMAIN_PASSWORD (encrypted) is uysed
    if "INFA_DEFAULT_DOMAIN_USER" not in os.environ:
        print("\t\tINFA_DEFAULT_DOMAIN_USER not set - cannot start infacmd")
        return
    if "INFA_DEFAULT_DOMAIN_PASSWORD" not in os.environ:
        print("\t\tINFA_DEFAULT_DOMAIN_PASSWORD not set - cannot start infacmd")
        return

    if platform.system() == "Windows":
        pm_pwd_bin = os.path.join(
            INFA_HOME, "clients\\DeveloperClient\\infacmd\\infacmd.bat"
        )
        cmd_to_execute = f'{pm_pwd_bin} isp updateConnection -dn infa_presales_cloud -cn {connection_name} -cpd "{pwd_to_encrypt}" -Options ""'
    else:
        pm_pwd_bin = os.path.join(INFA_HOME, "server/bin/infacmd.sh")
        cmd_to_execute = f"{pm_pwd_bin} isp updateConnection -dn infa_presales_cloud -cn {connection_name} -cpd '{pwd_to_encrypt}' -Options \"\""

    # print(cmd_to_execute)
    start = timer()
    print(f"\texecuting infacmd isp updateConnection -cn {connection_name}")
    process = subprocess.Popen(
        cmd_to_execute,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    outlines = []
    while True:
        output = process.stdout.readline().strip().decode()
        if output == "" and process.poll() is not None:
            break
        if output:
            outlines.append(output)
        rc = process.poll()
        rc = process.returncode

    end = timer()
    print(f"\t\treturn lines from pmpasswd {len(outlines)} in {end - start} seconds")
    print(f"\t\treturned ={outlines}")
    if outlines[0] == "Command ran successfully.":
        return True
    else:
        return False


def encrypt_pwd_infa(pwd_to_encrypt: str) -> str:
    print("\t\tgenerating encrypted password for infa services/connections...")
    if "INFA_HOME" in os.environ:
        INFA_HOME = os.environ["INFA_HOME"]
        print(f"\t\tusing INFA_HOME={INFA_HOME} from environment var")
    else:
        print("\t\tINFA_HOME not set - cannot encrypt a password")
        return

    if platform.system() == "Windows":
        pm_pwd_bin = os.path.join(INFA_HOME, "clients\\tools\\utils\\pmpasswd.exe")
    else:
        pm_pwd_bin = os.path.join(INFA_HOME, "server/bin/pmpasswd")

    cmd_to_execute = f"{pm_pwd_bin} {pwd_to_encrypt}"
    print(
        f"\t\tgenerating encrypted password using {pm_pwd_bin} <password> {pwd_to_encrypt}"
    )
    process = subprocess.Popen(
        cmd_to_execute,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    outlines = []
    encrypted_pwd = ""
    while True:
        output = process.stdout.readline().strip().decode()
        if output == "" and process.poll() is not None:
            break
        if output:
            outlines.append(output)
            if output.startswith("Encrypted string -->"):
                # print(output)
                encrypted_pwd = output[20:-3]
        rc = process.poll()
        rc = process.returncode

    print(f"\t\treturn lines from pmpasswd {len(outlines)}")
    print(f"\t\treturning encrypted pwd={encrypted_pwd}")
    return encrypted_pwd


def getCredential(alias, cacheflag, optflag):
    """
    call CA PAM given an alias and any options & process the result
    """
    ca_pam_exe = os.path.join(CSPM_CLIENT_HOME, CSPM_CLIENT_BINARY)
    if CSPM_CLIENT_BINARY.endswith(".py"):
        ca_pam_exe = "python " + ca_pam_exe

    # cmd = f"{CSPM_CLIENT_HOME}/{CSPM_CLIENT_BINARY} {alias} {cacheflag} {optflag}"
    cmd = f"{ca_pam_exe} {alias} {cacheflag} {optflag}"
    # print cmd
    print(f"\texecuting call to CA PAM: {cmd}")
    process = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    outlines = []
    while True:
        output = process.stdout.readline().strip().decode()
        if output == "" and process.poll() is not None:
            break
        if output:
            outlines.append(output)
            # print(output)
        rc = process.poll()
        rc = process.returncode

    # a2a_result = a2a_call.stdout.readline().rstrip().decode()
    # print(f"\n\tfinished rc={rc} {outlines}")

    f = os.popen(cmd)
    retVal = f.read().strip()
    # print(f"\treturn value={retVal}")
    # split the result
    rc_parts = retVal.strip().split(" ")
    # print(f"\trc parts... {rc_parts}")
    if rc_parts[0] == "400":
        print(f"\tvalid return code ({rc_parts[0]}) " f"- password can be updated")
    else:
        print(f"\tinvalid return code {rc_parts[0]} exiting")

    return rc_parts[0], rc_parts[1], rc_parts[2]


def check_ca_pam_binaries() -> bool:
    """
    read the 2 CA PAM environment variables
    """
    print(f"checking CA PAM executable for platform={platform.system()}")
    global CSPM_CLIENT_HOME
    global CSPM_CLIENT_BINARY
    if "CSPM_CLIENT_HOME" in os.environ:
        CSPM_CLIENT_HOME = os.environ["CSPM_CLIENT_HOME"]
        print(f"\tusing CSPM_CLIENT_HOME={CSPM_CLIENT_HOME} from environment var")

    if "CSPM_CLIENT_BINARY" in os.environ:
        CSPM_CLIENT_BINARY = os.environ["CSPM_CLIENT_BINARY"]
        print(f"\tusing CSPM_CLIENT_BINARY={CSPM_CLIENT_BINARY} from environment var")

    if CSPM_CLIENT_BINARY == "" or CSPM_CLIENT_HOME == "":
        print("env vars:  CSPM_CLIENT_HOME & CSPM_CLIENT_BINARY not set, exiting...")
        return False

    # check that the executable exists??? (not working for python calls...)
    if not os.path.exists(os.path.join(CSPM_CLIENT_HOME, CSPM_CLIENT_BINARY)):
        print(
            f"\texecutable {os.path.join(CSPM_CLIENT_HOME, CSPM_CLIENT_BINARY)} "
            "does not exist, exiting..."
        )
        return False

    return True


def get_edc_session():
    print("testing connection to EDC")
    edcSession.initUrlAndSessionFromEDCSettings()
    rc, message = edcSession.validateConnection()
    if rc == 200:
        print(
            f"\tEDC Version={edcSession.edcversion_str} "
            f"build:{edcSession.edc_build_vers} date:{edcSession.edc_build_date}"
        )
    else:
        print("\tcannot connect to EDC, returning")
        return False
    return True


def update_edc_resource_pwd(resource_name: str, password: str):
    """
    read the resource, if it exists, update the password
    """

    print(
        f"\n\tstarting process to update edc resource: {resource_name} with new password"  # ,  {password}"
    )

    # get the resource from EDC
    # resourceUrl = f"{edcSession.baseUrl}/access/1/catalog/resources/"
    rc, res_def = edcutils.getResourceDefUsingSession(
        edcSession.baseUrl, edcSession.session, resource_name
    )
    if rc != 200:
        print(f"error reading resource, {rc} {res_def} skipping...")
        return False

    # update the password field - for source metadata scanner
    for config in res_def["scannerConfigurations"]:
        # check that we are editing a source scanner (not profiling)
        scanner_type = config["scanner"]["providerTypeName"]
        if scanner_type == "Source Metadata":
            scanner_id = config["scanner"]["scannerId"]
        # if scanner_type != "Source Metadata":
        #     continue
        for opt in config["configOptions"]:
            optId = opt.get("optionId")
            if optId == "Password" and scanner_type == "Source Metadata":
                opt["optionValues"] = [password]
                print("\t\tpassword replaced")
            if optId == "SourceEdrConnName" and scanner_type == "Data Discovery":
                print(f"\t\tconnection name is {opt.get('optionValues')[0]}")

    # save (PUT) the resource back
    print(f"\tupdating resource {resource_name}")
    start = timer()
    rc = edcutils.updateResourceDefUsingSession(
        edcSession.baseUrl, edcSession.session, resource_name, res_def
    )
    end = timer()
    if rc == 200:
        print(f"\tresource updated in in {end - start} seconds")
        # run a test connect????
        if not edc_resource_test_connect(res_def, resource_name, scanner_id):
            # failed test connect, store the resource name
            mem.failed_edc_test.append(resource_name)
        return True


def edc_resource_test_connect(resource_json, resource_name, scanner_id) -> bool:
    apiURL = urljoin(
        edcSession.baseUrl,
        f"/access/1/catalog/resources/{resource_name}/testconnection",
    )
    # print("\turl=" + apiURL)
    header = {"Accept": "application/json", "Content-Type": "application/json"}
    print("\n\tstarting resource test connect")
    start = timer()
    tResp = edcSession.session.post(
        apiURL,
        params={"scannerid": scanner_id},
        data=json.dumps(resource_json),
        headers=header,
    )
    end = timer()
    print(f"\tresponse={tResp.status_code} in {end - start} seconds")
    if tResp.status_code == 200:
        # valid - return the jsom
        print(f"\ttest connect succeeded:{tResp.status_code} {tResp.text}")
        return True

    # test connect failed... show first part of error message
    chars_to_display = 120
    try:
        # strip off all text after the first \n  (if it exists)
        chars_to_display = tResp.text.index("\\n")
    except ValueError:
        # do nothing (if there is no \n character), use default length (120)
        pass

    print(f"\ttest connect failed: {tResp.text[:chars_to_display]}")
    return False


if __name__ == "__main__":
    main()
