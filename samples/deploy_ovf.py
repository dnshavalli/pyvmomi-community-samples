#!/usr/bin/env python
"""
 Written by Tony Allen
 Github: https://github.com/stormbeard
 Blog: https://stormbeard.net/
 This code has been released under the terms of the Apache 2 licenses
 http://www.apache.org/licenses/LICENSE-2.0.html

 Script to deploy VM via a single .ovf and a single .vmdk file.
"""
import ssl
import time
import requests
from os import system, path
from sys import exit
from threading import Thread
from time import sleep
from argparse import ArgumentParser
from getpass import getpass

from pyVim import connect
from pyVmomi import vim


def get_args():
    """
    Get CLI arguments.
    """
    parser = ArgumentParser(description='Arguments for talking to vCenter')

    parser.add_argument('-s', '--host',
                        required=True,
                        action='store',
                        help='vSphere service to connect to.')

    parser.add_argument('-o', '--port',
                        type=int,
                        default=443,
                        action='store',
                        help='Port to connect on.')

    parser.add_argument('-u', '--user',
                        required=True,
                        action='store',
                        help='Username to use.')

    parser.add_argument('-p', '--password',
                        required=False,
                        action='store',
                        help='Password to use.')

    parser.add_argument('--datacenter_name',
                        required=False,
                        action='store',
                        default=None,
                        help=("Name of the Datacenter you "
                              "wish to use. If omitted, the first "
                              "datacenter will be used.")
                        )

    parser.add_argument('--datastore_name',
                        required=False,
                        action='store',
                        default=None,
                        help=("Datastore you wish the VM to be deployed to. "
                              "If left blank, VM will be put on the first "
                              "datastore found.")
                        )

    parser.add_argument('--cluster_name',
                        required=False,
                        action='store',
                        default=None,
                        help=("Name of the cluster you wish the VM to "
                              "end up on. If left blank the first cluster found "
                              "will be used")
                        )

    parser.add_argument('-v', '--vmdk_path',
                        required=True,
                        action='store',
                        default=None,
                        help='Path of the VMDK file to deploy.')

    parser.add_argument('-f', '--ovf_path',
                        required=True,
                        action='store',
                        default=None,
                        help='Path of the OVF file to deploy.')

    parser.add_argument('--disable_ssl_verification',
                        required=False,
                        action='store_true',
                        default=False,
                        help=("WARNING! INSECURE: Ignore certificate errors "
                              "from vSphere. Boolean flag. Default=False")
                        )


    args = parser.parse_args()

    if not args.password:
        args.password = getpass(prompt='Enter password: ')

    return args


def get_ovf_descriptor(ovf_path):
    """
    Read in the OVF descriptor.
    """
    if path.exists(ovf_path):
        with open(ovf_path, 'r') as f:
            try:
                ovfd = f.read()
                f.close()
                return ovfd
            except:
                print "Could not read file: %s" % ovf_path
                exit(1)


def get_obj_in_list(obj_name, obj_list):
    """
    Gets an object out of a list (obj_list) whos name matches obj_name.
    """
    for o in obj_list:
        if o.name == obj_name:
            return o
    print ("Unable to find object by the name of %s in list:\n%s" %
           (o.name, map(lambda o: o.name, obj_list)))
    exit(1)


def get_objects(si, args):
    """
    Return a dict containing the necessary objects for deployment.
    """
    # Get datacenter object.
    datacenter_list = si.content.rootFolder.childEntity
    if args.datacenter_name:
        datacenter_obj = get_obj_in_list(args.datacenter_name, datacenter_list)
    else:
        datacenter_obj = datacenter_list[0]

    # Get datastore object.
    datastore_list = datacenter_obj.datastoreFolder.childEntity
    if args.datastore_name:
        datastore_obj = get_obj_in_list(args.datastore_name, datastore_list)
    elif len(datastore_list) > 0:
        datastore_obj = datastore_list[0]
    else:
        print "No datastores found in DC (%s)." % datacenter_obj.name

    # Get cluster object.
    cluster_list = datacenter_obj.hostFolder.childEntity
    if args.cluster_name:
        cluster_obj = get_obj_in_list(args.cluster_name, cluster_list)
    elif len(cluster_list) > 0:
        cluster_obj = cluster_list[0]
    else:
        print "No clusters found in DC (%s)." % datacenter_obj.name

    # Generate resource pool.
    resource_pool_obj = cluster_obj.resourcePool

    return {"datacenter": datacenter_obj,
            "datastore": datastore_obj,
            "resource pool": resource_pool_obj}


def keep_lease_alive(lease):
    """
    Keeps the lease alive while POSTing the VMDK.
    """
    while(True):
        sleep(5)
        try:
            # Choosing arbitrary percentage to keep the lease alive.
            lease.HttpNfcLeaseProgress(50)
            if (lease.state == vim.HttpNfcLease.State.done):
                return
            # If the lease is released, we get an exception.
            # Returning to kill the thread.
        except:
            return


def upload_file(service_instance):
    '''
    This function sourced and modified from:
    ../pyvmomi-community-samples/samples/upload_file_to_datastore.py
    '''
    content = service_instance.RetrieveContent()
    session_manager = content.sessionManager

    # Get the list of all datacenters we have available to us
    datacenters_object_view = content.viewManager.CreateContainerView(
        content.rootFolder,
        [vim.Datacenter],
        True)

    # Find the datastore and datacenter we are using
    datacenter = None
    datastore = None
    for dc in datacenters_object_view.view:
        datastores_object_view = content.viewManager.CreateContainerView(
            dc,
            [vim.Datastore],
            True)
        for ds in datastores_object_view.view:
            if ds.info.name == args.datastore:
                datacenter = dc
                datastore = ds
    if not datacenter or not datastore:
        print("Could not find the datastore specified")
        raise SystemExit(-1)
    # Clean up the views now that we have what we need
    datastores_object_view.Destroy()
    datacenters_object_view.Destroy()

    # Build the url to put the file - https://hostname:port/resource?params
    if not args.remote_file.startswith("/"):
        remote_file = "/" + args.remote_file
    else:
        remote_file = args.remote_file
    resource = "/folder" + remote_file
    params = {"dsName": datastore.info.name,
              "dcPath": datacenter.name}
    http_url = "https://" + args.host + ":443" + resource

    # Get the cookie built from the current session
    client_cookie = service_instance._stub.cookie
    # Break apart the cookie into it's component parts - This is more than
    # is needed, but a good example of how to break apart the cookie
    # anyways. The verbosity makes it clear what is happening.
    cookie_name = client_cookie.split("=", 1)[0]
    cookie_value = client_cookie.split("=", 1)[1].split(";", 1)[0]
    cookie_path = client_cookie.split("=", 1)[1].split(";", 1)[1].split(
        ";", 1)[0].lstrip()
    cookie_text = " " + cookie_value + "; $" + cookie_path
    # Make a cookie
    cookie = dict()
    cookie[cookie_name] = cookie_text

    # Get the request headers set up
    headers = {'Content-Type': 'application/octet-stream'}

    # Get the file to upload ready, extra protection by using with against
    # leaving open threads
    with open(args.vmdk_path, "rb") as f:
        # Connect and upload the file
        request = requests.put(http_url,
                               params=params,
                               data=f,
                               headers=headers,
                               cookies=cookie,
                               verify=args.disable_ssl_verification)

def main():
    args = get_args()
    ovfd = get_ovf_descriptor(args.ovf_path)
    try:
        if args.disable_ssl_verification:
            context = ssl._create_unverified_context()
            verify = False
            requests.packages.urllib3.disable_warnings()
        else:
            context = None
            verify = True
        si = connect.SmartConnect(host=args.host,
                                  user=args.user,
                                  pwd=args.password,
                                  port=args.port,
                                  sslContext=context)
    except Exception as e:
        print "Unable to connect to '%s'. Exception: '%s'" % (args.host,str(e))
        exit(1)
    objs = get_objects(si, args)
    manager = si.content.ovfManager
    spec_params = vim.OvfManager.CreateImportSpecParams()
    import_spec = manager.CreateImportSpec(ovfd,
                                           objs["resource pool"],
                                           objs["datastore"],
                                           spec_params)
    lease = objs["resource pool"].ImportVApp(import_spec.importSpec, objs["datacenter"].vmFolder)
    while(True):
        if (lease.state == vim.HttpNfcLease.State.ready):
            # Assuming single VMDK.
            print("Lease state ready...Attempting upload...")
            url = lease.info.deviceUrl[0].url.replace('*', args.host)
            print(url)
            # Spawn a dawmon thread to keep the lease active while POSTing
            # VMDK.
            keepalive_thread = Thread(target=keep_lease_alive, args=(lease,))
            keepalive_thread.start()
            # POST the VMDK to the host via curl. Requests library would work
            # too.
            '''
            curl_cmd = (
                "curl -Ss -X POST --insecure -T %s -H 'Content-Type: "
                "application/x-vnd.vmware-streamVmdk' %s" %
                (args.vmdk_path, url))
            system(curl_cmd)
            '''
            headers = {'Content-Type': 'application/x-vnd.vmware-streamVmdk'}
            try:
                with open(args.vmdk_path,'rb') as f:
                    r = requests.put(url, data=f, headers=headers, verify=verify)
                    print(r.status_code)
                    print(r.content)
            except Exception as ex:
                print("Exception opening vmdk or uploading vmdk: %s" % str(ex))
                exit(1)
            lease.HttpNfcLeaseComplete()
            keepalive_thread.join()
            return 0
        elif (lease.state == vim.HttpNfcLease.State.error):
            print "Lease error: " + lease.state.error
            exit(1)
    connect.Disconnect(si)

if __name__ == "__main__":
    exit(main())
