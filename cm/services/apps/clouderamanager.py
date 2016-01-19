import os
import time
import threading
# import subprocess
import socket
# import re

from ansible.runner import Runner
from ansible.inventory import Inventory
from cm_api.api_client import ApiResource
from cm_api.api_client import ApiException
# from cm_api.endpoints.clusters import ApiCluster
from cm_api.endpoints.clusters import create_cluster
# from cm_api.endpoints.parcels import ApiParcel
from cm_api.endpoints.parcels import get_parcel
from cm_api.endpoints.cms import ClouderaManager
from cm_api.endpoints.services import ApiService, ApiServiceSetupInfo
# from cm_api.endpoints.services import ApiService, create_service
# from cm_api.endpoints.types import ApiCommand, ApiRoleConfigGroupRef
# from cm_api.endpoints.role_config_groups import get_role_config_group
# from cm_api.endpoints.role_config_groups import ApiRoleConfigGroup
# from cm_api.endpoints.roles import ApiRole
from time import sleep

from cm.util import misc
import cm.util.paths as paths
# from cm.services import ServiceRole
# from cm.services import service_states
from cm.services.apps import ApplicationService
from cm.services import (Service, ServiceDependency, ServiceRole, ServiceType,
                         service_states)

import logging
import urllib2
log = logging.getLogger('cloudman')

NUM_START_ATTEMPTS = 2  # Number of times we attempt to auto-restart the service


class ClouderaManagerService(ApplicationService):

    def __init__(self, app):
        super(ClouderaManagerService, self).__init__(app)
        self.svc_roles = [ServiceRole.CLOUDERA_MANAGER]
        self.name = ServiceRole.to_string(ServiceRole.CLOUDERA_MANAGER)
        self.dependencies = []
        self.remaining_start_attempts = NUM_START_ATTEMPTS
        self.db_pwd = misc.random_string_generator()
        # Indicate if the web server has been configured and started
        self.started = False
        self.cm_port = 7180

        # Default cluster configuration
        # TODO - read local cloud host name!
        # self.cm_host = socket.gethostname()
        self.cm_host = self.app.cloud_interface.get_local_hostname()
        # The actual worker nodes (note: this is a list of Instance objects)
        # (because get_worker_instances currently depends on tags, which is only
        # supported by EC2, get the list of instances only for the case of EC2 cloud.
        # This initialization is applicable only when restarting a cluster.
        # self.host_list = get_worker_instances() if (
        #    self.app.cloud_type == 'ec2' or self.app.cloud_type == 'openstack') else []
        # self.host_list = ["w1", "w2"]
        # self.instances = self.app.manager.worker_instances
        # self.host_list = [l.get_local_hostname() for l in self.app.manager.worker_instances]
        # self.host_list = [l.get_private_ip for l in self.app.manager.worker_instances]
        self.host_list = None
        self.cluster_name = "Cluster 1"
        self.cdh_version = "CDH5"
        self.cdh_version_number = "5"
        self.cm_username = "admin"
        self.cm_password = "admin"
        self.mgmt_service_name = "ManagementService"
        self.host_username = "ubuntu"
        self.host_password = self.app.config.get('password')
        self.cm_repo_url = None
        self.hdfs_service_name = "HDFS"
        self.hadoop_data_dir_prefix = "/mnt/dfs"
        self.yarn_service_name = "YARN"
        self.parcel_version = "5.4.1"
        self.cmd_timeout = 180
        self.api = None
        self.manager = None
        self.cluster = None
        self.hdfs_service = None
        self.yarn_service = None
        self.service_types_and_names = {
            "HDFS": "HDFS",
            "YARN": "YARN"
        }

    @property
    def cm_api_resource(self):
        ar = None
        try:
            ar = ApiResource(self.cm_host, self.cm_port,
                             self.cm_username, self.cm_password)
            ar.echo('Authenticated')  # Issue a sample request to test the conn
        except ApiException, aexc:
            if aexc.code == 401:
                log.debug("Changing default API username to {0}".format(self.cm_username))
                self.cm_username = self.host_username
                self.cm_password = self.host_password
                ar = ApiResource(self.cm_host, self.cm_port,
                                 self.cm_username, self.cm_password)
            else:
                log.error("Api Exception connecting to ClouderaManager: {0}".format(aexc))
        except Exception, exc:
            log.debug("Exception connecting to ClouderaManager: {0}".format(exc))
        return ar

    @property
    def cm_manager(self):
        if self.cm_api_resource:
            return self.cm_api_resource.get_cloudera_manager()
        else:
            log.debug("No cm_api_resource; cannot get cm_manager")
            return None

    def start(self):
        """
        Start Cloudera Manager web server.
        """
        log.debug("Starting Cloudera Manager service")
        self.state = service_states.STARTING
        misc.run('/sbin/sysctl vm.swappiness=0')  # Recommended by Cloudera
        threading.Thread(target=self.__start).start()

    def __start(self):
        """
        Start all the service components.

        Intended to be called in a dedicated thread.
        """
        try:
            self.configure_db()
            self.start_webserver()
            self.set_default_user()
            self.create_cluster()
            self.remaining_start_attempts -= 1
        except Exception, exc:
            log.error("Exception creating a cluster: {0}".format(exc))

    def remove(self, synchronous=False):
        """
        Stop the Cloudera Manager web server.
        """
        log.info("Stopping Cloudera Manager service")
        super(ClouderaManagerService, self).remove(synchronous)
        self.state = service_states.SHUTTING_DOWN
        try:
            if self.cm_api_resource:
                cluster = self.cm_api_resource.get_cluster(self.cluster_name)
                cluster.stop()
        except Exception, exc:
            log.error("Exception stopping cluster {0}: {1}".format(self.cluster_name, exc))
        if misc.run("service cloudera-scm-server stop"):
            self.state = service_states.SHUT_DOWN

    def configure_db(self):
        """
        Add the necessary tables to the default PostgreSQL server running on the
        host and prepare the necessary roles and databases.
        """
        # Update psql settings
        pg_conf = paths.P_PG_CONF
        lif = ["listen_addresses = '*'",
               "shared_buffers = 256MB",
               "wal_buffers = 8MB",
               "checkpoint_segments = 16",
               "checkpoint_completion_target = 0.9"]
        for l in lif:
            log.debug("Updating PostgreSQL conf file {0} setting: {1}".format(pg_conf, l))
            regexp = ' '.join(l.split(' ')[:2])
            try:
                Runner(inventory=Inventory(['localhost']),
                       transport='local',
                       become=True,
                       become_user='postgres',
                       module_name="lineinfile",
                       module_args=('dest={0} backup=yes line="{1}" owner=postgres regexp="{2}"'
                                    .format(pg_conf, l, regexp))
                       ).run()
            except Exception, e:
                log.error("Exception updating psql conf {0}: {1}".format(l, e))
        # Restart psql
        misc.run("service postgresql restart")
        # Add required roles to the main Postgres server
        roles = ['scm', 'amon', 'rman', 'hive']
        for role in roles:
            log.debug("Adding PostgreSQL role {0} (with pwd: {1})".format(role,
                      self.db_pwd))
            try:
                Runner(inventory=Inventory(['localhost']),
                       transport='local',
                       become=True,
                       become_user='postgres',
                       module_name="postgresql_user",
                       module_args=("name={0} role_attr_flags=LOGIN password={1}"
                                    .format(role, self.db_pwd))
                       ).run()
            except Exception, e:
                log.error("Exception creating psql role {0}: {1}".format(role, e))
        # Create required databases
        databases = ['scm', 'amon', 'rman', 'metastore']
        for db in databases:
            owner = db
            if db == 'metastore':
                owner = 'hive'
            log.debug("Creating database {0} with owner {1}".format(db, owner))
            try:
                r = Runner(inventory=Inventory(['localhost']),
                           transport='local',
                           become=True,
                           become_user='postgres',
                           module_name="postgresql_db",
                           module_args=("name={0} owner={1} encoding='UTF-8'"
                                        .format(db, owner))
                           ).run()
                if r.get('contacted', {}).get('localhost', {}).get('failed'):
                    msg = r.get('contacted', {}).get('localhost', {}).get('msg', 'N/A')
                    log.error("Creating the database filed: {0}".format(msg))
            except Exception, e:
                log.error("Exception creating database {0}: {1}".format(db, e))
        # Alter one of the DBs
        sql_cmds = [
            "ALTER DATABASE metastore SET standard_conforming_strings = off"
        ]
        for sql_cmd in sql_cmds:
            misc.run_psql_command(sql_cmd, 'postgres', self.app.path_resolver.psql_cmd, 5432)
        # Prepare the scm database
        cmd = ("/usr/share/cmf/schema/scm_prepare_database.sh -h localhost postgresql scm scm {0}"
               .format(self.db_pwd))
        misc.run(cmd)
        # Make sure we have a clean DB env
        f = '/etc/cloudera-scm-server/db.mgmt.properties'
        if os.path.exists(f):
            log.debug("Deleting file {0}".format(f))
            os.remove(f)

    def start_webserver(self):
        """
        Start the Cloudera Manager web server (defaults to port 7180)
        """
        def _disable_referer_check():
            log.debug("Disabling refered check")
            config = {u'REFERER_CHECK': u'false',
                      u'REMOTE_PARCEL_REPO_URLS': u'http://archive.cloudera.com/cdh5/parcels/5.4.1/'}
            done = False
            self.state = service_states.CONFIGURING
            while not done:
                try:
                    self.cm_manager.update_config(config)
                    log.debug("Succesfully disabled referer check")
                    done = True
                    self.started = True
                except Exception:
                    log.debug("Still have not disabled referer check... ")
                    time.sleep(15)
                    if self.state in [service_states.SHUTTING_DOWN,
                                      service_states.SHUT_DOWN,
                                      service_states.ERROR]:
                        log.debug("Service state {0}; not configuring ClouderaManager."
                                  .format(self.state))
                        done = True

        if misc.run("service cloudera-scm-server start"):
            _disable_referer_check()

    def set_default_user(self):
        """
        Replace the default 'admin' user with a default system one (generally
        ``ubuntu``) and it's password.
        """
        log.info("Setting up default user.")
        host_username_exists = default_username_exists = False
        existing_users = self.cm_api_resource.get_all_users().to_json_dict().get('items', [])
        for existing_user in existing_users:
            if existing_user.get('name', None) == self.host_username:
                host_username_exists = True
            if existing_user.get('name', None) == 'admin':
                default_username_exists = True
        if not host_username_exists:
            log.debug("Setting default user to {0}".format(self.host_username))
            # Create new admin user (use 'ubuntu' and password provided at cloudman startup)
            self.cm_api_resource.create_user(self.host_username, self.host_password, ['ROLE_ADMIN'])
        else:
            log.debug("Admin user {0} exists.".format(self.host_username))
        if default_username_exists:
            # Delete the default 'admin' user
            old_admin = self.cm_username
            self.cm_username = self.host_username
            self.cm_password = self.host_password
            log.debug("Deleting the old default user 'admin'...")
            self.cm_api_resource.delete_user(old_admin)

    def init_cluster(self):
        """
        Create cluster and add hosts.
        """

        log.info("Retrieving the list of hosts...")

        # Add the CM host to the list of hosts to add in the cluster so it can run the management services
        self.host_list = []
        for host in self.app.manager.worker_instances:
            host_ip = host.get_private_ip()
            host_name = host.get_local_hostname()
            self.host_list.append(host_ip)
            log.info("Host: {0}".format(host_name))

        # install hosts on this CM instance
        cmd = self.manager.host_install(self.host_username, self.host_list,
                                           password=self.host_password,
                                           cm_repo_url=self.cm_repo_url)
#                                           java_install_strategy=None)
        log.debug("Installing hosts. This might take a while...")
        while cmd.success is None:
            sleep(5)
            cmd = cmd.fetch()

        if cmd.success is not True:
            log.error("Adding hosts to Cloudera Manager failed: {0}".format(cmd.resultMessage))
        log.debug("Host added to Cloudera Manager")

        log.info("Creating cluster {0}...".format(self.cluster_name))
        self.cluster = self.api.create_cluster(self.cluster_name, version=self.cdh_version)

        log.info("Adding hosts to the cluster...")
        all_hosts = []
        for h in self.api.get_all_hosts():
            all_hosts.append(h.hostname)
            log.info("Host: {0}, {1}, {2}".format(h.hostId, h.hostname, h.ipAddress))

        self.cluster.add_hosts(all_hosts)

        # Update the list of worker hosts - use local hostname retrieved from ClouderaManager API instead of IP addresses
        # remove master host
        self.host_list = all_hosts
        self.host_list.remove(self.cm_host)

        self.cluster = self.api.get_cluster(self.cluster_name)

    # def deploy_management(manager, self):
    def deploy_management(self):
        """
        Create and deploy management services
        """
        mgmt_service_config = {
            'zookeeper_datadir_autocreate': 'true',
        }
        mgmt_role_config = {
            'quorumPort': 2888,
        }
        amon_role_name = "ACTIVITYMONITOR"
        amon_role_conf = {
            'firehose_database_host': self.cm_host + ":7432",
            'firehose_database_user': 'amon',
            'firehose_database_password': self.db_pwd,
            'firehose_database_type': 'postgresql',
            'firehose_database_name': 'amon',
            'firehose_heapsize': '268435456',
        }
        apub_role_name = "ALERTPUBLISER"
        apub_role_conf = {}
        eserv_role_name = "EVENTSERVER"
        eserv_role_conf = {
            'event_server_heapsize': '215964392'
        }
        hmon_role_name = "HOSTMONITOR"
        hmon_role_conf = {}
        smon_role_name = "SERVICEMONITOR"
        smon_role_conf = {}

        mgmt = self.manager.create_mgmt_service(ApiServiceSetupInfo())

        # create roles
        mgmt.create_role(amon_role_name + "-1", "ACTIVITYMONITOR", self.cm_host)
        mgmt.create_role(apub_role_name + "-1", "ALERTPUBLISHER", self.cm_host)
        mgmt.create_role(eserv_role_name + "-1", "EVENTSERVER", self.cm_host)
        mgmt.create_role(hmon_role_name + "-1", "HOSTMONITOR", self.cm_host)
        mgmt.create_role(smon_role_name + "-1", "SERVICEMONITOR", self.cm_host)
        
        # now configure each role
        for group in mgmt.get_all_role_config_groups():
            if group.roleType == "ACTIVITYMONITOR":
                group.update_config(amon_role_conf)
            elif group.roleType == "ALERTPUBLISHER":
                group.update_config(apub_role_conf)
            elif group.roleType == "EVENTSERVER":
                group.update_config(eserv_role_conf)
            elif group.roleType == "HOSTMONITOR":
                group.update_config(hmon_role_conf)
            elif group.roleType == "SERVICEMONITOR":
                group.update_config(smon_role_conf)
            
        # now start the management service
        mgmt.start().wait()
        return mgmt

    def deploy_parcels(self):
        """
        Downloads and distributes parcels
        """

        # get and list all available parcels
        parcels_list = []
        for p in self.cluster.get_all_parcels():
            print '\t' + p.product + ' ' + p.version
            if p.version.startswith(self.cdh_version_number) and p.product == "CDH":
                parcels_list.append(p)

        if len(parcels_list) == 0:
            log.error("No {0} parcel found!".format(self.cdh_version))

        cdh_parcel = parcels_list[0]
        for p in parcels_list:
            if p.version > cdh_parcel.version:
                cdh_parcel = p

        # download the parcel
        log.debug("Starting parcel downloading...")
        cmd = cdh_parcel.start_download()
        if cmd.success is not True:
            log.error("Parcel download failed!")

        # make sure the download finishes
        while cdh_parcel.stage != 'DOWNLOADED':
            sleep(5)
            cdh_parcel = get_parcel(self.cm_api_resource, cdh_parcel.product, cdh_parcel.version, self.cluster_name)

        log.debug("Parcel: {0} {1} downloaded".format(cdh_parcel.product, cdh_parcel.version))

        # distribute the parcel
        log.debug("Distributing parcels...")
        cmd = cdh_parcel.start_distribution()
        if cmd.success is not True:
            log.error("Parcel distribution failed!")

        # make sure the distribution finishes
        while cdh_parcel.stage != "DISTRIBUTED":
            sleep(5)
            cdh_parcel = get_parcel(self.cm_api_resource, cdh_parcel.product, cdh_parcel.version, self.cluster_name)

        log.debug("Parcel: {0} {1} distributed".format(cdh_parcel.product, cdh_parcel.version))

        # activate the parcel
        log.debug("Activating parcels...")
        cmd = cdh_parcel.activate()
        if cmd.success is not True:
            log.error("Parcel activation failed!")

        # make sure the activation finishes
        while cdh_parcel.stage != "ACTIVATED":
            cdh_parcel = get_parcel(self.cm_api_resource, cdh_parcel.product, cdh_parcel.version, self.cluster_name)

        log.debug("Parcel: {0} {1} activated".format(cdh_parcel.product, cdh_parcel.version))
        
    def deploy_hdfs(self):
        """
        Deploys HDFS - NN, DNs, SNN, gateways.
        """

        ### HDFS Configuration ###
        dfs_rep = None
        if len(self.host_list) > 2:
            dfs_rep = 3
        else:
            dfs_rep = len(self.host_list)

        hdfs_config = {
            'dfs_replication': dfs_rep,
            'dfs_permissions': 'false',
            'dfs_block_local_path_access_user': 'mapred,spark',
        }
        hdfs_nn_service_name = "nn"
        hdfs_nn_host = self.cm_host
        hdfs_nn_config = {
            'dfs_name_dir_list': self.hadoop_data_dir_prefix + '/nn', 
            'dfs_namenode_handler_count': 30,
        }
        hdfs_snn_host = self.host_list[1]
        hdfs_snn_config = {
            'fs_checkpoint_dir_list': self.hadoop_data_dir_prefix + '/snn',
        }
        hdfs_dn_hosts = self.host_list
        # dfs_datanode_du_reserved must be smaller than the amount of free space across the data dirs
        # Ideally each data directory will have at least 1TB capacity; they need at least 100GB at a minimum
        # dfs_datanode_failed_volumes_tolerated must be less than the number of different data dirs (ie volumes) in
        # dfs_data_dir_list
        hdfs_dn_config = {
            'dfs_data_dir_list': self.hadoop_data_dir_prefix + '/dn',  # '/datanode',
            'dfs_datanode_handler_count': 30,
            'dfs_datanode_du_reserved': 1073741824,
            'dfs_datanode_failed_volumes_tolerated': 0,
            'dfs_datanode_data_dir_perm': 755,
        }
        hdfs_gw_hosts = self.host_list
        hdfs_gw_hosts.append(self.cm_host)
        hdfs_gw_config = {
            'dfs_client_use_trash': 'true'
        }

        self.hdfs_service = self.cluster.create_service(self.hdfs_service_name, "HDFS")
        self.hdfs_service.update_config(hdfs_config)

        nn_role_group = self.hdfs_service.get_role_config_group("{0}-NAMENODE-BASE".format(self.hdfs_service_name))
        nn_role_group.update_config(hdfs_nn_config)
        nn_service_pattern = "{0}-" + hdfs_nn_service_name
        self.hdfs_service.create_role(nn_service_pattern.format(self.hdfs_service_name), "NAMENODE", hdfs_nn_host)

        snn_role_group = self.hdfs_service.get_role_config_group("{0}-SECONDARYNAMENODE-BASE".format(self.hdfs_service_name))
        snn_role_group.update_config(hdfs_snn_config)
        self.hdfs_service.create_role("{0}-snn".format(self.hdfs_service_name), "SECONDARYNAMENODE", hdfs_snn_host)

        dn_role_group = self.hdfs_service.get_role_config_group("{0}-DATANODE-BASE".format(self.hdfs_service_name))
        dn_role_group.update_config(hdfs_dn_config)

        gw_role_group = self.hdfs_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.hdfs_service_name))
        gw_role_group.update_config(hdfs_gw_config)

        datanode = 0
        for host in hdfs_dn_hosts:
            datanode += 1
            self.hdfs_service.create_role("{0}-dn-".format(self.hdfs_service_name) + str(datanode), "DATANODE", host)

        gateway = 0
        for host in hdfs_gw_hosts:
            gateway += 1
            self.hdfs_service.create_role("{0}-gw-".format(self.hdfs_service_name) + str(gateway), "GATEWAY", host)

    def init_hdfs(self):
        """
        Initializes HDFS - format the file system
        """
        hdfs = self.cluster.get_service(self.hdfs_service_name)
        cmd = hdfs.format_hdfs("{0}-nn".format(self.hdfs_service_name))[0]
        if not cmd.wait(self.cmd_timeout).success:
            log.info("WARNING: Failed to format HDFS, attempting to continue with the setup")

    def deploy_yarn(self):
        """
        Deploys YARN - RM, JobHistoryServer, NMs, gateways
        """

        ### YARN Configuration ###
        yarn_service_config = {
            'hdfs_service': self.hdfs_service_name,
        }
        yarn_rm_host = self.cm_host
        yarn_rm_config = {}
        yarn_jhs_host = self.cm_host
        yarn_jhs_config = {}
        yarn_nm_hosts = self.host_list
        yarn_nm_config = {
            'yarn_nodemanager_local_dirs': self.hadoop_data_dir_prefix + '/yarn/nm',
        }
        yarn_gw_hosts = self.host_list
        yarn_gw_config = {
            'mapred_submit_replication': min(3, len(yarn_gw_hosts))
        }

        self.yarn_service = self.cluster.create_service(self.yarn_service_name, "YARN")
        self.yarn_service.update_config(yarn_service_config)

        rm = self.yarn_service.get_role_config_group("{0}-RESOURCEMANAGER-BASE".format(self.yarn_service_name))
        rm.update_config(yarn_rm_config)
        self.yarn_service.create_role("{0}-rm".format(self.yarn_service_name), "RESOURCEMANAGER", yarn_rm_host)

        jhs = self.yarn_service.get_role_config_group("{0}-JOBHISTORY-BASE".format(self.yarn_service_name))
        jhs.update_config(yarn_jhs_config)
        self.yarn_service.create_role("{0}-jhs".format(self.yarn_service_name), "JOBHISTORY", yarn_jhs_host)

        nm = self.yarn_service.get_role_config_group("{0}-NODEMANAGER-BASE".format(self.yarn_service_name))
        nm.update_config(yarn_nm_config)

        nodemanager = 0
        for host in yarn_nm_hosts:
            nodemanager += 1
            self.yarn_service.create_role("{0}-nm-".format(self.yarn_service_name) + str(nodemanager), "NODEMANAGER", host)

        gw = self.yarn_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.yarn_service_name))
        gw.update_config(yarn_gw_config)

        gateway = 0
        for host in yarn_gw_hosts:
            gateway += 1
            self.yarn_service.create_role("{0}-gw-".format(self.yarn_service_name) + str(gateway), "GATEWAY", host)

    def post_startup(self):
        """
        Executes steps that need to be done after the final startup once everything is deployed and running.
        """
        # Create HDFS temp dir
        self.hdfs_service.create_hdfs_tmp()

        # give the create db command time to complete
        sleep(30)

        # Deploy client configs to all necessary hosts
        cmd = self.cluster.deploy_client_config()
        if not cmd.wait(self.cmd_timeout).success:
            log.info("Failed to deploy client configs for {0}".format(self.cluster_name))

        # Now change permissions on the /user dir so YARN will work
        shell_command = ['sudo -u hdfs hadoop fs -chmod 775 /user']
        misc.run(shell_command)
        # user_chmod_output = Popen(shell_command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()

    def create_cluster(self):
        """
        Create a cluster and Cloudera Manager Service on master host
        """
        log.info("Creating Cloudera cluster: '{0}'. Please wait...".format(self.cluster_name))

        ### CM Definitions ###
        CM_CONFIG = {
            'TSQUERY_STREAMS_LIMIT': 1000,
        }
        
        self.api = self.cm_api_resource
        
        self.manager = self.api.get_cloudera_manager()

        ### Create and deploy new cluster ##
        log.info("Creating and deplying new cluster...")
        ## Initialize a cluster ##
        log.info("Initializing the cluster...")
        self.init_cluster()

        log.info("Cloudera cluster: {0} initialized".format(self.cluster_name) + " which uses CDH version " + self.cdh_version_number)

       ## Deploy management service ##
        log.info("Deploying management service...")
        self.deploy_management()
        log.info("Deployed CM management service " + self.mgmt_service_name + " to run on " + self.cm_host)

        ## Downloading, distribute and active parcels ##
        log.info("Installing parcels...")
        self.deploy_parcels()
        log.info("Parcels are downloaded and distributed.")

        # Deploying hadoop services ##
        log.info("Deploying hdfs...")
        self.hdfs_service = self.deploy_hdfs()
        log.info("Deployed HDFS service " + self.hdfs_service_name)
        self.init_hdfs()
        log.info("Initialized HDFS service")

        log.info("Deploying yarn...")
        self.yarn_service = self.deploy_yarn()
        log.info("Deployed YARN service " + self.yarn_service_name)

        log.info("About to restart cluster.")
        self.cluster.stop().wait()
        self.cluster.start().wait()
        log.info("Done restarting cluster.")

        # inspect hosts and print the result
        log.debug("Inspecting hosts. This might take a few minutes")

        cmd = self.cm_manager.inspect_hosts()
        while cmd.success is None:
            sleep(5)
            cmd = cmd.fetch()

        if cmd.success is not True:
            log.error("Host inpsection failed!")

        log.debug("Hosts successfully inspected:\n".format(cmd.resultMessage))
        log.info("Cluster '{0}' installed".format(self.cluster_name))

    def status(self):
        """
        Check and update the status of the service.
        """
        if self.state == service_states.UNSTARTED or \
           self.state == service_states.STARTING or \
           self.state == service_states.SHUTTING_DOWN or \
           self.state == service_states.SHUT_DOWN or \
           self.state == service_states.WAITING_FOR_USER_ACTION:
            return
        # Capture possible status messages from /etc/init.d/cloudera-scm-server
        status_output = ['is dead and pid file exists',
                         'is dead and lock file exists',
                         'is not running',
                         'status is unknown']
        svc_status = misc.getoutput('service cloudera-scm-server status', quiet=True)
        for so in status_output:
            if so in svc_status:
                log.warning("Cloudera server not running: {0}.".format(so))
                if self.remaining_start_attempts > 0:
                    log.debug("Resetting ClouderaManager service")
                    self.state = service_states.UNSTARTED
                else:
                    log.error("Exceeded number of restart attempts; "
                              "ClouderaManager service in ERROR.")
                    self.state = service_states.ERROR
        if not self.started:
            pass
        elif 'is running' in svc_status:
            self.state = service_states.RUNNING
            # Once the service gets running, reset the number of start attempts
            self.remaining_start_attempts = NUM_START_ATTEMPTS
