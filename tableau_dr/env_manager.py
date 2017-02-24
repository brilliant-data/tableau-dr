"""
tableau-dr
Copyright (C) 2016 brilliant-data.com

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import pwd
import subprocess
import shlex
from crontab import CronTab
import defaults as d
import logging
import os
import tempfile
import shutil
import time
import psutil
from requests_kerberos.exceptions import KerberosExchangeError
import urllib
import socket
from winrm.exceptions import BasicAuthDisabledError, InvalidCredentialsError, AuthenticationError, \
    WinRMError, WinRMTransportError, WinRMOperationTimeoutError
from bs4 import BeautifulSoup
import platform
import re
import utils
import uuid

# Custom exceptions
class ValidateEnvironmentException(Exception):
    pass


class EnvironmentManagerException(Exception):
    pass


# Object for managing the Disaster Recovery environment
class EnvironmentManager:

    # Constructor
    def __init__(self,
                 rescue_user,
                 is_sudoer,
                 pg_data_root_dir,
                 cluster_source_root_dir,
                 sync_root_dir,
                 cluster_target_root_dir,
                 tab_data_config_dir,
                 mount_dir,
                 pg_absolute_dir,
                 pg_database,
                 pg_user,
                 pg_password,
                 pg_port,
                 cluster_source_pg_data_dir,
                 cluster_target_pg_data_dir,
                 backups_dir,
                 dr_unix_ip,
                 tdfs_enabled,
                 filestore_app_dir,
                 filestore_temp_mount_dir,
                 dataengine_dir,
                 is_reverse):
        logging.debug("EnvironmentManager class is being initialized!")
        self.rescue_user = rescue_user
        logging.debug("Distaster recovery user is set to %s." % rescue_user)
        self.is_sudoer = is_sudoer
        logging.debug("Distaster recovery user is sudoer? %s." % is_sudoer)

        self.cluster_source_mount_full_path = os.path.join(cluster_source_root_dir, mount_dir)
        logging.debug("The mount directory for the source cluster has been set to %s"
                      % self.cluster_source_mount_full_path)

        self.cluster_target_mount_full_path = None
        if cluster_target_root_dir is not None:
            cluster_target_root_dir = os.path.join(cluster_target_root_dir, mount_dir)
            self.cluster_target_mount_full_path = cluster_target_root_dir
            logging.debug("The mount directory for the target cluster has been set to %s" %
                          self.cluster_target_mount_full_path)


        self.sync_full_path = os.path.join(sync_root_dir, mount_dir)
        logging.debug("The sync directory has been set to %s" % self.sync_full_path)

        self.pg_absolute_dir = pg_absolute_dir
        logging.debug("Postgres absolute directory has been set to %s" % pg_absolute_dir)

        self.pg_database = pg_database
        logging.debug("Postgres database has been set to %s" % pg_database)

        self.pg_user = pg_user
        logging.debug("Postgres user has been set to %s" % pg_user)

        self.pg_password = pg_password
        logging.debug("Postgres password has been set!")

        self.pg_port = pg_port
        logging.debug("Postgres port has been set to %s" % pg_port)

        self.pg_data_root_dir=pg_data_root_dir
        logging.debug("Postgres data directory root has been set to %s" % self.pg_data_root_dir)

        self.cluster_source_pg_data_dir = cluster_source_pg_data_dir
        logging.debug("Postgres data directory for the source cluster has been set to %s" % cluster_source_pg_data_dir)

        self.cluster_target_pg_data_dir = cluster_target_pg_data_dir
        if self.cluster_target_pg_data_dir is not None:
            logging.debug("Postgres data directory for the target cluster has been set to %s"
                          % self.cluster_target_pg_data_dir)

        self.backups_dir = backups_dir
        logging.debug("Directory for storing backup files has been set to %s" % backups_dir)

        self.dr_unix_ip = dr_unix_ip
        logging.debug("Tableau DR Linux's IP address is set to %s" % dr_unix_ip)

        self.tdfs_enabled = tdfs_enabled
        logging.debug("TDFS switch is set to %s" % tdfs_enabled)

        if tdfs_enabled:

            self.src_tab_data_config_dir_abs = os.path.join(self.cluster_source_mount_full_path, tab_data_config_dir)
            logging.debug("The data configuration directory for the cluster source has been set to %s"
                          % self.src_tab_data_config_dir_abs)
            if cluster_target_root_dir is not None:
                self.tgt_tab_data_config_dir_abs = os.path.join(self.cluster_target_mount_full_path, tab_data_config_dir)
                logging.debug("The data configuration directory for the cluster target has been set to %s"
                              % self.tgt_tab_data_config_dir_abs)

            self.filestore_app_dir = filestore_app_dir
            logging.debug("Filestore binary directory is set to %s" % filestore_app_dir)

            self.filestore_temp_mount_dir = filestore_temp_mount_dir
            logging.debug("Filestore temporary mount directory is set to %s" % filestore_temp_mount_dir)

            self.dataengine_dir = dataengine_dir
            logging.debug("Dataengine directory is set to %s" % dataengine_dir)

            self.__tdfs_crons, self.__tdfs_cronjob = self.__make_cronjob_filestore()
        else:
            self.dataengine_dir = None
            self.filestore_temp_mount_dir = None
        #endif

        self.is_reverse = is_reverse
        logging.debug("Reverse switch is set to %s" % is_reverse)

    def validate_user(self):
        logging.debug("Validating that current user is the one to execute failover with...")
        stdout, stderr = self.__execute_cmd("whoami")
        os_user = stdout.rstrip()
        if os_user != self.rescue_user:
            raise ValidateEnvironmentException("Current OS user ({current_user}) "
                                               "is not the one to execute failover with ({rescue_user})!"
                                               .format(current_user=os_user,
                                                       rescue_user=self.rescue_user))
        logging.info("User OK!")
        return True

    def validate_rescue_dir(self):
        logging.debug("Validating Tableau DR's directory...")
        rescue_dir_path = os.path.split(self.backups_dir)[0]
        if not os.access(rescue_dir_path, os.W_OK):
            raise ValidateEnvironmentException("The provided directory for Tableau DR (%s) "
                                               "is not writable by %s!" % (rescue_dir_path, self.rescue_user))
        logging.info("Tableau DR directory OK!")
        return True

    def validate_paths(self):
        logging.debug("Validating that Tableau DR's directory tree is correct...")
        dirs_to_validate = [self.cluster_source_mount_full_path,
                            self.cluster_target_mount_full_path,
                            self.pg_data_root_dir,
                            self.cluster_source_pg_data_dir,
                            self.cluster_target_pg_data_dir,
                            self.backups_dir,
                            self.sync_full_path]
        dirs_to_validate = filter(lambda x: x is not None,
                                  dirs_to_validate)
        for dir_path in dirs_to_validate:
            logging.debug("Validating %s..." % dir_path)
            if not os.path.exists(dir_path):
                raise EnvironmentManagerException("%s does not exist! Make sure to execute the prepare phase "
                                                  "before validation!" % dir_path)
        logging.info("Directories OK!")

    def validate_os(self):
        logging.debug("Validating the Linux distribution is in progress...")
        distname, version, id = platform.linux_distribution()
        if distname not in d.ALLOWED_LINUX_DIST_NAMES:
            raise EnvironmentManagerException("Current Linux distribution ({dist} {version}) is not supported by "
                                              "Tableau DR!"
                                              .format(dist=distname,
                                                      version=version))
        logging.info("Operating system OK!")

    def validate_mountdir(self):
        logging.debug("Validating mount directories is in progress...")
        # Obtain list of mountpoints
        valid_mount_paths = [self.cluster_source_mount_full_path,
                             self.cluster_target_mount_full_path]
        # Ensure that missing target mount path is correctly handled
        valid_mount_paths = filter(lambda x: x is not None, valid_mount_paths)

        tableau_server_mountpoints = self.__get_relevant_mount_points(mount_paths=valid_mount_paths)

        # TODO: Handle cases where there may be more Tableau servers than 2?
        if len(tableau_server_mountpoints) < len(valid_mount_paths):
            raise ValidateEnvironmentException(
                "Was not able to find all required mount points! Identified mount points: %s"
                % (tableau_server_mountpoints))

        # Check that directories actually exist
        for mountpoint in tableau_server_mountpoints:
            try:
                dir_content = os.listdir(mountpoint)
                if dir_content == []:
                    raise ValidateEnvironmentException("Mountpoint dir (%s) is empty!" % mountpoint)
            except OSError, e:
                raise ValidateEnvironmentException("Mountpoint dir (%s) does not exist! OS Error: %s" % (mountpoint,
                                                                                                         e))

        # Here we can safely assume that mountdirs exists and are functional
        logging.info("Mount points are OK!")
        return True

    # Function to remove all relevant cron jobs
    def remove_relevant_cron_jobs(self):
        logging.debug("Removing relevant cron jobs is in progress...")
        # Get list of scheduled jobs from crontab
        user_crons = CronTab(user=self.rescue_user)
        rsync_cron_jobs = self.__get_relevant_rsync_jobs(user_crons)
        for cron_job in rsync_cron_jobs:
            user_crons.remove(cron_job)
        user_crons.write()
        logging.debug("Successfully cleared relevant replication jobs from crontab!")

    # Function to add initial cron jobs
    def add_initial_rsync_jobs(self):
        self.disable_rsync()

        logging.debug("Adding initial replication jobs is in progress...")

        # Get list of scheduled jobs from crontab
        user_crons = CronTab(user=self.rescue_user)
        rsync_cron_jobs = self.__get_relevant_rsync_jobs(user_crons)
        rsync_cron_job_cmds = map(lambda x: x.command, rsync_cron_jobs)

        jobs_to_add = []

        # Construct cron jobs
        for replication_subfolder in d.REPLICATION_DIRS:
            source_path = os.path.join(self.cluster_source_mount_full_path, replication_subfolder)
            sync_path = os.path.join(self.sync_full_path, replication_subfolder)
            if self.cluster_target_mount_full_path is not None:
                target_path = os.path.join(self.cluster_target_mount_full_path, replication_subfolder)
            else:
                target_path = None

            # Create sync path if not already exists
            if not os.path.exists(sync_path):
                os.makedirs(sync_path)

            # Ensure that there is replication between source and sync
            source_sync_rsync_job = d.RSYNC_TEMPLATE.format(source_path=utils.add_trailing_slash(source_path),
                                                            destination_path=utils.remove_trailing_slash(sync_path),
                                                            rescue_dir = os.path.split(self.backups_dir)[0],
                                                            uuid=uuid.uuid4())
            jobs_to_add.append(source_sync_rsync_job)

            if target_path is not None:
                sync_target_rsync_job = d.RSYNC_TEMPLATE.format(source_path=utils.add_trailing_slash(sync_path),
                                                                destination_path=utils.remove_trailing_slash(
                                                                    target_path),
                                                                rescue_dir = os.path.split(self.backups_dir)[0],
                                                            uuid=uuid.uuid4())
                jobs_to_add.append(sync_target_rsync_job)

        # Rsync between prod and sync config dir
        config_path_prod = os.path.join(self.cluster_source_mount_full_path, "config")
        config_path_sync = os.path.join(self.sync_full_path, "config")

        if not os.path.exists(config_path_sync):
            os.mkdir(config_path_sync)

        source_sync_rsync_job = d.RSYNC_TEMPLATE.format(source_path=utils.add_trailing_slash(config_path_prod),
                                                        destination_path=utils.remove_trailing_slash(config_path_sync),
                                                        rescue_dir = os.path.split(self.backups_dir)[0],
                                                        uuid=uuid.uuid4())
        jobs_to_add.append(source_sync_rsync_job)

        for job_cmd in jobs_to_add:
            logging.debug("Executing the replication job before adding it...")
            self.__execute_cmd(job_cmd)
            if job_cmd in rsync_cron_job_cmds:
                rsync_cron_jobs[rsync_cron_job_cmds.index(job_cmd)].enable()
            else:
                cron_job = user_crons.new(command=job_cmd)
                cron_job.setall('* * * * *')
                if not cron_job.is_valid():
                    raise EnvironmentManagerException("The following cron job is not valid: %s" % cron_job)
                cron_job.enable()

        user_crons.write()

    def delete_directory_tree(self):
        logging.debug("Deleting Tableau DR's directory tree...")
        clusters_dir = os.path.split(self.cluster_source_mount_full_path)[0]
        dirs_to_remove = [clusters_dir,
                          self.backups_dir]
        for dir_path in dirs_to_remove:
            if os.path.exists(dir_path):
                logging.debug("Deleting %s..." % dir_path)
                shutil.rmtree(dir_path)
            else:
                logging.debug("%s does not exist." % dir_path)

        pg_dirs_to_remove = [self.cluster_source_pg_data_dir,
                             self.cluster_target_pg_data_dir,
                             os.path.split(self.cluster_source_pg_data_dir)[0]]
        pg_dirs_to_remove = filter(lambda x: x is not None,
                                   pg_dirs_to_remove)
        for dir_path in pg_dirs_to_remove:
            logging.debug("Deleting %s..." % dir_path)
            try:
                self.__execute_cmd("rm -rf %s" % dir_path,
                                   as_unix_pg_user=True)
            except Exception, e:
                logging.debug("Was not able to remove %s due to the following error: %s" % (dir_path,
                                                                                            e))

        # Removing everything that remains in the folder
        tableau_dr_dir = os.path.split(self.backups_dir)[0]
        for root, dirs, files in os.walk(tableau_dr_dir, topdown=False):
            for filename in files:
                file_abs_path = os.path.join(root, filename)
                logging.debug("Deleting %s..." % file_abs_path)
                os.remove(file_abs_path)
            for dir_name in dirs:
                dir_abs_path = os.path.join(root, dir_name)
                logging.debug("Deleting %s..." % dir_abs_path)
                shutil.rmtree(dir_abs_path)

        logging.debug("Tableau DR's directory tree has been successfully deleted!")

    def create_directory_tree(self):
        logging.debug("Creating Tableau DR's directory tree...")
        dirs_to_create = [self.cluster_source_mount_full_path,
                          self.cluster_target_mount_full_path,
                          self.pg_data_root_dir,
                          self.cluster_source_pg_data_dir,
                          self.cluster_target_pg_data_dir,
                          self.backups_dir,
                          self.sync_full_path]

        if self.tdfs_enabled:
            dirs_to_create.append(os.path.join(self.sync_full_path, self.dataengine_dir))
            dirs_to_create.append(self.filestore_app_dir)

        dirs_to_create = filter(lambda x: x is not None,
                                dirs_to_create)
        for dir_path in dirs_to_create:
            if not os.path.exists(dir_path):
                logging.debug("Creating %s..." % dir_path)
                os.makedirs(dir_path)

        dirs_to_chmod = [
            [self.pg_data_root_dir,0775],
            [self.cluster_target_pg_data_dir,0775],
                         ]

        dirs_to_chmod = filter(lambda x: x[0] is not None,
                               dirs_to_chmod)
        for chmod_list in dirs_to_chmod:
            if os.path.exists(chmod_list[0]):
                logging.debug("Applying chmod {chmod} to {file}...".format(chmod=chmod_list[1],
                                                                        file=chmod_list[0]))
                os.chmod(chmod_list[0],chmod_list[1])

        logging.debug("Tableau DR's directory tree has been successfully created!")

    def check_rsync(self):
        logging.debug("Checking for scheduled replication jobs is in progress...")
        # Get list of scheduled jobs from crontab
        user_crons = CronTab(user=self.rescue_user)
        rsync_cron_jobs = self.__get_relevant_rsync_jobs(user_crons)

        if len(rsync_cron_jobs) == 0:
            raise ValidateEnvironmentException("There are no replication cron jobs scheduled!")

        # Sync between Source and Sync
        # Make sure sync cluster dir is included
        cluster_cron_jobs_source = filter(
            lambda x: self.cluster_source_mount_full_path in x.command and
            self.sync_full_path in x.command, rsync_cron_jobs)
        if len(cluster_cron_jobs_source) == 0:
            raise ValidateEnvironmentException("It seems like there is no replication cron job "
                                               "scheduled between source and sync dir!")

        if self.cluster_target_mount_full_path is not None:
            # Sync between Sync and Target
            cluster_cron_jobs_target = filter(
                lambda x: self.sync_full_path in x.command and  # Make sure sync dir is included
                self.cluster_target_mount_full_path in x.command,
                # Make sure target cluster dir is included
                rsync_cron_jobs)
            if len(cluster_cron_jobs_target) == 0:
                raise ValidateEnvironmentException(
                    "It seems like there is no replication scheduled between sync and target dir!")

        cluster_cron_jobs_source = filter(lambda x: x.is_enabled(),  # Make sure job is enabled
                                          cluster_cron_jobs_source)
        if len(cluster_cron_jobs_source) == 0:
            raise ValidateEnvironmentException(
                "Even though there is a cron job for replication between source and sync dir, it is not enabled!")
        if len(cluster_cron_jobs_source) != 1:
            logging.debug(
                "More than one rsync jobs detected between source and sync dir! \
                If this is not intentional, stop Tableau DR and fix!")

        # Checking whether rsync's direction makes sense or not...
        for job in cluster_cron_jobs_source:
            job_cmd = job.command
            logging.debug("Checking whether the following replication command's direction makes sense or not: %s" %
                          job_cmd)
            if job_cmd.find(self.cluster_source_mount_full_path) > job_cmd.find(self.sync_full_path):
                raise ValidateEnvironmentException(
                    "Replication's direction does not make sense! Sync is not from source to sync, but vice versa!\n"
                    "Are you trying to do a reverse switchover? "
                    "If so, modify the reverse parameter in the config file or add/remove the --reverse CLI argument")
            else:
                logging.debug("Replication between source and sync dir seems to make sense")

        if self.cluster_target_mount_full_path is not None:
            cluster_cron_jobs_target = filter(lambda x: x.is_enabled(),  # Make sure job is enabled
                                              cluster_cron_jobs_target)
            if len(cluster_cron_jobs_target) == 0:
                raise ValidateEnvironmentException(
                    "Even though there is a cron job for replication between sync and target dir, it is not enabled!")
            if len(cluster_cron_jobs_target) != 1:
                logging.debug(
                    "It seems like there are more than one replication jobs between sync and target dir! "
                    "If this is not intentional, stop Tableau DR and fix!")

            # Checking whether rsync's direction makes sense or not...
            for job in cluster_cron_jobs_target:
                job_cmd = job.command
                logging.debug("Checking whether the following replication command's direction makes sense or not: %s" %
                              job_cmd)
                if job_cmd.find(self.cluster_target_mount_full_path) < job_cmd.find(self.sync_full_path):
                    raise ValidateEnvironmentException(
                        "Replication's direction does not make sense! Sync is not from sync to target, "
                        "but vice versa!\n"
                        "For reverse swithover modify the reverse parameter in the config file"
                        "or add/remove the --reverse CLI argument"
                    )
                else:
                    logging.debug("Tableau File Store Repository replication between "
                                  "sync and target dir seems to make sense")

        # We can assume that rsync is scheduled and the direction makes sense
        logging.info("Tableau File Store Repository is OK!")
        return True

    def check_postgres(self, source_server, target_server=None):
        logging.debug("Checking Tableau Postgres Repository is in progress...")

        # Checking that data directories provided in the config file actually exists and contain
        # the necessary configuration files
        self.__validate_pg_datadir(self.cluster_source_pg_data_dir, source_server)

        if target_server is not None:
            self.__validate_pg_datadir(self.cluster_target_pg_data_dir, target_server, stop_after_basebackup=True)

        # Find Postgres processes
        logging.debug("Checking whether the correct Postgres is running on Tableau DR Linux or not.")
        procs = subprocess.check_output(shlex.split("ps -ef")).splitlines()
        postgres_procs = filter(lambda x: "postgres" in x,
                                procs)
        if len(postgres_procs) == 0:
            logging.debug("No running PostgreSQL process was found, starting..")
            self.__manage_postgres(pg_absolute_dir=self.pg_absolute_dir,
                                   pg_data_dir=self.cluster_source_pg_data_dir,
                                   start=True)

        source_postgres_procs = filter(lambda x: self.cluster_source_pg_data_dir in x.split(" "),
                                       postgres_procs)
        if len(source_postgres_procs) == 0 and target_server is not None:
            target_postgres_procs = filter(lambda x: self.cluster_target_pg_data_dir in x.split(" "),
                                           postgres_procs)
            if len(target_postgres_procs) > 0:
                raise ValidateEnvironmentException("The source Tableau Server's Postgres replica is not running, "
                                                   "but the target Tableau Server's Postgres replica is active!\n"
                                                   "Please use the --reverse CLI argument or set reverse to true in "
                                                   "the configuration file!")
            else:
                logging.debug("Neither source Tableau Server's replica Postgres, nor target Tableau Server's replica "
                              "is running! Attempting to start source server's replica...")
                self.__manage_postgres(pg_absolute_dir=self.pg_absolute_dir,
                                       pg_data_dir=self.cluster_source_pg_data_dir,
                                       start=True)
        else:
            logging.debug("Source Tableau Server's replica Postgres is not running! Attempting to start it...")
            self.__manage_postgres(pg_absolute_dir=self.pg_absolute_dir,
                                   pg_data_dir=self.cluster_source_pg_data_dir,
                                   start=True)

        # Attempting to run psql to tests connection to local Postgres
        logging.debug("Attempting to connect to local Postgres...")
        self.__test_local_pg_connection(pg_absolute_dir=self.pg_absolute_dir,
                                        pg_port=self.pg_port,
                                        pg_user=self.pg_user,
                                        pg_database=self.pg_database)

        # Attempting to connect to Postgres on source to pg_database
        logging.debug("Attempting to connect to source Tableau Server's Postgres...")
        retry_needed = self.__test_source_pg_connection(pg_absolute_dir=self.pg_absolute_dir,
                                                        pg_host=source_server.host,
                                                        pg_port=8060,
                                                        pg_user="tableau",
                                                        pg_database=self.pg_database,
                                                        source_server=source_server,
                                                        target_server=target_server,
                                                        test_replication_role=True)
        num_retries = 0
        while retry_needed and num_retries <= d.REMOTE_PG_CONNECT_MAX_RETRIES:
            num_retries += 1
            retry_needed = self.__test_source_pg_connection(pg_absolute_dir=self.pg_absolute_dir,
                                                            pg_host=source_server.host,
                                                            pg_port=8060,
                                                            pg_user="tableau",
                                                            pg_database=self.pg_database,
                                                            source_server=source_server,
                                                            target_server=target_server,
                                                            test_replication_role=True)

        if retry_needed:
            # TODO: Add more meaningful info on why this may have happened
            raise ValidateEnvironmentException("Was not able to connect to source Tableau Server's Postgres!")

        logging.info("Tableau Postgres Repository is OK!")
        return True

    def test_winrm(self, source_server, target_server):
        servers = [source_server, target_server]
        servers = filter(lambda x: x is not None,
                         servers)

        for server in servers:
            self.__test_server_connection(server)
            server.validate_tableau_paths()

        logging.info("Tableau servers are OK!")
        return True

    def disable_rsync(self):
        logging.debug("Disabling Tableau File Store Repository sync is in progress...")
        user_crons = CronTab(user=self.rescue_user)
        cluster_cron_jobs = self.__get_relevant_rsync_jobs(crontab=user_crons)
        logging.debug("Found the following cron jobs: %s" % cluster_cron_jobs)
        if cluster_cron_jobs is not None:
            for job in cluster_cron_jobs:
                if job.is_enabled():
                    job.enable(False)  # Disable job
                    logging.debug("The following job has been disabled: %s" % job.command)

        user_crons.write()  # Write modifications
        logging.debug("Tableau File Store Repository sync has been successfully disabled!")

    def reverse_rsync_direction(self):
        if self.cluster_target_mount_full_path is None:
            raise EnvironmentManagerException("Cannot reverse replication direction in a single cluster setting!")

        logging.debug("Reversing Rsync direction...")
        user_crons = CronTab(user=self.rescue_user)
        rsync_cron_jobs = self.__get_relevant_rsync_jobs(crontab=user_crons)
        user_cron_cmds = map(lambda x: x.command, rsync_cron_jobs)
        logging.debug("Found the following replication cron jobs: %s" % rsync_cron_jobs)

        # Disable relevant jobs that already exist
        map(lambda x: x.enable(False),
            rsync_cron_jobs)

        # Find sync between Sync and Target
        cluster_cron_jobs_target = filter(
            lambda x: self.sync_full_path in x.command and  # Make sure sync dir is included
            # Make sure target cluster dir is included
            self.cluster_target_mount_full_path in x.command and
            # Make sure direction is good
            x.command.find(self.sync_full_path) < x.command.find(self.cluster_target_mount_full_path),
            rsync_cron_jobs)

        # Switch direction to Target --> Sync
        cluster_cron_jobs_target_cmd = map(lambda x: x.command, cluster_cron_jobs_target)
        logging.debug("Found the following replication jobs between target and sync: %s" % cluster_cron_jobs_target)
        for item in cluster_cron_jobs_target_cmd:
            switched_cron_cmd = utils.switch_direction_of_rsync(rsync_cmd_str=item,
                                                                 source_dir=self.sync_full_path,
                                                                 target_dir=self.cluster_target_mount_full_path)
            if switched_cron_cmd not in user_cron_cmds:
                switched_cron_job = user_crons.new(command=switched_cron_cmd)
                switched_cron_job.setall('* * * * *')
                if not switched_cron_job.is_valid():
                    raise Exception("The following cron job is not valid: %s" % switched_cron_cmd)
                switched_cron_job.enable()
            else:
                switched_cron_job = user_crons[user_cron_cmds.index(switched_cron_cmd)]
                switched_cron_job.enable()

        # Find sync between Source and Sync
        cluster_cron_jobs_source = filter(
            lambda x: self.cluster_source_mount_full_path in x.command and  # Make sure source cluster dir is included
            self.sync_full_path in x.command and
            x.command.find(self.cluster_source_mount_full_path) < x.command.find(self.sync_full_path),
            rsync_cron_jobs)

        cluster_cron_jobs_source_cmd = map(lambda x: x.command,
                                           cluster_cron_jobs_source)
        logging.debug("Found the following replication jobs between target and sync: %s" % cluster_cron_jobs_source_cmd)

        # Switch direction to Sync --> Source
        for item in cluster_cron_jobs_source_cmd:
            switched_cron_cmd = utils.switch_direction_of_rsync(rsync_cmd_str=item,
                                                                 source_dir=self.cluster_source_mount_full_path,
                                                                 target_dir=self.sync_full_path)
            if switched_cron_cmd not in user_cron_cmds:
                switched_cron_job = user_crons.new(command=switched_cron_cmd)
                switched_cron_job.setall('* * * * *')
                if not switched_cron_job.is_valid():
                    raise EnvironmentManagerException("The following cron job is not valid: %s" % switched_cron_cmd)
                switched_cron_job.enable()
            else:
                switched_cron_job = user_crons[user_cron_cmds.index(switched_cron_cmd)]
                switched_cron_job.enable()

        # Rsync between dr and sync config dir
        config_path_dr = os.path.join(self.cluster_target_mount_full_path, "config")
        config_path_sync = os.path.join(self.sync_full_path, "config")

        dr_sync_rsync_job = d.RSYNC_TEMPLATE.format(source_path=utils.add_trailing_slash(config_path_dr),
                                                    destination_path=utils.remove_trailing_slash(config_path_sync),
                                                    rescue_dir=os.path.split(self.backups_dir)[0],
                                                    uuid=uuid.uuid4())

        if dr_sync_rsync_job not in user_cron_cmds:
            dr_sync_rsync_job = user_crons.new(command=dr_sync_rsync_job)
            dr_sync_rsync_job.setall('* * * * *')
            if not dr_sync_rsync_job.is_valid():
                raise EnvironmentManagerException("The following cron job is not valid: %s" % dr_sync_rsync_job)
            dr_sync_rsync_job.enable()
        else:
            dr_sync_rsync_job = user_crons[user_cron_cmds.index(dr_sync_rsync_job)]
            dr_sync_rsync_job.enable()

        # Write modifications
        user_crons.write()
        logging.debug("Rsync direction has been successfully reversed!")

    def install_java(self):
        logging.info("Checking if Java is installed..")

        try:
            err, out = self.__execute_cmd(d.JAVA_CHECK_CMD)
        except EnvironmentManagerException:
            logging.info("Java installer is not found! Installing Java JRE 8..")
            map(lambda cmd: self.__execute_cmd(cmd), d.JAVA_INSTALL_CMDS)

    def check_filestore(self):

        # Check if filestore is running properly
        if not self.__is_filestore_running():
            raise EnvironmentManagerException("Filestore validation error! Filestore process is not running..")

        # Check if filestore is in sync, act if not
        # self.sync_filestore()

        logging.debug("Filestore validation completed successfully!")

    def build_filestore(self):

        if self.__is_filestore_running():
            raise EnvironmentManagerException("Filestore process is already running! "
                                              "Make sure it is stopped before prepare.")

        self.install_java()
        self.install_filestore()
        self.cleanup_filestore()
        self.configure_filestore()

    def __get_filestore_pid(self):
        """Check if Filestore process is running by looking for 'java' processes
        opening 'filestore/lib/tab-tdfs-filestore-latest.jar' file
        """
        open_file_to_look_for = os.path.join(self.filestore_app_dir, "lib", "tab-tdfs-filestore-latest.jar")
        for proc in psutil.process_iter():
            try:
                pinfo = proc.as_dict(attrs=["pid", "name", "open_files"])
                if pinfo["name"] is not None and pinfo["open_files"] is not None:
                    if pinfo["name"] == "java":
                        if len(filter(lambda of: of.path == open_file_to_look_for, pinfo["open_files"])) > 0:
                            logging.debug("Filestore process is running with pid: %s" % pinfo["pid"])
                            return pinfo["pid"]
            except psutil.NoSuchProcess:
                return False

        return None

    def __is_filestore_running(self):
        return True if self.__get_filestore_pid() is not None else False

    def __callback_filestore_stop(self, proc):
        logging.debug("Filestore process {} terminated".format(proc))

    def stop_filestore(self):
        filestore_pid = self.__get_filestore_pid()
        if filestore_pid is None:
            logging.warn("Failed to stop Filestore, it is not running!")

        logging.debug("Stopping Filestore..")
        filestore_proc = None
        for proc in psutil.process_iter():
            try:
                pinfo = proc.as_dict(attrs=['pid'])
                if pinfo["pid"] == filestore_pid:
                    filestore_proc = proc
                    filestore_proc.terminate()
                    break

            except psutil.NoSuchProcess:
                raise EnvironmentManagerException("Failed to find Filestore process with pid: %d!" % filestore_pid)

        gone, alive = psutil.wait_procs([filestore_proc], timeout=5, callback=self.__callback_filestore_stop)
        for p in alive:
            p.kill()

    def run_filestore(self, is_switchover):

        if self.__get_filestore_pid() is not None:
            raise EnvironmentManagerException("Filestore process is already running! "
                                              "Make sure it is stopped before prepare.")

        logging.debug("Starting Filestore process..")
        self.sync_filestore(is_switchover)

        # Start filestore
        logging.debug("Enable cron entry for Filestore..")
        self.__tdfs_cronjob.enable()
        self.__tdfs_crons.write()

        filestore_retry_sec = 5
        filestore_timeout_sec = 95
        for i in range(0, filestore_timeout_sec, filestore_retry_sec):
            if self.__get_filestore_pid() is not None:
                break
            logging.debug("Filestore is not started, waiting %d seconds.." % filestore_retry_sec)
            time.sleep(filestore_retry_sec)

            if i == filestore_timeout_sec - filestore_retry_sec:
                raise EnvironmentManagerException("Failed to start Filestore process, check filestore.log!")

    # Check if data discrepacy found
    def sync_filestore(self, is_switchover=False):
        logging.debug("Filestore data validation check..")
        src = None
        tgt = None

        # prepare phase or reverse switchover
        if not is_switchover:
            src = os.path.join(self.cluster_source_mount_full_path, self.dataengine_dir)
            tgt = os.path.join(self.sync_full_path, self.dataengine_dir)
        else:
            src = os.path.join(self.sync_full_path, self.dataengine_dir)
            tgt = os.path.join(self.cluster_target_mount_full_path, self.dataengine_dir)

        out, err = self.__execute_cmd(d.FILESTORE_IS_SYNC_DRY_CMD.format(
            src_dataengine_dir=src,
            tgt_dataengine_dir=tgt))

        mismatches = len(filter(lambda l: l.startswith("extract"), out.split("\n")))

        if mismatches > 0:
            logging.info("Data discrepacy found between Tableau cluster and Tableau DR! "
                         "Initiate re-sync for %d items.." % mismatches)

            out, err = self.__execute_cmd(d.FILESTORE_IS_SYNC_CMD.format(
                src_dataengine_dir=src,
                tgt_dataengine_dir=tgt))

            logging.debug("Re-sync output: %s" % out)
        else:
            logging.debug("Filestore data directory is in sync!")

    def disable_filestore(self):
        logging.debug("Disabling Filestore cron entry..")
        self.__tdfs_cronjob.enable(False)
        self.__tdfs_crons.write()

    def __make_cronjob_filestore(self):
        tdfs_cronjob_comment = "Tableau DR TDFS"
        user_crons = CronTab(user=self.rescue_user)
        filestore_cronjob = None
        for job_iter, job in enumerate(user_crons.find_comment(tdfs_cronjob_comment)):
            filestore_cronjob = job
            if job_iter > 0:
                raise EnvironmentManagerException("Multiple Filestore cron entry was found! Clean up crontab manually!")

        if filestore_cronjob is None:

            # run in every minute by default
            filestore_cronjob = user_crons.new(command=d.FILESTORE_BIN_CMD.
                                               format(conn_prop=os.path.join(self.filestore_app_dir,
                                                                             "conf",
                                                                             "connections.properties"),
                                                      filestore_prop=os.path.join(self.filestore_app_dir,
                                                                                  "conf",
                                                                                  "filestore.properties"),
                                                      log4j_xml=os.path.join(self.filestore_app_dir,
                                                                             "conf",
                                                                             "log4j.xml"),
                                                      bin_path=os.path.join(self.filestore_app_dir,
                                                                            "bin"),
                                                      lib_path=os.path.join(self.filestore_app_dir,
                                                                            "lib")),
                                               comment=tdfs_cronjob_comment)
            filestore_cronjob.enable(False)
            user_crons.write()

        if not filestore_cronjob.is_valid():
            raise EnvironmentManagerException("Filestore cron entry is not valid: %s!" % job)

        return user_crons, filestore_cronjob

    def install_filestore(self):

        # Re-create cron job for Filestore
        logging.debug("Re-create cron entry for TDFS..")
        self.__make_cronjob_filestore()

        logging.debug("Cleaning out Filestore binary location...")
        try:
            shutil.rmtree(self.filestore_app_dir)
        except OSError:
            logging.debug("Failed to delete non-existing directory: %s" % self.filestore_app_dir)

        logging.debug("Building Filestore binary...")

        # Copy all jars from /bin and /lib recursively
        for root, dirs, files in os.walk(self.filestore_temp_mount_dir):
            for file in files:
                if (not root.startswith(os.path.join(self.filestore_temp_mount_dir, "bin"))) and \
                        (not root.startswith(os.path.join(self.filestore_temp_mount_dir, "lib"))):
                    continue
                if file.endswith(".jar"):
                    src = os.path.join(root, file)
                    tgt_dir = os.path.join(self.filestore_app_dir,
                                           os.sep.join(root.split(os.sep)[len(self.filestore_app_dir.split(os.sep)):]))
                    tgt = os.path.join(tgt_dir, file)
                    os.makedirs(tgt_dir) if not os.path.exists(tgt_dir) else None  # create target dir recursively
                    if not os.path.exists(tgt):
                        logging.debug("Copying file %s to %s" % (src, tgt))
                        shutil.copy(src, tgt)

    def cleanup_filestore(self):
        # Remove temporary mount directory
        try:
            self.__execute_cmd(d.UMOUNT_CMD.format(mount_abs_path=self.filestore_temp_mount_dir))
            os.rmdir(self.filestore_temp_mount_dir)  # TODO: make sure its empty before delete
        except EnvironmentManagerException as exc:
            logging.error(str(exc))
        except OSError as exc:
            logging.error("Failed to delete Filestore temporary mount directory: %s"
                          % self.filestore_temp_mount_dir)

    def configure_filestore(self, is_switchover=False):
        # Create local Filestore configuration file
        try:
            conf_dir = os.path.join(self.filestore_app_dir, "conf")
            log_dir = os.path.join(self.filestore_app_dir, "log")

            for req_dir in [conf_dir, log_dir]:
                if not os.path.exists(req_dir):
                    os.mkdir(req_dir)

            config_dir = self.tgt_tab_data_config_dir_abs if is_switchover else self.src_tab_data_config_dir_abs

            config_files = [(config_dir, "filestore.properties"),
                            (config_dir, "connections.properties"),
                            (config_dir, "filestore", "log4j.xml")]

            for file_t in config_files:
                src = os.path.join(*file_t)
                tgt = os.path.join(conf_dir, file_t[-1])
                logging.debug("Copying %s to %s..." % (src, tgt))
                shutil.copyfile(src, tgt)

            # Create or update local config files
            # filestore.properties
            add_required_config_lines = [
                "worker.hosts",
                "ha2.filestore.enabled",
                "ha2.clustercontroller.enabled",
                "ha2.enabled",
                "filestore.orphanhostregistrationreapintervalms",
                "repository.data.dir",
                "mode.standalone.enabled"]

            new_file = ""
            with open(os.path.join(conf_dir, config_files[0][-1]), "r") as f:
                logging.debug("Updating %s..." % (config_files[0][-1]))
                for line in f:
                    line_k = line.split("=")[0]
                    line_v = line.split("=")[1].rstrip()
                    if line.startswith("filestore.") or \
                            line.startswith("filestore_") or \
                            line.startswith("coordinationservice.") or \
                            line_k in add_required_config_lines:

                        if line_k == "filestore.hostname":
                            line_v = socket.gethostname()

                        if line_k == "filestore.root":
                            line_v = self.sync_full_path

                        if line_k == "worker.hosts":
                            line_v = line_v + "," + socket.gethostname()

                        if line_k == "repository.data.dir":
                            line_v = ""

                        new_file += "%s=%s\n" % (line_k, line_v)

            with open(os.path.join(conf_dir, config_files[0][-1]), "w") as f:
                f.writelines(new_file)

            # log4j.xml
            soup = None
            logging.debug("Updating %s.." % config_files[2][-1])
            with open(os.path.join(conf_dir, config_files[2][-1]), "r") as f:
                soup = BeautifulSoup(f, "xml")

                # remove Windows only NTEvent Log
                soup.find("appender", {"name": "nteventlog"}).extract()
                soup.find("logger", {"name": "com.tableausoftware"}).extract()

                # modify log file path
                soup.find("appender", {"name": "file"}).find("param", {"name": "File"})['value'] =\
                    os.path.join(log_dir, "filestore.log")
                soup.find("appender", {"name": "rollingBufferedFile"}).find("param", {"name": "File"})['value'] =\
                    os.path.join(log_dir, "filestore.log")

                # make debug logging
                soup.find("root").find("priority")['value'] = "debug"

            with open(os.path.join(conf_dir, config_files[2][-1]), "w") as f:
                f.writelines(soup.prettify(formatter=None))

            # Update remote config files
            new_file = ""
            with open(os.path.join(config_dir, config_files[0][-1]), "r") as f:
                logging.debug("Updating remote file at %s..."
                              % (os.path.join(config_dir, config_files[0][-1])))
                for line in f:
                    line_k = line.split("=")[0]
                    line_v = line.split("=")[1].rstrip()

                    if line_k == "worker.hosts":
                        line_v += ",%s" % socket.gethostname()

                    new_file += "%s=%s\n" % (line_k, line_v)

            with open(os.path.join(config_dir, "filestore.properties"), "w") as f:
                f.writelines(new_file)

        except Exception as exc:
            logging.error("Failed to set up Filestore configuration: %s" % str(exc))

    def install_build_postgres(self, source_server):
        logging.debug("Installing and building Postgres is in progress...")
        tempdir = tempfile.gettempdir()
        temp_pg_dir = os.path.join(tempdir, "postgres")

        try:
            logging.info("Deleting temporaly postgres files...")
            self.delete_temp_pg_dir()
        except Exception, e:
            logging.debug("Was not able to remove %s due to the following error: %s" % (temp_pg_dir, e))
            raise Exception("Was not able to remove %s due to the following error: %s" % (temp_pg_dir, e))

        if not os.path.exists(temp_pg_dir):
            os.mkdir(temp_pg_dir)

        logging.debug("Obtaining and building Postgres source...")
        pg_tar_path = os.path.join(temp_pg_dir, "postgresql-%s.tar.bz2" % d.PG_VERSION)
        if not os.path.exists(pg_tar_path):
            try:
                urllib.urlretrieve(d.PG_SOURCE_URL,
                                   pg_tar_path)
            except Exception, e:
                raise EnvironmentManagerException("Tableau DR was not able to download Postgresql {version} source "
                                                  "due to the following error: {error}\n"
                                                  "Please download it manually and place the tar into {dest_path}!"
                                                  .format(error=e,
                                                          dest_path=temp_pg_dir,
                                                          version=d.PG_VERSION))

        untar_cmd = "tar xvf {pg_tar_path} -C {temp_pg_dir}".format(pg_tar_path=pg_tar_path,
                                                                    temp_pg_dir=temp_pg_dir)
        pg_source_path = os.path.join(temp_pg_dir, "postgresql-%s" % d.PG_VERSION)
        if not os.path.exists(pg_source_path):
            self.__execute_cmd(untar_cmd)
        else:
            if os.listdir(pg_source_path) == []:
                self.__execute_cmd(untar_cmd)

        # Execute the actual build procedure
        for cmd in d.PG_BUILD_PROCEDURE:
            try:
                self.__execute_cmd(cmd.format(prefix=self.pg_absolute_dir),
                                   cwd=pg_source_path)
            except EnvironmentManagerException:
                raise EnvironmentManagerException("Tableau DR was not able to prepare Postgresql! "
                                                  "Please make sure to install all of the required Linux "
                                                  "packages beforehand! "
                                                  "For the list of those packages please check the documentation.")

        for cmd in d.ADD_PG_USER_CMDS:
            try:
                cmd = cmd.format(pg_abs_path=self.pg_absolute_dir,
                                 rescue_user_gid=self.rescue_user)
                self.__execute_cmd(cmd)
            except EnvironmentManagerException, e:
                logging.debug("Error encountered while executing the following command: %s \n Error: %s" % (cmd, e))

        # Set localedef
        self.__execute_cmd(d.LOCALEDEF_CMD)

        try:
            logging.info("Deleting temporaly postgres files...")
            self.delete_temp_pg_dir()
        except Exception, e:
            logging.debug("Was not able to remove %s due to the following error: %s" % (temp_pg_dir, e))
            raise Exception("Was not able to remove %s due to the following error: %s" % (temp_pg_dir, e))

    def basebackup_start_source_postgres(self, source_server):
        self.postgres_basebackup(tab_host=source_server.host,
                                 pg_data_dir=self.cluster_source_pg_data_dir,
                                 initial=True)
        self.__validate_pg_datadir(self.cluster_source_pg_data_dir,
                                   tab_server=source_server)
        self.__validate_pg_log_dir()
        # Start Postgres
        self.start_source_postgres()

    def start_source_postgres(self):
        logging.debug("Starting Postgres instance for the source Tableau cluster...")
        self.__manage_postgres(pg_absolute_dir=self.pg_absolute_dir,
                               pg_data_dir=self.cluster_source_pg_data_dir)
        logging.debug("Postgres instance for the source Tableau cluster was successfully started!")

    def stop_source_postgres(self):
        logging.debug("Stopping Postgres instance for the source Tableau cluster...")
        self.__manage_postgres(pg_absolute_dir=self.pg_absolute_dir,
                               pg_data_dir=self.cluster_source_pg_data_dir,
                               start=False)
        logging.debug("Postgres instance for the source Tableau cluster was successfully stopped!")

    def start_target_postgres(self):
        if self.cluster_target_pg_data_dir is None:
            raise EnvironmentManagerException(
                "Target Tableau Server's Postgres replica is nonexistent in a single cluster setting!")
        logging.debug("Starting Postgres instance for the target Tableau cluster...")
        self.__manage_postgres(pg_absolute_dir=self.pg_absolute_dir,
                               pg_data_dir=self.cluster_target_pg_data_dir)
        logging.debug("Postgres instance for the target Tableau cluster was successfully started!")

    def stop_target_postgres(self):
        if self.cluster_target_pg_data_dir is None:
            raise EnvironmentManagerException(
                "Target Tableau Server's Postgres replica is nonexistent in a single cluster setting!")

        logging.debug("Stopping Postgres instance for the target Tableau cluster...")
        self.__manage_postgres(pg_absolute_dir=self.pg_absolute_dir,
                               pg_data_dir=self.cluster_target_pg_data_dir,
                               start=False)
        logging.debug("Postgres instance for the target Tableau cluster was successfully stopped!")

    def postgres_basebackup(self,
                            tab_host,
                            pg_data_dir,
                            initial=False):

        logging.debug("Executing Postgres basebackup...")
        # REmote connection settings
        remote_pg_user = "tableau"
        remote_pg_password = self.pg_password

        logging.debug("Validating PG data dir %s..." % pg_data_dir)
        check_dir_exists_cmd = "ls %s" % pg_data_dir
        try:
            self.__execute_cmd(cmd_str=check_dir_exists_cmd,
                               as_unix_pg_user=True)
        except EnvironmentManagerException:
            if not initial:
                raise EnvironmentManagerException("Postgres data directory (%s) does not exist!" % pg_data_dir)

        if not initial:
            # Preserve the content of recovery.conf and postgresql.conf
            recovery_conf_temp_path = os.path.join(tempfile.gettempdir(), "recovery.conf")
            recovery_conf_absolute_path = os.path.join(pg_data_dir, "recovery.conf")
            recovery_conf_temp_cmd = "echo \"$(cat %s)\" > %s" % (recovery_conf_absolute_path,
                                                                  recovery_conf_temp_path)
            self.__execute_cmd(cmd_str=recovery_conf_temp_cmd,
                               as_unix_pg_user=True)

            postgresql_conf_temp_path = os.path.join(tempfile.gettempdir(), "postgresql.conf")
            postgresql_conf_absolute_path = os.path.join(pg_data_dir, "postgresql.conf")
            postgresql_conf_temp_cmd = "echo \"$(cat %s)\" > %s" % (postgresql_conf_absolute_path,
                                                                    postgresql_conf_temp_path)
            self.__execute_cmd(cmd_str=postgresql_conf_temp_cmd,
                               as_unix_pg_user=True)

        # Config data is preserved, we can delete the old data directory
        chown_data_dir_cmd = "sudo chmod -R g+w %s" % pg_data_dir
        self.__execute_cmd(chown_data_dir_cmd)

        delete_data_dir_cmd = "rm -rf %s" % pg_data_dir
        self.__execute_cmd(cmd_str=delete_data_dir_cmd,
                           as_unix_pg_user=True)

        # Creating/Modifying the content of .pgpass
        self.__modify_pgpass(tab_host=tab_host,
                             remote_pg_port=8060,
                             remote_pg_db="replication",
                             remote_pg_user=remote_pg_user,
                             remote_pg_password=remote_pg_password,
                             append=True)

        # Executing basebackup
        backup_cmd = d.PG_BASEBACKUP_CMD.format(pg_dir=self.pg_absolute_dir,
                                                host=tab_host,
                                                pg_port=8060,
                                                pg_data_dir=pg_data_dir,
                                                pg_user=remote_pg_user,
                                                pg_pass=remote_pg_password)
        self.__execute_cmd(cmd_str=backup_cmd,
                           as_unix_pg_user=True,
                           env={"LD_LIBRARY_PATH": os.path.join(self.pg_absolute_dir, "lib")})

        if not initial:
            # Replacing config file contents in data dir from the temporary files
            replace_recovery_conf_cmd = "echo \"$(cat %s)\" > %s" % (recovery_conf_temp_path,
                                                                     recovery_conf_absolute_path)
            self.__execute_cmd(replace_recovery_conf_cmd,
                               as_unix_pg_user=True)

            replace_postgresql_conf_cmd = "echo \"$(cat %s)\" > %s" % (postgresql_conf_temp_path,
                                                                       postgresql_conf_absolute_path)
            self.__execute_cmd(cmd_str=replace_postgresql_conf_cmd,
                               as_unix_pg_user=True)

        logging.debug("Postgres basebackup has been successful!")

    def execute_source_pgdump(self, destination_dir, dump_format="p"):
        logging.debug("Executing Tableau Postgres Repository pgdump is in progress...")

        logging.debug("Executing Tableau Postgres Repository pgdump...")
        pg_dump_cmd = d.PG_DUMP_COMMAND.format(pg_dir=self.pg_absolute_dir,
                                               user=self.pg_user,
                                               database=self.pg_database,
                                               dump_format=dump_format)
        stdout, stderr = self.__execute_cmd(cmd_str=pg_dump_cmd,
                                            env={"LD_LIBRARY_PATH": os.path.join(self.pg_absolute_dir, "lib")})
        pg_dump_file_path = os.path.join(destination_dir, d.WORKGROUP_PG_DUMP_FILE)
        open(pg_dump_file_path, "w").write(stdout)

        logging.debug("Executing Tableau Postgres Repository pgdump all...")
        pg_dumpall_cmd = d.PG_DUMPALL_COMMAND.format(pg_dir=self.pg_absolute_dir,
                                                     user=self.pg_user)
        stdout, stderr = self.__execute_cmd(cmd_str=pg_dumpall_cmd,
                                            env={"LD_LIBRARY_PATH": os.path.join(self.pg_absolute_dir, "lib")})
        pg_dumpall_file_path = os.path.join(destination_dir, d.BACKUP_SQL_FILE)
        open(pg_dumpall_file_path, "w").write(stdout)

        logging.debug("Successfully executed Tableau Postgres Repository pgdump!")

    def create_backup(self):
        logging.info("Creating backup file...")

        # Create a temporary directory for tsbak contents
        backup_temp_dir = os.path.join(tempfile.gettempdir(), "backup")
        if os.path.exists(backup_temp_dir):
            logging.debug("Removing existing temporary directory for backup...")
            shutil.rmtree(backup_temp_dir)

        os.makedirs(backup_temp_dir)
        logging.debug("Temporary directory for backup has been created at %s." % backup_temp_dir)

        if not os.path.exists(self.backups_dir):
            logging.debug("Directory for backups (%s) does not exist! Creating it..." % self.backups_dir)
            os.makedirs(self.backups_dir)

        # Copy tabsvc.yaml to backup temp directory
        tab_config_abs_dir = os.path.join(self.sync_full_path,
                                          "config")
        tabsvc_yaml_abs_path = os.path.join(tab_config_abs_dir, "tabsvc.yml")
        tabsvc_yaml_temp_abs_path = os.path.join(backup_temp_dir, "config.yml")
        logging.debug("Copying %s to %s..." % (tabsvc_yaml_abs_path, tabsvc_yaml_temp_abs_path))
        shutil.copyfile(tabsvc_yaml_abs_path,
                        tabsvc_yaml_temp_abs_path)
        logging.debug("tabsvc.yaml has been successfully copied!")

        # Copy tabsvc-customization.yaml to backup temp directory
        tabsvc_cust_yaml_abs_path = os.path.join(tab_config_abs_dir, "tabsvc-customization.yml")
        tabsvc_cust_yaml_temp_abs_path = os.path.join(backup_temp_dir, "customization.yml")
        logging.debug("Copying %s to %s..." % (tabsvc_cust_yaml_abs_path,
                                               tabsvc_cust_yaml_temp_abs_path))
        if os.path.exists(tabsvc_cust_yaml_abs_path):
            shutil.copyfile(tabsvc_cust_yaml_abs_path,
                            tabsvc_cust_yaml_temp_abs_path)
            logging.debug("tabsvc_customization.yaml has been successfully copied!")

            # check customization.yml for images.
            custom_images_dict = {
                'header_logo.path': 'custom_headerlogo',
                'sign_in_logo.path': 'custom_signinlogo',
                'smalllogo': 'custom_smalllogo'
            }

            # open custom_images_dict
            with open(tabsvc_cust_yaml_temp_abs_path) as f:

                # check line for wgserver.{custom_images_dict[k]}.path: {filename}
                lines = f.readlines()
                for line in lines:
                    for logoname in custom_images_dict:
                        pattern = '^wgserver.' + logoname + ': /(.*)$'
                        output = re.search(pattern, line)
                        if output is not None:
                            path_to_logo = output.group(1)
                            logging.debug(logoname + " found in customization.yaml")

                            # copy the custom_image to the temporaly folder
                            copy_from = os.path.join(self.cluster_source_mount_full_path,
                                                     'data/tabsvc/wgserver',
                                                     path_to_logo)
                            copy_to = os.path.join(backup_temp_dir, custom_images_dict[logoname])

                            if os.path.exists(copy_from):
                                logging.debug("Copying %s to %s..." % (copy_from, copy_to))
                                shutil.copyfile(copy_from, copy_to)
                                logging.debug(logoname + " has been successfully copied!")
                            else:
                                logging.debug("Not copying %s since it is not present." % copy_from)

        # Creating manifest file
        manifest_temp_path = os.path.join(backup_temp_dir, "manifest.yml")
        logging.debug("Creating %s..." % manifest_temp_path)
        with open(manifest_temp_path, "w") as f:
            f.write("--- \n:version: \"1.6\"\n")
            logging.debug("manifest.yaml has been successfully created!")

        # Execute pgdump and pgdump_all and put the resulting files into backup temporary directory
        logging.debug("Executing pgdump and pgdump_all...")
        self.execute_source_pgdump(destination_dir=backup_temp_dir,
                                   dump_format="t")
        logging.debug("Pgdump and pgdump_all has been successful!")

        # Copy dataengine directory
        dataengine_abs_path = os.path.join(self.sync_full_path, d.DATAENGINE_DIR)
        dataengine_temp_path = os.path.join(backup_temp_dir, "dataengine")
        logging.debug("Copying %s to %s..." % (dataengine_abs_path,
                                               dataengine_temp_path))
        shutil.copytree(dataengine_abs_path,
                        dataengine_temp_path)
        logging.debug("Dataengine directory has been successfully copied!")

        # Copy webdataconnectors directory
        webdataconnectors_abs_path = os.path.join(self.sync_full_path, d.WEBDATACONNECTORS_DIR)
        webdataconnectors_temp_path = os.path.join(backup_temp_dir, "webdataconnectors")
        logging.debug("Copying %s to %s..." % (webdataconnectors_abs_path,
                                               webdataconnectors_temp_path))
        shutil.copytree(webdataconnectors_abs_path,
                        webdataconnectors_temp_path)
        logging.debug("Webdataconnectors directory has been successfully copied!")

        # Copy won't be executed if the source dir is empty. Let's ensure that they exist
        for item in [dataengine_temp_path, webdataconnectors_temp_path]:
            if not os.path.exists(item):
                os.makedirs(item)

        timestamp = time.strftime("%Y%m%d%H%M%S")
        backup_zip_filename = "backup-%s.tsbak" % timestamp
        backup_zip_abs_path = os.path.join(self.backups_dir, backup_zip_filename)
        logging.debug("Zipping backup file to %s..." % backup_zip_abs_path)
        zip_command = "7z a -tzip -mx1 %s %s/*" % (backup_zip_abs_path, backup_temp_dir)
        try:
            self.__execute_cmd(cmd_str=zip_command)
        except OSError:
            raise EnvironmentManagerException("Seems like you do not have 7z installed!")
        logging.debug("Zipping has been successful!")

        logging.debug("Removing temporary backup directory...")
        shutil.rmtree(backup_temp_dir)
        logging.debug("Successfully removed temporary backup directory")

        logging.info("Backup file (%s) has been successfully created!" % backup_zip_abs_path)

    # Remove mount dirs
    def remove_mount_dirs(self):

        # Obtain list of mountpoints
        valid_mount_paths = [self.cluster_source_mount_full_path,
                             self.cluster_target_mount_full_path]
        # Ensure that missing target mount path is correctly handled
        valid_mount_paths = filter(lambda x: x is not None, valid_mount_paths)

        # Obtaining list of relevant mountpoints
        tableau_server_mountpoints = self.__get_relevant_mount_points(mount_paths=valid_mount_paths)

        logging.debug("Removing mountpoints...")
        mount_dirs = [self.cluster_source_mount_full_path, self.sync_full_path, self.cluster_target_mount_full_path]
        mount_dirs = filter(lambda x: x is not None,
                            mount_dirs)
        for mount_dir in mount_dirs:

            if mount_dir in tableau_server_mountpoints:
                logging.debug("Unmounting and deleting %s..." % mount_dir)
                try:
                    self.__execute_cmd(d.UMOUNT_CMD.format(mount_abs_path=mount_dir))
                    shutil.rmtree(mount_dir)
                except EnvironmentManagerException, e:
                    logging.debug("Tableau DR was not able to remove an existing mount point (%s) "
                                  "due to the following error: %s" % (mount_dir,
                                                                      str(e)))

            else:
                if not os.path.exists(mount_dir):
                    logging.debug("%s does not exist." % mount_dir)

                else:
                    logging.debug("Deleting %s..." % mount_dir)
                    shutil.rmtree(mount_dir)

    # Create mount dirs and do mount.cifs
    def create_mount_dirs(self, source_server, target_server=None, initial=True):

        # Obtain list of mountpoints
        valid_mount_paths = [self.cluster_source_mount_full_path,
                             self.cluster_target_mount_full_path,
                             self.filestore_temp_mount_dir]

        # Ensure that missing target mount path is correctly handled
        valid_mount_paths = filter(lambda x: x is not None, valid_mount_paths)

        # Obtaining list of relevant mountpoints
        tableau_server_mountpoints = self.__get_relevant_mount_points(mount_paths=valid_mount_paths)

        logging.debug("Creating mount directories...")
        mount_dirs = [self.cluster_source_mount_full_path, self.sync_full_path, self.cluster_target_mount_full_path]
        if self.tdfs_enabled:
            mount_dirs.append(self.filestore_temp_mount_dir)
            mount_rights = "rw"  # TDFS needs to update configs on Tableau

        mount_dirs = filter(lambda x: x is not None,
                            mount_dirs)
        for mount_dir in mount_dirs:

            if mount_dir in tableau_server_mountpoints:
                logging.debug("Unmounting %s..." % mount_dir)
                try:
                    self.__execute_cmd(d.UMOUNT_CMD.format(mount_abs_path=mount_dir))
                except EnvironmentManagerException, e:
                    logging.debug("Tableau DR was not able to remove an existing mount point (%s) "
                                  "due to the following error: %s" % (mount_dir,
                                                                      str(e)))

            else:
                if not os.path.exists(mount_dir):
                    logging.debug("%s does not exist! Creating it..." % mount_dir)
                    os.makedirs(mount_dir)

                elif os.listdir(mount_dir) != []:
                    if mount_dir == self.sync_full_path:
                        if initial:
                            logging.debug(
                                "Sync directory is not empty! This is most likely the sign of an earlier Tableau DR installation.")
                    else:
                        raise EnvironmentManagerException("The directory (%s) is not empty and it's "
                                                          "likely not caused by Tableau DR! Please make sure to run"
                                                          "uninstall before reattempting not "
                                                          "to store anything in the folders managed by Tableau DR!"
                                                          % mount_dir)

        # Create Samba credential files
        logging.debug("Creating mount points...")
        config_path = os.path.join(os.path.expanduser("~"),
                                   "tableau_dr",
                                   "config")
        if not os.path.exists(config_path):
            os.makedirs(config_path)

        prod_smb_cred_file_abs_path = os.path.join(config_path, ".smb_credentials_prod")
        smb_cred_prod_content = d.SMB_CREDENTIALS_CONTENT.format(user=source_server.user,
                                                                 password=source_server.password,
                                                                 domain=source_server.domain)
        open(prod_smb_cred_file_abs_path, "w").write(smb_cred_prod_content)

        mount_rights="ro"
        create_prod_mount_cmd = d.MOUNT_CIFS_CMD_DATA.format(server_host=source_server.host,
                                                             mount_abs_path=self.cluster_source_mount_full_path,
                                                             cred_file_path=prod_smb_cred_file_abs_path,
                                                             rescue_user=self.rescue_user,
                                                             failover_group=self.rescue_user,
                                                             rights=mount_rights)
        if os.listdir(self.cluster_source_mount_full_path) == []:
            self.__execute_cmd(create_prod_mount_cmd)

        if target_server is not None:
            dr_smd_cred_file_abs_path = os.path.join(config_path, ".smb_credentials_dr")
            smb_cred_dr_content = d.SMB_CREDENTIALS_CONTENT.format(user=target_server.user,
                                                                   password=target_server.password,
                                                                   domain=target_server.domain)
            open(dr_smd_cred_file_abs_path, "w").write(smb_cred_dr_content)

            mount_rights = "rw"
            create_dr_mount_cmd = d.MOUNT_CIFS_CMD_DATA.format(server_host=target_server.host,
                                                          mount_abs_path=self.cluster_target_mount_full_path,
                                                          cred_file_path=dr_smd_cred_file_abs_path,
                                                          rescue_user=self.rescue_user,
                                                          failover_group=self.rescue_user,
                                                          rights=mount_rights)

            if os.listdir(self.cluster_target_mount_full_path) == []:
                self.__execute_cmd(create_dr_mount_cmd)

        if self.tdfs_enabled:
            create_filestore_temp_mount_cmd =\
                d.MOUNT_CIFS_CMD_FILES.format(server_host=source_server.host,
                                              mount_abs_path=self.filestore_temp_mount_dir,
                                              cred_file_path=prod_smb_cred_file_abs_path,
                                              rescue_user=self.rescue_user,
                                              rights="rw")
            # if not os.path.exists(self.filestore_temp_mount_dir):
            self.__execute_cmd(create_filestore_temp_mount_cmd)

    def __manage_postgres(self, pg_absolute_dir, pg_data_dir, start=True):
        logging.debug("Managing Postgres is in progress...")

        operation = "start" if start else "stop"
        logging.debug("Postgres management operation is set to %s." % operation)
        manage_cmd = d.MANAGE_PG_COMMAND.format(pg_dir=pg_absolute_dir,
                                                operation=operation,
                                                 pg_data_dir=pg_data_dir,
                                                pg_data_dir_short=os.path.split(pg_data_dir)[1])
        try:
            self.__execute_cmd(cmd_str=manage_cmd,
                               as_unix_pg_user=True)
        except EnvironmentManagerException, e:
            logging.debug("Failed to run pg_ctl, error: %s" % e.message)

        logging.debug("Checking running Postgres after operation...")
        stdout, stderr = self.__execute_cmd(cmd_str="ps -ef")
        procs = stdout.splitlines()
        postgres_procs = filter(lambda x: "postgres" in x and
                                pg_data_dir in x.split(" "),
                                procs)
        time.sleep(1)
        if start:
            if len(postgres_procs) == 0:
                # TODO: Check logs to find out the reason
                raise EnvironmentManagerException("Postgres on Rescue Unix is not running after start!")

        # Now we can be pretty certain that the operation was successful
        logging.debug("Postgres %s operation was successful!" % operation)

    def __execute_cmd(self, cmd_str, as_unix_pg_user=False, stdin=None, cwd=None, env=None):

        def demote(user_uid):
            """Returns a demoter to {user_uid} function for subprocess.Popen(preexec_fn)"""
            def set_ids():
                os.setuid(user_uid)
            return set_ids

        if isinstance(env, dict):
            my_env = os.environ.copy()
            for k, v in env.iteritems():
                my_env[k] = v

        popen_as_shell = False

        #If rescue_user runs this command as postgresql user, modify the command according
        #to it's sudoer status.
        if as_unix_pg_user:
            if (True == self.is_sudoer):
                cmd_str = d.CMD_AS_PG_USER.format(cmd=cmd_str,
                                                  pg_dir=self.pg_absolute_dir)
            else:
                cmd_str = "{cmd1}; {cmd2}".format(cmd1=d.CMD_ROOT_AS_PG_USER.format(pg_dir=self.pg_absolute_dir),
                                                  cmd2=cmd_str)
                popen_as_shell=True
                # endif

        # If rescue_user is not sudoer and runs this command as itself, strip the sudo from the beginning of ehe command line, is any.
        else:
            if not self.is_sudoer:
                cmd_str = re.sub('^sudo ', '', cmd_str.rstrip())
        #endif

        #logging.warning("SHELL as %s cmd> %s" % (("postgresql" if as_unix_pg_user else self.rescue_user), cmd_str))

        if popen_as_shell:
            p = subprocess.Popen(cmd_str,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 stdin=subprocess.PIPE,
                                 cwd=cwd,
                                 env=env,
                                 shell=True,
                                 preexec_fn=demote(pwd.getpwnam('postgresql')[2]))
        else:
            p = subprocess.Popen(shlex.split(cmd_str),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             cwd=cwd,
                             env=env)

        stdout, stderr = p.communicate(input=stdin)

        #logging.warning("SHELL as %s out> %s" % (("postgresql" if as_unix_pg_user else self.rescue_user), stdout))
        #logging.warning("SHELL as %s err> %s" % (("postgresql" if as_unix_pg_user else self.rescue_user), stderr))

        if p.returncode != 0:
            raise EnvironmentManagerException(
                "Executing the following command was not successful: %s\nStatus code: %s\nSTDOUT: %s\nSTDERR: %s" % (
                    cmd_str,
                    p.returncode,
                    stdout,
                    stderr))

        return stdout, stderr

    def __get_relevant_rsync_jobs(self, crontab):
        logging.debug("Obtaining relevant cron jobs is in progress...")
        # Get rsync jobs
        rsync_cron_jobs = list(crontab.find_command("rsync"))  # Cast to list since find_command returns generator

        # Filter for relevant cron jobs that contain any of the absolute paths
        if self.cluster_target_mount_full_path is not None:
            relevant_cron_jobs = filter(lambda x: self.cluster_source_mount_full_path in x.command or
                                        self.sync_full_path in x.command or
                                        self.cluster_target_mount_full_path in x.command,
                                        rsync_cron_jobs)
        else:
            relevant_cron_jobs = filter(
                lambda x: self.cluster_source_mount_full_path in x.command or
                self.sync_full_path in x.command,
                rsync_cron_jobs)
        return relevant_cron_jobs

    def __test_server_connection(self, server):
        logging.debug("Testing connection to %s" % server.host)
        try:
            server.connect()
            server.test_connection()
        except BasicAuthDisabledError:
            raise ValidateEnvironmentException("Basic auth is not enabled on {host}!".format(host=server.host))
        except InvalidCredentialsError:
            raise ValidateEnvironmentException("Cannot connect to {host} due to invalid credentials!"
                                               .format(host=server.host))
        except AuthenticationError, e:
            raise ValidateEnvironmentException("Cannot connect to {host} due to authentication error: {error}"
                                               .format(host=server.host,
                                                       error=e))
        except WinRMError, e:
            raise ValidateEnvironmentException("Cannot connect to {host} due to generic WinRM error: {error}"
                                               .format(host=server.host,
                                                       error=e))
        except WinRMTransportError, e:
            raise ValidateEnvironmentException("Cannot connect to {host} due to transport-level problem: {error}"
                                               .format(host=server.host,
                                                       error=e))
        except WinRMOperationTimeoutError, e:
            raise ValidateEnvironmentException("Cannot connect to {host} due to operation timeout error: {error}"
                                               .format(host=server.host,
                                                       error=e))
        except KerberosExchangeError, e:
            raise ValidateEnvironmentException("Kerberos exchange error was encountered while connecting to {host}! "
                                               "Error: {error}"
                                               .format(host=server.host, error=e))

    # Validate that logs folder actually exists
    def __validate_pg_log_dir(self):
        logging.debug("Validating PG logs dir...")

        check_dir_exists_cmd = "ls {pg_absolute_dir}/logs".format (pg_absolute_dir = d.PG_ABSOLUTE_DIR)
        try:
            self.__execute_cmd(cmd_str=check_dir_exists_cmd,
                               as_unix_pg_user=True)
        except EnvironmentManagerException:
            logging.debug("The Postgres logs directory does not exist! Creating it...")
            pg_logs_mkdir_cmd = "mkdir {pg_absolute_dir}/logs".format (pg_absolute_dir = d.PG_ABSOLUTE_DIR)
            self.__execute_cmd(cmd_str=pg_logs_mkdir_cmd,
                               as_unix_pg_user=True)

    # Validate PG data directory
    def __validate_pg_datadir(self, data_dir, tab_server, pg_replication_user="tableau", stop_after_basebackup=False):
        logging.debug("Validating PG data dir %s..." % data_dir)
        if not os.path.exists(data_dir):
            logging.debug("The Postgres data directory (%s) does not exist! Creating it..." % data_dir)
            create_data_dir_cmd = "mkdir -m 775 %s" % data_dir
            self.__execute_cmd(create_data_dir_cmd, as_unix_pg_user=True)
            self.__validate_postgresql_conf(data_dir)
            self.__validate_recovery_conf(data_dir, tab_server, pg_replication_user)

            # Creating a basebackup
            tab_server.start()  # Ensuring that Tableau server (therefore Postgres) is running
            self.postgres_basebackup(tab_server.host, data_dir)
            if stop_after_basebackup:
                tab_server.stop()

        # Validate configuration files
        self.__validate_postgresql_conf(data_dir)
        self.__validate_recovery_conf(data_dir, tab_server, pg_replication_user)

    def __validate_postgresql_conf(self, data_dir):
        # TODO: Introduce methods to check the content of an existing postgresql.conf if it already exists!
        postgres_conf_absolute_path = os.path.join(data_dir, "postgresql.conf")
        logging.debug("Validating that %s exists..." % postgres_conf_absolute_path)
        if not os.path.exists(postgres_conf_absolute_path):
            logging.debug("The Postgres data directory (%s) does not contain postgresql.conf! "
                          "Creating it with predefined values..." % data_dir)
            config_file_content = d.POSTGRESQL_CONF_CONTENT
            write_config_file_cmd = "cat > %s" % postgres_conf_absolute_path
            self.__execute_cmd(write_config_file_cmd, as_unix_pg_user=True,
                               stdin=config_file_content)

    def __validate_recovery_conf(self, data_dir, tab_server, pg_replication_user):
        # TODO: Introduce methods to check the content of an existing recovery.conf if it already exists!
        recovery_conf_absolute_path = os.path.join(data_dir, "recovery.conf")
        logging.debug("Validating that %s exists..." % recovery_conf_absolute_path)
        if not os.path.exists(recovery_conf_absolute_path):
            logging.debug("The Postgres data directory (%s) does not contain recovery.conf! "
                          "Creating it with predefined values..." % data_dir)
            config_file_content = d.RECOVERY_CONF_CONTENT.format(host=tab_server.host,
                                                                 port=8060,
                                                                 user=pg_replication_user,
                                                                 password=self.pg_password)
            write_config_file_cmd = "cat > %s" % recovery_conf_absolute_path
            self.__execute_cmd(write_config_file_cmd, as_unix_pg_user=True, stdin=config_file_content)

            logging.debug("Recreating %s/postgresql.conf with predefined values..." % data_dir)
            postgres_conf_absolute_path = os.path.join(data_dir, "postgresql.conf")
            config_file_content = d.POSTGRESQL_CONF_CONTENT
            write_config_file_cmd = "cat > %s" % postgres_conf_absolute_path
            self.__execute_cmd(write_config_file_cmd, as_unix_pg_user=True,
                               stdin=config_file_content)

    # Function to overwrite/append the content of pgpass
    def __modify_pgpass(self, tab_host, remote_pg_port, remote_pg_db, remote_pg_user, remote_pg_password, append=True):
        logging.debug("Modifying the content of .pgpass...")
        pgpass_absolute_path = os.path.join(self.pg_absolute_dir, ".pgpass")
        # Reading and cleaning current content of .pgpass
        if os.path.exists(pgpass_absolute_path):
            pgpass_content, cat_stderr = self.__execute_cmd("cat %s" % pgpass_absolute_path,
                                                            as_unix_pg_user=True)
        else:
            logging.debug(".pgpass does not exist! ")
            pgpass_content = ""

        pgpass_content = pgpass_content.splitlines()
        pgpass_content = map(utils.clean_str, pgpass_content)

        # Creating/Modifying the content of .pgpass
        pgpass_file_new_content = d.PGPASS_FILE_CONTENT.format(host=tab_host,
                                                               port=remote_pg_port,
                                                               database=remote_pg_db,
                                                               user=remote_pg_user,
                                                               pwd=remote_pg_password)
        if append:
            overwrite_pgpass_cmd = "echo \"{pgpass_file_new_content}\" >> {pgpass_absolute_path}".format(
                pgpass_absolute_path = pgpass_absolute_path,
                pgpass_file_new_content = pgpass_file_new_content)
        else:
            overwrite_pgpass_cmd = "echo \"{pgpass_file_new_content}\" > {pgpass_absolute_path}".format(
                pgpass_absolute_path=pgpass_absolute_path,
                pgpass_file_new_content=pgpass_file_new_content)

        if pgpass_file_new_content not in pgpass_content:
            self.__execute_cmd(cmd_str=overwrite_pgpass_cmd,
                               as_unix_pg_user=True)

        pgpass_chmod_cmd = "chmod 600 {pgpass_absolute_path}".format(pgpass_absolute_path = pgpass_absolute_path)
        self.__execute_cmd(pgpass_chmod_cmd, as_unix_pg_user=True)

    # Test connection to remote Postgres
    def __test_local_pg_connection(self,
                                   pg_absolute_dir,
                                   pg_port,
                                   pg_user,
                                   pg_database):

        pg_host = "localhost"
        logging.debug("Testing connection to local Postgres at %s:%s" % (pg_host, pg_port))
        command = "{abs_dir}/bin/psql -h {host} -p {port} -U {user} {database} --no-password".format(abs_dir=pg_absolute_dir,
                                                                                             host=pg_host,
                                                                                             port=pg_port,
                                                                                             user=pg_user,
                                                                                             database=pg_database)
        command = d.CMD_AS_PG_USER.format(cmd=command,
                                          pg_dir=self.pg_absolute_dir)

        #logging.warning("SHELL cmd> %s" % command)
        psql_proc = subprocess.Popen(shlex.split(command),
                                     stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
        return_code = psql_proc.poll()
        if return_code is None:
            psql_proc.communicate(input="\\q")
        else:
            stdout, stderr = psql_proc.communicate()
            raise ValidateEnvironmentException("Wasn't able to connect to local Postgres at %s:%s!\n"
                                               "Stdout: %s\nStderr:%s" % (pg_host, pg_port, stdout, stderr))

    # Test connection to remote Postgres
    def __test_source_pg_connection(self,
                                    pg_absolute_dir,
                                    pg_host,
                                    pg_port,
                                    pg_user,
                                    pg_database,
                                    source_server,
                                    target_server=None,
                                    test_replication_role=False):

        if test_replication_role:
            logging.debug("Testing replication role...")
            test_cmd = "select rolreplication from pg_roles where rolname = 'tableau'; \\q"
        else:
            test_cmd = "\\q"

        logging.debug("Testing connection to Postgres at %s:%s" % (pg_host, pg_port))
        pg_host = source_server.host
        command = "{abs_dir}/bin/psql -h {host} -p {port} -U {user} {database} --no-password".format(abs_dir=pg_absolute_dir,
                                                                                             host=pg_host,
                                                                                             port=pg_port,
                                                                                             user=pg_user,
                                                                                             database=pg_database)
        command = d.CMD_AS_PG_USER.format(cmd=command,
                                          pg_dir=self.pg_absolute_dir)
        #logging.warning("SHELL cmd> %s" % command)

        psql_proc = subprocess.Popen(shlex.split(command),
                                     stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
        return_code = psql_proc.poll()
        retry_needed = False  # Boolean to indicate whether retrying to connect to Pg is needed
        if return_code is None:
            stdout, stderr = psql_proc.communicate(input=test_cmd)
            if test_replication_role:
                logging.debug("STDOUT: %s\nSTDERR: %s" % (stdout, stderr))
                repl_role_info = stdout.splitlines()
                repl_role_info = map(utils.clean_str, repl_role_info)
                if "f" in repl_role_info:
                    logging.warn("Replication role is not defined for user %s!" % pg_user)
                    self.__handle_psql_errors(stderr="no replication role",
                                              tab_server=source_server,
                                              start_tab_server=True)
                    if target_server is not None:
                        self.__handle_psql_errors(stderr="no replication role",
                                                  tab_server=target_server)

                elif "t" in repl_role_info:
                    logging.debug("Replication role is defined for user %s!" % pg_user)

                if psql_proc.returncode == 0:
                    logging.debug("Successfully connected to Postgres at %s:%s" % (pg_host, pg_port))
                else:
                    logging.debug("Wasn't able to connect to Postgres at %s:%s!\n"
                                  "Stdout: %s\nStderr:%s" % (pg_host, pg_port, stdout, stderr))
                    if "Connection refused" in stderr:
                        logging.warn(
                            "Connection refused encountered when attempting to connect to "
                            "Postgres of Tableau Server at %s!"
                            "If this is not expected, stop Disaster Recovery and take the necessary steps!" % pg_host)
                        retry_needed = False
                    else:
                        self.__handle_psql_errors(stderr=stderr, tab_server=source_server, start_tab_server=True)
                        if target_server is not None:
                            self.__handle_psql_errors(stderr="no replication role",
                                                      tab_server=target_server)
                        retry_needed = True

        else:
            stdout, stderr = psql_proc.communicate()
            logging.debug("Wasn't able to connect to Postgres at %s:%s!\n"
                          "Stdout: %s\nStderr:%s" % (pg_host, pg_port, stdout, stderr))
            self.__handle_psql_errors(stderr=stderr, tab_server=source_server, start_tab_server=True)
            if target_server is not None:
                self.__handle_psql_errors(stderr="no replication role",
                                          tab_server=target_server)
            retry_needed = True

        data_dir_short = os.path.split(self.cluster_source_pg_data_dir)[1]
        log_lines_list = self.__parse_pg_log(data_dir_short)
        if log_lines_list is not None:
            last_line = log_lines_list[-1]
            if "FATAL: no pg_hba.conf entry for replication connection from host" in last_line:
                self.__handle_psql_errors(stderr="no pg_hba.conf entry for host",
                                          tab_server=source_server,
                                          start_tab_server=True)
                if target_server is not None:
                    self.__handle_psql_errors(stderr="no replication role",
                                              tab_server=target_server)
                retry_needed = True

        return retry_needed

    # Function to handle different psql errors
    def __handle_psql_errors(self, stderr, tab_server, start_tab_server=False):
        stderr = str(stderr)
        if "password authentication failed for user" in stderr:
            logging.debug(
                "Was not able to authenticate as PG user 'tableau'! "
                "Changing user password...")
            tab_server.change_db_pass("tableau", self.pg_password)
        elif "no pg_hba.conf entry for host" in stderr:
            logging.debug(
                "Replication connection is not enabled for user 'tableau'. Enabling it...")
            tab_server.enable_user_replication_connection(pg_user="tableau",
                                                          ip=self.dr_unix_ip)
        elif "fe_sendauth: no password supplied" in stderr:
            logging.debug(".pgpass does not exist or has invalid content. Modifying it...")
            for db_name in [self.pg_database, "replication"]:
                self.__modify_pgpass(tab_host=tab_server.host,
                                     remote_pg_port=8060,
                                     remote_pg_db=db_name,
                                     remote_pg_user="tableau",
                                     remote_pg_password=self.pg_password,
                                     append=True)
        elif "no replication role" in stderr:
            logging.debug("No replication role is provided for 'tableau' user! Adding it...")
            tab_server.stop()
            tab_server.alter_user_role_replication(pg_user="tableau")
            if start_tab_server:
                tab_server.start()
        else:
            raise ValidateEnvironmentException("Wasn't able to fix connection to Postgres!\nError: %s" % stderr)

    # Parsing the tail of PG log
    def __parse_pg_log(self, data_dir):
        logging.debug("Parsing PG log for data dir %s is in progress..." % data_dir)
        log_filename = "postgresql_{pg_data_dir}.log".format(pg_data_dir=data_dir)
        log_abs_path = os.path.join(self.pg_absolute_dir, "logs", log_filename)
        tail_cmd = "tail {log_abs_path}".format(log_abs_path=log_abs_path)
        try:
            stdout, stderr = self.__execute_cmd(tail_cmd, as_unix_pg_user=True)
            stdout = str(stdout).splitlines()
            stdout = map(utils.clean_str, stdout)
            stdout = filter(lambda x: len(x) > 0,
                            stdout)
            logging.debug("Log content: %s" % stdout)
            return stdout
        except EnvironmentManagerException, e:
            logging.debug("Log could not be accessed! Error: %s" % e)
            return None

    def __get_relevant_mount_points(self, mount_paths):
        logging.debug("Obtaining list of valid mount directories...")
        disk_partitions = [item.mountpoint for item in psutil.disk_partitions(all=True)]
        relevant_mountpoints = filter(lambda x: x in mount_paths,
                                      disk_partitions)
        return relevant_mountpoints

    def delete_temp_pg_dir(self):
        tempdir = tempfile.gettempdir()
        temp_pg_dir = os.path.join(tempdir, "postgres")

        logging.debug("Deleting %s..." % temp_pg_dir)
        try:
            self.__execute_cmd("rm -rf %s" % utils.remove_trailing_slash(temp_pg_dir))
        except Exception,e:
            raise e
    #end def
