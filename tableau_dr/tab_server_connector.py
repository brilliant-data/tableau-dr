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

import logging
import os
import winrm
import winrm.exceptions as winrm_exc
from requests_kerberos.exceptions import KerberosExchangeError
import subprocess
import shlex
import time
from requests.exceptions import ReadTimeout
import defaults as d
import re
import utils

# Custom exception
class TableauServerConnectorException(Exception):
    pass


class TableauServerConnector:

    host = None  # Hostname of Windows server
    domain = None  # AD domain
    user = None  # User to connect to Windows server
    password = None  # The user's password
    winrm_session = None  # WinRM Session object
    tableau_install_dir = None  # Install directory of Tableau
    tableau_version = None  # Tableau version
    tableau_app_data_dir = None  # ProgramData directory
    protocol = None  # Protocol

    # Constructor
    def __init__(self,
                 host,
                 user,
                 password,
                 domain,
                 tableau_version,
                 tableau_app_data_dir,
                 tableau_install_dir="C:/Program Files/Tableau/Tableau Server",
                 protocol="nltm"):
        self.host = host
        logging.debug("Host is set to %s" % host)

        # Default domain is LOGON (for non-AD servers)
        if domain is None:
            domain = "LOGON"
        self.domain = domain
        logging.debug("Domain name is set to %s" % domain)

        self.user = user
        logging.debug("User is set to %s" % user)
        self.password = password
        logging.debug("Password is set.")
        self.tableau_install_dir = tableau_install_dir
        logging.debug("Tableau Server install directory has been set to %s" % tableau_install_dir)
        self.tableau_version = tableau_version
        logging.debug("Tableau Server version has been set to %s" % tableau_version)

        # Tableau ProgramData directory
        if tableau_app_data_dir is not None:
            self.tableau_app_data_dir = tableau_app_data_dir
            logging.debug("Tableau Server Program Data directory is set to %s." % tableau_app_data_dir)

        self.protocol = protocol
        logging.debug("WinRM transport protocol is set to %s" % protocol)

    def prepare_postgres_config(self):

        #create a backup of postgresql.conf.templ
        self.__execute_remote_command('Copy-Item "{copy_from}" "{copy_to}"'.format(
                copy_from=os.path.join(self.tableau_install_dir , str(self.tableau_version), "templates" , "postgresql.conf.templ"),
                  copy_to=os.path.join(self.tableau_install_dir , str(self.tableau_version), "templates" , "postgresql.conf.templ.bak")
            ),
            powershell=True
        )

        #modify postgresql.conf.templ
        self.__execute_remote_command(d.REPLACE_FILE_CONTENT_CMD.format(
                file_name= os.path.join(self.tableau_install_dir, str(self.tableau_version), "templates", "postgresql.conf.templ"),
                replace_from="wal_keep_segments = 32",
                replace_to="wal_keep_segments = 64`nwal_sender_timeout = 0"
            ),
            powershell=True
        )

        #create a backup of postgresql.conf
        self.__execute_remote_command('Copy-Item "{copy_from}" "{copy_to}"'.format(
                copy_from=os.path.join(self.tableau_app_data_dir, "data", "tabsvc", "config", "postgresql.conf"),
                  copy_to=os.path.join(self.tableau_app_data_dir, "data", "tabsvc", "config", "postgresql.conf.bak")
            ),
            powershell=True
        )

        #modify postgresql.conf
        self.__execute_remote_command(d.REPLACE_FILE_CONTENT_CMD.format(
                file_name=os.path.join(self.tableau_app_data_dir, "data", "tabsvc", "config", "postgresql.conf"),
                replace_from="wal_keep_segments = 32",
                replace_to="wal_keep_segments = 64`nwal_sender_timeout = 0"
            ),
            powershell=True
        )
    #end def

    # Function to connect to Windows server through WinRM
    def connect(self):
        logging.debug("Connecting to %s..." % self.host)
        winrm_session = winrm.Session(target=self.host,
                                      auth=("{user}@{domain}".format(user=self.user,
                                                                     domain=self.domain.upper()),
                                            self.password),
                                      transport=self.protocol,
                                      # TODO: May need to change this adaptively if an hour proves to be too long
                                      read_timeout_sec=3600)
        self.winrm_session = winrm_session

        # Determine ProgramData dir if not provided by user
        # TODO: Replace this with value provided by user
        if self.tableau_app_data_dir is None:
            system_drive = self.get_system_drive()
            app_data_dir = "{sys_drive}\\ProgramData\\Tableau\\Tableau Server".format(sys_drive=system_drive)
            self.tableau_app_data_dir = app_data_dir
            logging.debug("Tableau Server application data directory is set to %s." % app_data_dir)

    # Ensure that the provided Tableau paths are in fact valid
    def validate_tableau_paths(self):
        logging.debug("Validating Tableau Server paths on %s..." % self.host)
        tab_install_dir = "%s/%s" % (self.tableau_install_dir,
                                     self.tableau_version)
        tabadmin_exe_path = "%s/bin/tabadmin.exe" % tab_install_dir
        for tab_path in [tab_install_dir,
                         tabadmin_exe_path,
                         self.tableau_app_data_dir]:
            self.__validate_tableau_path(tab_path)

    # Function to get server status
    def status(self, verbose=False):
        logging.debug("Obtaining Tableau Server status...")
        command = "cd {tab_install_dir}/{tab_version}/bin & .\\tabadmin.exe status" \
            .format(tab_install_dir=self.tableau_install_dir,
                    tab_version=self.tableau_version)
        drive = self.tableau_install_dir.split(":")[0]
        command = "%s: & " % drive.upper() + command
        if verbose:
            command += " -v"
        stdout, stderr = self.__execute_remote_command(command)
        logging.debug("Tableau Server status: %s" % stdout)
        return stdout, stderr

    # Function to start Tableau server
    def start(self):
        if self.__is_tableau_server_running():
            logging.info("Tableau Server is running, therefore no start is needed!")
            return

        logging.debug("Starting Tableau Server...")
        command = "cd {tab_install_dir}/{tab_version}/bin & .\\tabadmin.exe start" \
            .format(tab_install_dir=self.tableau_install_dir,
                    tab_version=self.tableau_version)
        drive = self.tableau_install_dir.split(":")[0]
        command = "%s: & " % drive.upper() + command
        self.__execute_remote_command(command)
        if self.__is_tableau_server_running():
            logging.info("Tableau Server has been successfully started!")
        else:
            raise TableauServerConnectorException("Tableau Server does not appear to be running after start!")

    # Restore Tableau server
    def restore(self, tsbak_path):
        logging.debug("Stopping Tableau Server before restore...")
        self.stop()

        logging.debug("Restoring Tableau server is in progress...")
        command = "cd {tab_install_dir}/{tab_version}/bin & .\\tabadmin.exe restore " \
                  "--no-config " \
                  "--password {password} " \
                  "\"{tsbak_abs_path}\"" \
            .format(tab_install_dir=self.tableau_install_dir,
                    tab_version=self.tableau_version,
                    tsbak_abs_path=tsbak_path,
                    password=self.password)
        drive = self.tableau_install_dir.split(":")[0]
        command = "%s: & " % drive.upper() + command
        self.__execute_remote_command(command)
        logging.info("Restoring Tableau Server has been successful!")

    # Function to stop Tableau server
    def stop(self):
        if not self.__is_tableau_server_running():
            logging.debug("Tableau Server is not running, therefore no stopping is needed!")
            return

        logging.debug("Stopping Tableau Server...")
        command = "cd {tab_install_dir}/{tab_version}/bin & .\\tabadmin.exe stop" \
            .format(tab_install_dir=self.tableau_install_dir,
                    tab_version=self.tableau_version)
        drive = self.tableau_install_dir.split(":")[0]
        command = "%s: & " % drive.upper() + command
        self.__execute_remote_command(command)
        logging.debug("Obtaining Server status...")
        if self.__is_tableau_server_running():
            raise TableauServerConnectorException("Tableau Server appears to be running after stop!")
        else:
            logging.info("Tableau Server has been successfully stopped!")

    # Function to change Postgres password
    def change_db_pass(self, db_user, db_password):
        logging.debug("Changing database pass for user %s..." % db_user)
        command = "cd {tab_install_dir}/{tab_version}/bin & .\\tabadmin.exe dbpass --username {user} {password}" \
            .format(tab_install_dir=self.tableau_install_dir,
                    tab_version=self.tableau_version,
                    user=db_user,
                    password=db_password)
        drive = self.tableau_install_dir.split(":")[0]
        command = "%s: & " % drive.upper() + command
        self.__execute_remote_command(command)

    # Tableau Server Solr reindex
    def reindex(self):
        logging.debug("Checking whether Tableau Server is running...")
        if self.__is_tableau_server_running():
            logging.warn("Tableau Server running! Reindexing can still run, but will be considerably slower"
                         " than it would be on a stopped Tableau Server!")
        command = "cd {tab_install_dir}/{tab_version}/bin & .\\tabadmin.exe reindex" \
            .format(tab_install_dir=self.tableau_install_dir,
                    tab_version=self.tableau_version)
        drive = self.tableau_install_dir.split(":")[0]
        command = "%s: & " % drive.upper() + command
        logging.debug("Reindexing Tableau Server...")
        self.__execute_remote_command(command)

    # Restore Postgres
    def restore_postgres(self):
        logging.debug("Restoring Postgres...")
        pg_dump_file_path = "{tableau_app_data_dir}/{pg_dump_file}".\
            format(tableau_app_data_dir=self.tableau_app_data_dir,
                   pg_dump_file=d.WORKGROUP_PG_DUMP_FILE)
        self.__run_psql_query(file_path=pg_dump_file_path)

    # Grant alter role replication to tableau user
    def alter_user_role_replication(self, pg_user):
        logging.debug("Altering the role of %s to include replication..." % pg_user)
        self.__run_psql_query(query="alter role %s with replication;" % pg_user)

    # Enable replication connections from the user
    def enable_user_replication_connection(self, pg_user, ip):
        logging.debug("Enabling replication connection from user %s..." % pg_user)
        replication_hba_conf = "host replication {pg_user} {ip}/32 md5".format(pg_user=pg_user,
                                                                               ip=ip)
        self.__append_pg_hba_conf_template(replication_hba_conf)
        self.__append_pg_hba_conf(replication_hba_conf)

    # Stop Postgres
    def stop_postgres(self):
        logging.debug("Stopping Postgres...")
        command = "cd {tab_install_dir}/{tab_version}/pgsql/bin & " \
                  "pg_ctl.exe stop -D \"{tableau_app_data_dir}/data/tabsvc/pgsql/data\" -o \"-p 8060\" -w" \
            .format(tableau_app_data_dir=self.tableau_app_data_dir,
                    tab_install_dir=self.tableau_install_dir,
                    tab_version=self.tableau_version)
        self.__execute_remote_command(command)
        logging.info("Postgres has been successfully stopped!")

    # Start Postgres
    def start_postgres(self):
        logging.debug("Starting Postgres...")

        # Postgres can only be started by an unelevated user
        if self.__is_elevated():
            logging.warn("Elevated shell detected!")

        command = "cd {tab_install_dir}/{tab_version}/pgsql/bin & " \
                  "pg_ctl.exe start -D \"{tableau_app_data_dir}/data/tabsvc/pgsql/data\" -o \"-p 8060\" -W" \
            .format(tableau_app_data_dir=self.tableau_app_data_dir,
                    tab_install_dir=self.tableau_install_dir,
                    tab_version=self.tableau_version)
        drive = self.tableau_install_dir.split(":")[0]
        command = "%s: & " % drive.upper() + command
        self.__execute_remote_command(command)
        logging.info("Postgres has been successfully started!")

    # Function to tests the connection
    def test_connection(self):
        logging.debug("Testing connection to %s..." % self.host)
        self.__execute_remote_command("whoami")

    # delete net share
    def delete_net_share(self):
        try:
            self.__execute_remote_command(d.NET_SHARE_DELETE)
        except TableauServerConnectorException:
            logging.debug("Net share nonexistent.")

    # Create net share
    def net_share_tab_data(self):
        logging.debug("Sharing Tableau server's Program Data directory...")
        net_share_cmd = d.NET_SHARE_DATA_CMD.format(programdata_dir=self.tableau_app_data_dir,
                                                    domain=self.domain.upper(),
                                                    user=self.user)
        try:
            self.__execute_remote_command(net_share_cmd)
        except TableauServerConnectorException, e:
            if "The name has already been shared" in str(e):
                logging.debug("Net share has already been done for %s!" % self.host)
                self.delete_net_share()
                self.__execute_remote_command(net_share_cmd)
            else:
                raise TableauServerConnectorException("The following error was encountered while trying"
                                                      "to share Tableau application data: %s" % e)

    # Function to ensure that runas user has the appropriate permissions for Program Data\Tableau
    def ensure_app_data_permissions(self):
        logging.debug("Ensuring that Tableau runas user has the appropriate write permission "
                      "for the Tableau Server application folder...")
        change_program_data_dir_acl_cmd = d.REMOTE_ACL_COMMAND.format(path=self.tableau_app_data_dir,
                                                                      user=self.user,
                                                                      domain=self.domain.upper())
        try:
            self.__execute_remote_command(change_program_data_dir_acl_cmd, powershell=True)
        except TableauServerConnectorException, e:
            logging.warn(
                "Tableau DR was not able to automatically change permissions for %s! "
                "Due to the following error: %s\n"
                "Please ensure that the runas user has write permission to that folder!" % (self.tableau_app_data_dir,
                                                                                            str(e)))

    # Function to get current system drive
    def get_system_drive(self):
        stdout, stderr = self.__execute_remote_command("echo %systemdrive%")
        system_drive = utils.clean_str(stdout)
        return system_drive

    # Function to get execution policy
    def validate_exec_policy(self):
        stdout, stderr = self.__execute_remote_command("Get-ExecutionPolicy", powershell=True)
        exec_policy = stdout.strip()
        logging.debug("Execution policy is set to %s." % exec_policy)
        if exec_policy != "Unrestricted":
            raise TableauServerConnectorException("Execution policy on server {host} is set to {exec_policy}! "
                                                  "Tableau DR needs unrestricted execution policy to be"
                                                  "able to run. "
                                                  "Run the following on the Windows server in an elevated shell: "
                                                  "Set-ExecutionPolicy Unrestricted".format(host=self.host,
                                                                                            exec_policy=exec_policy))
        logging.info("Execution policy is correct on server %s!" % self.host)

    # Function to download a file
    def download_file(self, source_url, destination_path):
        logging.debug("Downloading %s..." % source_url)

        download_ps_cmd = "(New-Object System.Net.WebClient).DownloadFile(\"{url}\", \"{dest_path}\")" \
            .format(url=source_url,
                    dest_path=destination_path)
        self.__execute_remote_command(download_ps_cmd,
                                      powershell=True)

    def validate_winrm_config(self):
        logging.debug("Validating WinRM config for %s..." % self.host)
        winrm_config_cmd = "winrm get winrm/config"
        stdout, stderr = self.__execute_remote_command(winrm_config_cmd,
                                                       powershell=True)
        winrm_config_content = stdout.splitlines()
        winrm_config_content = map(utils.clean_str,
                                   winrm_config_content)
        maxmem_line = filter(lambda x: "MaxMemoryPerShellMB" in x,
                             winrm_config_content)[0]
        maxmem_value = re.findall('\d+', maxmem_line)[0]
        if int(maxmem_value) < d.MIN_ALLOWED_WINRM_SHELL_MEMORY:
            raise TableauServerConnectorException("WinRM shell sessions are not allocated sufficient memory for "
                                                  "Tableau DR to run! Please set it to at least {min_mem} "
                                                  "by running the following "
                                                  "Powershell command in an elevated shell: "
                                                  "winrm set winrm/config/winrs '@{MaxMemoryPerShellMB=\"{min_mem}\"}'"
                                                  .format(min_mem=d.MIN_ALLOWED_WINRM_SHELL_MEMORY))

    # Function to check whether Tableau Server is running or not
    def __is_tableau_server_running(self):
        logging.debug("Checking whether Tableau Server is running...")
        status_stdout, status_stderr = self.status()
        if "RUNNING" in status_stdout:
            logging.debug("Tableau Server is running!")
            return True
        else:
            logging.debug("Tableau Server is not running!")
            return False

    # Function to execute an arbitrary command
    def __execute_remote_command(self, command, powershell=False, retries=0, wait_before_retry=15):
        run_cmd = self.__obtain_run_cmd(powershell)

        for retry_loop_counter in range(retries+1):
            if (retry_loop_counter>0):
                time.sleep(wait_before_retry)
            #endif
            try:
                #logging.warning("%s> %s" % (self.host, command))
                cmd = run_cmd(command)
            except winrm_exc.BasicAuthDisabledError:
                raise TableauServerConnectorException("Basic auth is not enabled on {host}!".format(host=self.host))
            except winrm_exc.InvalidCredentialsError:
                raise TableauServerConnectorException("Cannot connect to {host} due to invalid credentials!"
                                                      .format(host=self.host))
            except winrm_exc.AuthenticationError, e:
                raise TableauServerConnectorException("Cannot connect to {host} due to authentication error: {error}"
                                                      .format(host=self.host,
                                                              error=e))
            except winrm_exc.WinRMError, e:
                raise TableauServerConnectorException("Cannot connect to {host} due to generic WinRM error: {error}"
                                                      .format(host=self.host,
                                                              error=e))
            except winrm_exc.WinRMTransportError, e:
                raise TableauServerConnectorException("Cannot connect to {host} due to transport-level problem: {error}"
                                                      .format(host=self.host,
                                                              error=e))

            except ReadTimeout:
                logging.debug("Read Timeout encountered! Attempting to reconnect and rerun the command...")
                self.connect()
                run_cmd = self.__obtain_run_cmd(powershell)
                cmd = run_cmd(command)

            except winrm_exc.WinRMOperationTimeoutError:
                # TODO: Investigate this in more detail!
                logging.debug("Operation has timed out. Establishing a new session...")
                self.connect()
                run_cmd = self.__obtain_run_cmd(powershell)
                cmd = run_cmd(command)
            except KerberosExchangeError:
                logging.debug("Kerberos Exchange error encountered! Attempting to kinit...")
                kinit_cmd = d.KINIT_CMD.format(pwd=self.password,
                                               user=self.user,
                                               domain=self.domain.upper())
                try:
                    self.__execute_local_cmd(kinit_cmd)
                    time.sleep(1)
                except TableauServerConnectorException, e:
                    raise TableauServerConnectorException(
                        "Kerberos-related issue was encountered, but could not be resolved!.\nError: %s" % e)
                cmd = run_cmd(command)

            if cmd.status_code == 0:
                return cmd.std_out, cmd.std_err
            else:
                logging.debug("Remote command returned status code %s..." % cmd.status_code)

        #endfor
        raise TableauServerConnectorException(
            "The command (%s) returned %s\nSTDOUT: %s \nSTDERR: %s" % (command,
                                                                       cmd.status_code,
                                                                   cmd.std_out,
                                                                   cmd.std_err))

    # Function to determine whether current shell is elevated or not
    def __is_elevated(self):
        logging.debug("Testing whether current shell is elevated...")
        try:
            # TODO: Replace this with something more "elegant"
            command = "net session"
            self.__execute_remote_command(command)
            logging.warn("Current shell is elevated!")
            return True
        except:
            logging.debug("Current shell is not elevated!")
            return False

    # A helper function to get the exact command runner function
    def __obtain_run_cmd(self, powershell):
        if powershell:
            run_cmd = self.winrm_session.run_ps
        else:
            run_cmd = self.winrm_session.run_cmd
        return run_cmd

    # Run psql queries
    def __run_psql_query(self, query=None, file_path=None):
        logging.debug("Running Psql query on Tableau Server's Postgres is in progress...")

        logging.debug("Finding out the location of temporary directory")
        tempdir = self.__obtain_temp_dir()
        timestr = time.strftime("%Y%m%d-%H%M%S")
        pgctl_temp_file = "{tempdir}\\pgctl_cmd_{timestamp}".format(tempdir=tempdir,
                                                                    timestamp=timestr)

        if file_path is None and query is not None:
            query_to_temp_file = "echo \"{query}\" | Out-File -Encoding ASCII -FilePath {temppath}".\
                format(temppath=pgctl_temp_file,
                       query=query)
            self.__execute_remote_command(query_to_temp_file, powershell=True)
            pg_cmd = d.REMOTE_PSQL_COMMAND + " -RedirectStandardInput \"%s\"" % pgctl_temp_file

        elif file_path is not None and query is None:
            pg_cmd = d.REMOTE_PSQL_COMMAND + " -RedirectStandardInput \"{file_path}\"".format(file_path=file_path)
        else:
            raise TableauServerConnectorException("Cannot run Psql query without an input file or a query!")

        pg_cmd = pg_cmd.format(tab_install_dir=self.tableau_install_dir,
                               tab_version=self.tableau_version,
                               pg_database="workgroup_test",
                               user="tblwgadmin")

        stdout, stderr = self.__run_remote_pg_cmd(pg_cmd)

        # Remove temporary file
        test_path_delete_item_cmd = "If(Test-Path \"%s\"){ Remove-Item \"%s\" }" % (pgctl_temp_file, pgctl_temp_file)
        self.__execute_remote_command(test_path_delete_item_cmd,
                                      powershell=True)

        return stdout, stderr

    # Function to simplify executing commands that require that Tableau's postgres is running
    def __run_remote_pg_cmd(self, ps_cmd):
        start_pg_cmd = d.START_PG_PS.format(tableau_app_data=self.tableau_app_data_dir,
                                            tab_install_dir=self.tableau_install_dir,
                                            tab_version=self.tableau_version)
        check_pg_status_cmd = d.CHECK_PG_STATUS.format(tab_install_dir=self.tableau_install_dir,
                                                       tableau_app_data=self.tableau_app_data_dir,
                                                       tab_version=self.tableau_version)
        command = "$statusProc = {pg_status_cmd}; " \
                  "If($statusProc.ExitCode -eq 0){{ " \
                  "echo 'PG is running'; {specific_cmd}; }} " \
                  "Else {{ " \
                  "echo 'Starting PG since it is not running'; " \
                  "$pgProc = {start_pg_cmd}; " \
                  "While($pgProc.hasExited -eq $False){{ Start-Sleep -s 1 }} " \
                  "If($pgProc.ExitCode -eq 0){{ " \
                  "{specific_cmd}; }} " \
                  "Else {{ exit 1 }} }}".format(pg_status_cmd=check_pg_status_cmd,
                                                start_pg_cmd=start_pg_cmd,
                                                specific_cmd=ps_cmd)
        stdout, stderr = self.__execute_remote_command(command, powershell=True)
        return stdout, stderr

    # Add line to pg_hba.conf template
    def __append_pg_hba_conf_template(self, line):
        logging.debug("Adding the following line to pg_hba.conf.templ: %s" % line)

        # Get pg_hba.conf's current content
        current_content_cmd = \
            "cat \"{tab_install_dir}\\{tab_version}\\templates\\pg_hba.conf.templ\"".format(
                tab_install_dir=self.tableau_install_dir.replace("/", "\\"),
                tab_version=self.tableau_version)
        current_content, stderr = self.__execute_remote_command(current_content_cmd, powershell=True)
        current_content = map(utils.clean_str, current_content.splitlines())

        if line not in current_content:
            append_cmd = "Add-Content \"{tab_install_dir}\\{tab_version}\\templates\\pg_hba.conf.templ\" \"`n{line}\"" \
                .format(tab_install_dir=self.tableau_install_dir.replace("/", "\\"),
                        tab_version=self.tableau_version, line=line)
            stdout, stderr = self.__execute_remote_command(append_cmd, powershell=True)
            return stdout, stderr
        else:
            return None, None

    # Add line to pg_hba.conf
    def __append_pg_hba_conf(self, line):
        logging.debug("Adding the following line to pg_hba.conf: %s" % line)

        # Get pg_hba.conf's current content
        current_content_cmd = \
            "cat \"{tableau_app_data}\\data\\tabsvc\\config\\pg_hba.conf\"".format(
                tableau_app_data=self.tableau_app_data_dir)
        current_content, stderr = self.__execute_remote_command(current_content_cmd, powershell=True)
        current_content = map(utils.clean_str, current_content.splitlines())

        if line not in current_content:
            append_cmd = "Add-Content \"{tableau_app_data}\\data\\tabsvc\\config\\pg_hba.conf\" \"`n{line}\"" \
                .format(tableau_app_data=self.tableau_app_data_dir,
                        line=line)
            stdout, stderr = self.__execute_remote_command(append_cmd, powershell=True)
            return stdout, stderr
        else:
            return None, None

    def __execute_local_cmd(self, cmd_str, stdin=None):

        logging.debug("Executing the following command: %s..." % cmd_str)
        p = subprocess.Popen(shlex.split(cmd_str),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             stdin=subprocess.PIPE)

        #logging.warning("SHELL cmd> %s" % cmd_str)
        stdout, stderr = p.communicate(input=stdin)
        #logging.warning("SHELL out> %s" % stdout)
        #logging.warning("SHELL err> %s" % stderr)

        if p.returncode != 0:
            raise TableauServerConnectorException(
                "Executing the command was not successful!\nStatus code: %s\nSTDOUT: %s\nSTDERR: %s" % (
                    p.returncode,
                    stdout,
                    stderr))

        return stdout, stderr

    def __obtain_temp_dir(self):
        stdout, stderr = self.__execute_remote_command("ECHO %Temp%")
        tempdir = utils.clean_str(stdout)
        return tempdir

    def __validate_tableau_path(self, tab_path):
        logging.debug("Validating the following path: %s" % tab_path)
        test_path_cmd = d.TEST_PATH_PS_CMD.format(path=tab_path)
        try:
            self.__execute_remote_command(test_path_cmd, powershell=True)
        except:
            raise TableauServerConnectorException("The following path does not seem to exist on %s: %s"
                                                  % (self.host, tab_path))
