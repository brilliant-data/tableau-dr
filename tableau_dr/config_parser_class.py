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
from tab_server_connector import TableauServerConnector
import defaults
import socket
import os


# Custom exception
class ConfigParserException(Exception):
    pass


# A class for parsing configuration values
class ConfigParser:

    cluster_data = None
    reverse = None
    single_cluster = False
    reverse = False
    tdfs_enabled = None

    # Constructor
    def __init__(self, cluster_data):
        logging.debug("Configuration parser object is being initialized!")
        self.cluster_data = cluster_data
        servers_block = self.__get_servers_block(cluster_data)
        if servers_block.get("target") is None:
            self.single_cluster = True
            logging.info("There is no disaster recovery Tableau Server defined, running in single cluster mode.")
        logging.debug("Validating the configuration file is in progress...")
        self.__validate_config_file(cluster_data=cluster_data)
        logging.info("The configuration file has been successfully validated!")
        reverse_switch = cluster_data.get("reverse")
        logging.debug("Reverse switch is set to %s." % reverse_switch)
        if reverse_switch is True:
            if self.single_cluster:
                raise ConfigParserException("Cannot reverse Tableau DR direction in a single cluster setting!")
            self.reverse = True
            logging.info("Tableau DR direction is reversed (DR -> Prod)!")

    # Obtain IP for DR Unix
    def obtain_ip(self):
        rescue_env = self.cluster_data.get("rescue_env")
        dr_ip = rescue_env.get("ip")
        if dr_ip is None:
            dr_ip = socket.gethostbyname(socket.gethostname())
            if dr_ip == "127.0.0.1":
                raise ConfigParserException("Was not able to determine DR Unix's IP automatically!\n"
                                            "Please provide the value in the config file!")
        logging.debug("DR Unix IP is determined to be %s" % dr_ip)
        return dr_ip

    # Obtain source server object
    def get_source_server(self):
        servers_block = self.cluster_data.get("servers")
        if self.reverse:
            source_data = servers_block.get("target")
        else:
            source_data = servers_block.get("source")
        domain, host, user, password, connection_protocol = self.__get_connection_data(source_data)
        tableau_install_dir, tableau_app_data_dir, tableau_version = self.__get_tableau_data(source_data)

        # Default protocol is NTLM
        if connection_protocol is None:
            connection_protocol = "ntlm"
            logging.info("Connection protocol is not set, using NTLM.")
        elif connection_protocol not in defaults.SUPPORTED_WINRM_PROTOCOLS:
                raise ConfigParserException("The following transport protocol is not supported by Tableau DR: %s!\n"
                                            "Possible options: %s"
                                            % (connection_protocol, ", ".join(defaults.SUPPORTED_WINRM_PROTOCOLS)))

        # Ensure that domain is provided when using Kerberos auth
        if connection_protocol == "kerberos" and domain is None:
            raise ConfigParserException("You need to provide the domain name to use Kerberos authentication!")

        source_server = TableauServerConnector(host=host,
                                               user=user,
                                               password=password,
                                               domain=domain,
                                               tableau_install_dir=tableau_install_dir,
                                               tableau_app_data_dir=tableau_app_data_dir,
                                               tableau_version=tableau_version,
                                               protocol=connection_protocol)
        source_server.connect()
        return source_server

    def get_target_server(self):
        servers_block = self.cluster_data.get("servers")
        if not self.single_cluster:
            if self.reverse:
                target_data = servers_block.get("source")
            else:
                target_data = servers_block.get("target")
            domain, host, user, password, connection_protocol = self.__get_connection_data(target_data)
            tableau_install_dir, tableau_app_data_dir, tableau_version = self.__get_tableau_data(target_data)

            # Default protocol is NTLM
            if connection_protocol is None:
                connection_protocol = "ntlm"
                logging.info("Connection protocol is not set, using NTLM.")
            elif connection_protocol not in defaults.SUPPORTED_WINRM_PROTOCOLS:
                raise ConfigParserException("The following transport protocol is not supported by Tableau DR: %s!\n"
                                            "Possible options: %s"
                                            % (connection_protocol, ", ".join(defaults.SUPPORTED_WINRM_PROTOCOLS)))

            # Ensure that domain is provided when using Kerberos auth
            if connection_protocol == "kerberos" and domain is None:
                raise ConfigParserException("You need to provide the domain name to use Kerberos authentication!")

            target_server = TableauServerConnector(host=host,
                                                   user=user,
                                                   password=password,
                                                   domain=domain,
                                                   tableau_install_dir=tableau_install_dir,
                                                   tableau_app_data_dir=tableau_app_data_dir,
                                                   tableau_version=tableau_version,
                                                   protocol=connection_protocol)
            target_server.connect()
            return target_server
        else:
            return None

    def recovery_data(self):
        rescue_env = self.cluster_data.get("rescue_env")
        rescue_user, is_sudoer, cluster_a_root_dir, cluster_b_root_dir, cluster_sync_root_dir, mount_dir, backups_dir, \
            tdfs_enabled, filestore_bin_dir, filestore_temp_mount_dir, tab_data_config_dir, dataengine_dir = \
            self.__get_rescue_env_data(rescue_env)

        if self.single_cluster:
            cluster_b_root_dir = None

        if self.reverse:
            return rescue_user, is_sudoer, cluster_b_root_dir, cluster_a_root_dir, cluster_sync_root_dir, mount_dir, \
                backups_dir, tdfs_enabled, filestore_bin_dir, filestore_temp_mount_dir, tab_data_config_dir, dataengine_dir
        else:
            return rescue_user, is_sudoer, cluster_a_root_dir, cluster_b_root_dir, cluster_sync_root_dir, mount_dir, \
                backups_dir, tdfs_enabled, filestore_bin_dir, filestore_temp_mount_dir, tab_data_config_dir, dataengine_dir

    def postgres_data(self):
        rescue_env = self.cluster_data.get("rescue_env")
        tableau_dr_dir = rescue_env.get("rescue_dir")
        absolute_dir, port, user, password, database, rescue_dir_pgsql_root_dir, data_a_dir, data_b_dir = \
            self.__get_postgres_data(postgres_data=rescue_env.get("postgres"),
                                     rescue_dir=tableau_dr_dir)
        if self.single_cluster:
            data_b_dir = None
        if self.reverse:
            return absolute_dir, port, user, password, database, rescue_dir_pgsql_root_dir, data_b_dir, data_a_dir
        else:
            return absolute_dir, port, user, password, database, rescue_dir_pgsql_root_dir, data_a_dir, data_b_dir

    def __get_servers_block(self, cluster_data):
        servers_block = cluster_data.get("servers")
        return servers_block

    def __get_connection_data(self, server_data):
        host = server_data.get("host")
        domain = server_data.get("domain")
        user = server_data.get("user")
        password = server_data.get("password")
        connection_protocol = server_data.get("protocol")
        return domain, host, user, password, connection_protocol

    # Get Tableau data for Tableau server
    def __get_tableau_data(self, server_data):
        tableau_data = server_data.get("tableau")
        tableau_install_dir = tableau_data.get("install_dir")
        tableau_app_data_dir = tableau_data.get("app_data_dir")
        tableau_version = self.__validate_tableau_version(tableau_data.get("version"))
        return tableau_install_dir, tableau_app_data_dir, tableau_version

    # Get recovery environment data
    def __get_rescue_env_data(self, rescue_env_data):
        rescue_user = rescue_env_data.get("rescue_user")
        is_sudoer = (rescue_env_data.get("is_sudoer") == True)
        rescue_dir = rescue_env_data.get("rescue_dir")
        data_root_dir = os.path.join(rescue_dir, "data")
        cluster_a_root_dir = os.path.join(data_root_dir, "prod")
        cluster_b_root_dir = os.path.join(data_root_dir, "dr")
        cluster_sync_root_dir = os.path.join(data_root_dir, "sync")
        mount_dir = defaults.CLUSTER_SERVER_DIR
        backups_dir = os.path.join(rescue_dir, "backups")
        # TODO: TDFS-specific variables need to be enforced/validated!
        tdfs_enabled = rescue_env_data.get("tdfs")
        if not tdfs_enabled:
            filestore_bin_dir = ""
            filestore_temp_mount_dir = ""
            tab_data_config_dir = ""
            dataengine_dir = ""
        else:
            #TODO: TDFS-related variables to a separate block in config file!
            filestore_app_dir = rescue_env_data.get("filestore_app_dir")
            filestore_temp_mount_dir = rescue_env_data.get("filestore_temp_mount_dir")
            tab_data_config_dir = rescue_env_data.get("tab_data_config_dir")
            dataengine_dir = rescue_env_data.get("dataengine_dir")
            filestore_bin_dir = os.path.join(rescue_dir, filestore_app_dir)
            filestore_temp_mount_dir = os.path.join(rescue_dir, filestore_temp_mount_dir)

        return rescue_user, is_sudoer, cluster_a_root_dir, cluster_b_root_dir, cluster_sync_root_dir, mount_dir, backups_dir, \
            tdfs_enabled, filestore_bin_dir, filestore_temp_mount_dir, tab_data_config_dir, dataengine_dir

    # Obtain Postgres data (on the Unix environment)
    def __get_postgres_data(self, postgres_data, rescue_dir):
        absolute_dir = postgres_data.get("absolute_dir")
        if absolute_dir is None:
            absolute_dir = defaults.PG_ABSOLUTE_DIR
        port = postgres_data.get("port")
        if port is None:
            port = defaults.PG_PORT
        user = defaults.PG_USER
        password = postgres_data.get("password")
        database = defaults.PG_DATABASE
        rescue_dir_pgsql_root_dir = os.path.join(rescue_dir, "pgsql")
        data_a_dir = os.path.join(rescue_dir_pgsql_root_dir, "prod")
        data_b_dir = os.path.join(rescue_dir_pgsql_root_dir, "dr")
        return absolute_dir, port, user, password, database, rescue_dir_pgsql_root_dir, data_a_dir, data_b_dir

    def __validate_config_file(self, cluster_data):
        self.__validate_config_dict(dict=cluster_data,
                                    desired_keys=defaults.CONTEXT_KEYS,
                                    dict_human_readable_name="Tableau Server cluster's context block")
        servers_block = cluster_data.get("servers")
        self.__validate_config_dict(dict=servers_block,
                                    desired_keys=defaults.SERVERS_BLOCK_KEYS,
                                    dict_human_readable_name="Tableau Servers block")
        source_server_data = servers_block.get("source")
        self.__validate_config_dict(dict=source_server_data,
                                    desired_keys=defaults.SERVER_DATA_KEYS,
                                    dict_human_readable_name="source Tableau Server's block")
        self.__validate_config_dict(dict=source_server_data.get("tableau"),
                                    desired_keys=defaults.TABLEAU_DATA_KEYS,
                                    dict_human_readable_name="source Tableau Server data")
        target_server_data = servers_block.get("target")
        if target_server_data is not None:
            self.__validate_config_dict(dict=target_server_data,
                                        desired_keys=defaults.SERVER_DATA_KEYS,
                                        dict_human_readable_name="target Tableau Server's block")
            self.__validate_config_dict(dict=target_server_data.get("tableau"),
                                        desired_keys=defaults.TABLEAU_DATA_KEYS,
                                        dict_human_readable_name="target Tableau Server data")

        rescue_env_data = cluster_data.get("rescue_env")
        tdfs_enabled = rescue_env_data.get("tdfs")
        if not tdfs_enabled:
            self.__validate_config_dict(dict=rescue_env_data,
                                        desired_keys=defaults.RESCUE_ENV_KEYS,
                                        dict_human_readable_name="recovery environment parameters block")
        else:
            rescue_env_keys = defaults.RESCUE_ENV_KEYS + ["filestore_app_dir",
                                                              "filestore_temp_mount_dir",
                                                              "tab_data_config_dir",
                                                              "dataengine_dir"]
            self.__validate_config_dict(dict=rescue_env_data,
                                        desired_keys=rescue_env_keys,
                                        dict_human_readable_name="recovery environment parameters block")
        recovery_postgres_data = rescue_env_data.get("postgres")
        self.__validate_config_dict(dict=recovery_postgres_data,
                                    desired_keys=defaults.RECOVERY_POSTGRES_KEYS,
                                    dict_human_readable_name="recovery environment Postgres parameters block")

    def __validate_config_dict(self, dict, desired_keys, dict_human_readable_name):
        logging.debug("Validating configuration information for %s..." % dict_human_readable_name)
        dict = {k: v for k, v in dict.items() if v}  # Remove keys from the dict with missing values
        dict_keys = dict.keys()
        missing_keys = list(set(desired_keys) - set(dict_keys))
        if len(missing_keys) > 0:
            raise ConfigParserException("There was an error while parsing the configuration file!\n"
                                        "The %s is missing the following parameters: %s" % (dict_human_readable_name,
                                                                                            ", ".join(missing_keys))
                                        )
        # At this point, we can be certain that the dictionary is valid
        logging.debug("Successfully validated configuration information for %s!" % dict_human_readable_name)
        return True

    def __validate_tableau_version(self, tableau_version):
        try:
            tableau_data_float = float(tableau_version)
            if tableau_data_float < defaults.TABLEAU_VERSION_LIMIT_LOWER or \
                    tableau_data_float > defaults.TABLEAU_VERSION_LIMIT_UPPER:
                raise ConfigParserException("Tableau version (%s) is not supported by Tableau DR!"
                                            % tableau_version)
            return tableau_version
        except ValueError:
            raise ConfigParserException("Tableau version (%s) does not seem to be valid!" % tableau_version)
