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
import threading
import Queue
import sys

def prepare_remote_server(remote_server, pg_pass, dr_ip, start_afterwards=False):
    logging.info("Preparing Tableau Server on %s..." % remote_server.host)
    logging.info("Stopping Tableau Server on %s..." % remote_server.host)
    remote_server.stop()
    logging.info("Preparing the database of Tableau Server on %s..." % remote_server.host)
    remote_server.prepare_postgres_config()
    remote_server.change_db_pass("tableau", pg_pass)
    remote_server.alter_user_role_replication(pg_user="tableau")
    remote_server.enable_user_replication_connection(pg_user="tableau", ip=dr_ip)
    if start_afterwards:
        logging.info("Starting Tableau Server on %s..." % remote_server.host)
        remote_server.start()


def prepare_dr_unix(env_manager, source_server, target_server):
    logging.info("Preparing Tableau DR Unix's machine...")
    logging.info("Preparing Tableau File Store Repository...")

    if env_manager.tdfs_enabled is True:
        env_manager.create_mount_dirs(source_server, target_server)
        env_manager.create_directory_tree()
        env_manager.install_build_postgres(source_server=source_server)
        env_manager.build_filestore()
        env_manager.run_filestore(is_switchover=False)
    else:
        env_manager.create_mount_dirs(source_server, target_server)
        env_manager.create_directory_tree()
        env_manager.add_initial_rsync_jobs()
        logging.info("Setting up the source Tableau Server's Postgres replica on Rescue Linux...")
        env_manager.install_build_postgres(source_server=source_server)


def validate_tableau_dr(env_manager, source_server, target_server):
    logging.info("Validating environment is in progress...")
    env_manager.validate_os()
    env_manager.validate_user()
    env_manager.validate_rescue_dir()
    env_manager.validate_paths()
    env_manager.validate_mountdir()

    if env_manager.tdfs_enabled:
        env_manager.check_filestore()
    else:
        env_manager.check_rsync()

    env_manager.check_postgres(source_server=source_server,
                               target_server=target_server)
    source_server.validate_exec_policy()
    source_server.validate_tableau_paths()
    source_server.validate_winrm_config()
    if target_server is not None:
        target_server.validate_exec_policy()
        target_server.validate_tableau_paths()
        target_server.validate_winrm_config()

    logging.info("Environment has been successfully validated!")


def prepare_tableau_dr(env_manager, source_server, target_server, dr_ip):
    logging.info("Preparing environment is in progress...")

    # Validate that failover user is running me
    env_manager.validate_os()
    env_manager.validate_user()
    env_manager.validate_rescue_dir()

    # Net share and folder permissions
    logging.info("Ensuring appropriate access control settings on the Windows machine...")
    servers = [source_server, target_server]
    servers = filter(lambda x: x is not None, servers)
    for server in servers:
        server.validate_exec_policy()
        server.validate_tableau_paths()
        server.validate_winrm_config()
        server.net_share_tab_data()
        server.ensure_app_data_permissions()

    prepare_dr_unix(env_manager,
                    source_server,
                    target_server)

    # Preparing source Tableau Server
    prepare_remote_server(source_server,
                          env_manager.pg_password,
                          dr_ip,
                          True)

    if target_server is not None:
        prepare_remote_server(target_server,
                              env_manager.pg_password,
                              dr_ip)

    logging.info("Creating a basebackup for the source Tableau Server and starting it afterwards...")
    env_manager.basebackup_start_source_postgres(source_server=source_server)
    logging.info("Environment has been successfully prepared!")


def uninstall_tableau_dr(env_manager, source_server, target_server):
    logging.info("Uninstalling Tableau DR is in progress...")

    # Validate that failover user is running me
    env_manager.validate_os()
    env_manager.validate_user()
    env_manager.validate_rescue_dir()

    logging.info("Stopping Postgres...")
    env_manager.stop_source_postgres()
    if target_server is not None:
        env_manager.stop_target_postgres()

    logging.info("Removing relevant replication jobs...")
    env_manager.remove_relevant_cron_jobs()

    logging.info("Removing mountpoints...")
    env_manager.remove_mount_dirs()

    logging.info("Removing shares...")
    source_server.delete_net_share()
    if target_server is not None:
        target_server.delete_net_share()

    logging.info("Deleting Tableau DR's directory tree...")
    env_manager.delete_directory_tree()

    logging.info("Deleting temporaly postgres files...")
    env_manager.delete_temp_pg_dir()

    logging.info("Tableau DR has been successfully uninstalled!")
