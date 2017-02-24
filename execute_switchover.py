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
import shutil
import os

def execute_switchover(env_manager, source_server, target_server):
    if target_server is None:
        raise Exception("Switchover is not possible in a single cluster setting!")

    if env_manager.tdfs_enabled:
        env_manager.disable_filestore()
        env_manager.stop_filestore()
    else:
        logging.info("Disabling Tableau File Store Repository sync on rescue Unix...")
        env_manager.disable_rsync()
        logging.info("Tableau File Store Repository sync has been successfully disabled!")

    # Source server
    #logging.info("Stopping Tableau Server on the source machine...")
    #source_server.stop()
    #logging.info("Tableau Server has been successfully stopped on the source server (%s)" % source_server.host)

    logging.info("Executing Tableau Postgres Repository dump...")
    pgdump_files_destination_dir = env_manager.cluster_target_mount_full_path
    env_manager.execute_source_pgdump(destination_dir=pgdump_files_destination_dir)
    logging.info("Executing Tableau Postgres Repository dump has been successful!")

    logging.info("Turning off source machine's Postgres replica on Rescue Unix...")
    env_manager.stop_source_postgres()
    logging.info("Source machine's Postgres replica has been successfully turned off!")

    # Target server
    logging.info("Stopping Tableau Server on the target machine...")
    # Making sure that the target server is in fact stopped
    target_server.stop()
    logging.info("Restoring Tableau Postgres Repository on the target machine...")
    target_server.restore_postgres()
    logging.info("Reindexing the target machine's Tableau Server...")
    target_server.reindex()
    logging.info("Starting Tableau Server on the target machine...")
    target_server.start()
    logging.info("Tableau Server has been successfully started on the target server (%s)" % target_server.host)

    ###Server role changes and reverted replication temporaly removed due bugs.
    # #  Setup Postgres and sync for the new prod Tableau server
    # logging.info("Setting up the target server's Postgres replica on Rescue Unix...")
    # env_manager.postgres_basebackup(tab_host=target_server.host,
    #                                 pg_data_dir=env_manager.cluster_target_pg_data_dir)
    # env_manager.start_target_postgres()
    # logging.info("Postgres replica for the target server has been successfully started!")
    #
    # if env_manager.tdfs_enabled:
    #     env_manager.configure_filestore(is_switchover=True)
    #     env_manager.run_filestore(is_switchover=True)
    # else:
    #     env_manager.create_mount_dirs(target_server, source_server)
    #     logging.info("Reversing the direction of Tableau File Store Repository sync on rescue Unix...")
    #     env_manager.reverse_rsync_direction()
    #     logging.info("The direction of Tableau File Store Repository sync has been successfully reversed!")

    logging.info("Switchover has been finished successfully.")


def execute_switchover_test(env_manager, source_server, target_server, tsbak_url):
    if target_server is None:
        raise Exception("Switchover is not possible in a single cluster setting!")

    logging.info("Executing switchover test is in progress...")

    # Making sure that source is stopped
    logging.info("Stopping Tableau Server on the source machine...")
    source_server.stop()
    logging.info("Tableau Server has been successfully stopped on the source server (%s)" % source_server.host)

    # Downloading tsbak on windows
    tsbak_remote_path = "C:\\Users\\Public\\Downloads\\{tsbak_filename}" \
        .format(tsbak_filename="test.tsbak")
    source_server.download_file(source_url=tsbak_url,
                                destination_path=tsbak_remote_path)

    # Running restore
    logging.debug("Executing tabadmin restore...")
    source_server.restore(tsbak_path=tsbak_remote_path)
    # TODO: Ensure that replication connection is still present after restore!

    logging.info("Disabling Tableau File Store Repository sync on rescue Unix...")
    env_manager.disable_rsync()
    logging.info("Tableau File Store Repository sync has been successfully disabled!")

    # Source server
    logging.info("Executing Tableau Postgres Repository dump...")
    pgdump_files_destination_dir = env_manager.cluster_target_mount_full_path
    env_manager.execute_source_pgdump(destination_dir=pgdump_files_destination_dir)
    logging.info("Executing Tableau Postgres Repository dump has been successful!")

    logging.info("Turning off source machine's Postgres replica on Rescue Unix...")
    env_manager.stop_source_postgres()
    logging.info("Source machine's Postgres replica has been successfully turned off!")

    # Target server
    logging.info("Stopping Tableau Server on the target machine...")
    # Making sure that the target server is in fact stopped
    target_server.stop()
    logging.info("Restoring Tableau Postgres Repository on the target machine...")
    target_server.restore_postgres()
    logging.info("Reindexing the target machine's Tableau Server...")
    target_server.reindex()
    logging.info("Starting Tableau Server on the target machine...")
    target_server.start()
    logging.info("Tableau Server has been successfully started on the target server (%s)" % target_server.host)

    # # Setup Postgres and sync for the new prod Tableau server
    # logging.info("Setting up the target server's Postgres replica on Rescue Unix...")
    # env_manager.postgres_basebackup(tab_host=target_server.host,
    #                                 pg_data_dir=env_manager.cluster_target_pg_data_dir)
    # env_manager.start_target_postgres()
    # logging.info("Postgres replica for the target server has been successfully started!")
    #
    # if env_manager.tdfs_enabled:
    #     env_manager.configure_filestore(is_switchover=True)
    #     env_manager.run_filestore(is_switchover=True)
    # else:
    #     env_manager.create_mount_dirs(target_server, source_server)
    #     logging.info("Reversing the direction of Tableau File Store Repository sync on rescue Unix...")
    #     env_manager.reverse_rsync_direction()
    #     logging.info("The direction of Tableau File Store Repository sync has been successfully reversed!")

    logging.info("Switchover has been finished successfully.")
