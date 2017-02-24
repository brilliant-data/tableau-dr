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

from docopt import docopt
import logging
import yaml
from validate_prepare_env import validate_tableau_dr, prepare_tableau_dr, uninstall_tableau_dr
from execute_switchover import execute_switchover, execute_switchover_test
from tableau_dr.env_manager import EnvironmentManager
from tableau_dr.config_parser_class import ConfigParser
import os
import tableau_dr.utils as utils

LOG_FORMAT = '[%(levelname)s] %(asctime)s - %(message)s'


# Get config data from file
def get_config_data(config_file_path, cluster_name, reverse=False, tdfs_enabled=False):
    config_data = utils.parse_config_file(config_file_path)
    logging.debug("Successfully parsed configuration data!")
    cluster_data = config_data.get(cluster_name)
    if cluster_data is None:
        raise Exception("The provided cluster name (%s) is not included in the configuration file!" % cluster_name)
    if reverse:
        cluster_data["reverse"] = True
    if tdfs_enabled:
        cluster_data["rescue_env"]["tdfs"] = True
        cluster_data["rescue_env"]["tdfs"] = True

    return cluster_data


def initialize_logger(config_file_path, cluster_name):
    rescue_dir_path = ''
    with open(config_file_path, 'r') as stream:
        try:
            rescue_dir_path = yaml.load(stream).get(cluster_name).get('rescue_env').get('rescue_dir')
        except Exception:
            raise Exception('Malformed config file or wrong cluster name ({cluster_name}) detected '.format(
                cluster_name=cluster_name
            ))
            # end try
    # end with

    #output_dir = os.path.join(rescue_dir_path, 'logs')
    output_dir = os.path.join(os.path.expanduser("~"),
                              "tableau_dr",
                              "logs")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO if not os.environ.get('tableau_dr_DEBUG') else logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create debug file handler and set level to debug
    handler = logging.FileHandler(os.path.join(output_dir, "tableau_dr.log"),
                                  "a")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Surpress logs
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)  # Surpress logs from urrlib3
    logging.getLogger("requests").setLevel(logging.CRITICAL)  # Surpress logs from requests
    logging.getLogger("requests_kerberos").setLevel(logging.CRITICAL)  # Surpress logs from requests_kerberos


if __name__ == '__main__':

    doc = """tableau_dr.py - Commandline tool to execute Tableau DR disaster recovery, prepare/validate the environment, and create backup files.

    Usage:
        tableau_dr.py (-h | --help)
        tableau_dr.py validate --rescue_group=<rescue_group> --config_file=<config_file> [--tdfs]
        tableau_dr.py switchover --rescue_group=<rescue_group> --config_file=<config_file> [--tdfs]
        tableau_dr.py backup --rescue_group=<rescue_group> --config_file=<config_file> [--tdfs]
        tableau_dr.py uninstall --rescue_group=<rescue_group> --config_file=<config_file>
        tableau_dr.py prepare --rescue_group=<rescue_group> --config_file=<config_file> [--tdfs]


    Options:
        -h, --help                                          Show this screen.
        -q, --quiet                                         Do not show output, only errors.
        --rescue_group=<rescue_group>                       REQUIRED: Name of the rescue group.
        --config_file=<config_file>                         REQUIRED: Absolute path to the configuration file.
        --tsbak_url=<tsbak_url>                             REQUIRED: URL to the tsbak file to execute tests with.
        --tdfs                                              Use TDFS based file replication (experimental)
    """

    #--reverse                                           Indicates whether to reverse switchover direction (DR->Prod)
    #        tableau_dr.py tests --rescue_group=<rescue_group> --config_file=<config_file> --tsbak_url=<tsbak_url> [--tdfs]

    args = docopt(doc, help=True, version=None)

    # Parse the configuration file
    cluster_name = args.get("--rescue_group")
    config_file_path = args.get("--config_file")
    reverse = True if args.get("--reverse") else False
    tdfs_enabled = True if args.get("--tdfs") else False

    initialize_logger(config_file_path=config_file_path,
                      cluster_name=cluster_name)

    cluster_data = get_config_data(config_file_path=config_file_path,
                                   cluster_name=cluster_name,
                                   reverse=reverse,
                                   tdfs_enabled=tdfs_enabled)

    logging.info("Tableau DR is starting...")
    logging.debug("Received the following arguments:")
    [logging.debug("%s:%s" % (k, v)) for k, v in args.iteritems()]

    # Obtain configuration data
    config_object = ConfigParser(cluster_data=cluster_data)
    source_server = config_object.get_source_server()
    target_server = config_object.get_target_server()
    rescue_user, is_sudoer, cluster_a_root_dir, cluster_b_root_dir, cluster_sync_root_dir, mount_dir, backups_dir, tdfs_enabled, \
        filestore_app_dir, filestore_temp_mount_dir, tab_data_config_dir, dataengine_dir = config_object.recovery_data()
    pg_absolute_dir, pg_port, pg_user, pg_password, pg_database, pg_data_root_dir, pg_data_cluster_a_dir, pg_data_cluster_b_dir =\
        config_object.postgres_data()
    dr_ip = config_object.obtain_ip()

    # Obtain Environment Manager object
    env_manager = EnvironmentManager(rescue_user=rescue_user,
                                     is_sudoer = is_sudoer,
                                     pg_data_root_dir = pg_data_root_dir,
                                     cluster_source_root_dir=cluster_a_root_dir,
                                     sync_root_dir=cluster_sync_root_dir,
                                     cluster_target_root_dir=cluster_b_root_dir,
                                     tab_data_config_dir=tab_data_config_dir,
                                     mount_dir=mount_dir,
                                     pg_absolute_dir=pg_absolute_dir,
                                     pg_database=pg_database,
                                     pg_user=pg_user,
                                     pg_port=pg_port,
                                     pg_password=pg_password,
                                     cluster_source_pg_data_dir=pg_data_cluster_a_dir,
                                     cluster_target_pg_data_dir=pg_data_cluster_b_dir,
                                     backups_dir=backups_dir,
                                     dr_unix_ip=dr_ip,
                                     tdfs_enabled=tdfs_enabled,
                                     filestore_app_dir=filestore_app_dir,
                                     filestore_temp_mount_dir=filestore_temp_mount_dir,
                                     dataengine_dir=dataengine_dir,
                                     is_reverse=config_object.reverse)

    # Prepare the environment
    if args.get("prepare"):
        prepare_tableau_dr(env_manager=env_manager,
                               source_server=source_server,
                               target_server=target_server,
                               dr_ip=dr_ip)

    # Validate the environment
    elif args.get("validate"):
        validate_tableau_dr(env_manager=env_manager,
                                source_server=source_server,
                                target_server=target_server)

    # Execute switchover
    elif args.get("switchover"):
        validate_tableau_dr(env_manager=env_manager,
                                source_server=source_server,
                                target_server=target_server)
        execute_switchover(env_manager=env_manager,
                           source_server=source_server,
                           target_server=target_server)

    # Create backup
    elif args.get("backup"):
        validate_tableau_dr(env_manager=env_manager,
                                source_server=source_server,
                                target_server=target_server)
        env_manager.create_backup()

    # Uninstall
    elif args.get("uninstall"):
        uninstall_tableau_dr(env_manager=env_manager,
                                 source_server=source_server,
                                 target_server=target_server)

    # Execute switchover tests
    elif args.get("tests"):
        tsbak_url = args.get("--tsbak_url")
        validate_tableau_dr(env_manager=env_manager,
                                source_server=source_server,
                                target_server=target_server)
        execute_switchover_test(env_manager=env_manager,
                                source_server=source_server,
                                target_server=target_server,
                                tsbak_url=tsbak_url)
