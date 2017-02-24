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

import unittest
from tableau_dr.config_parser_class import ConfigParser, ConfigParserException
from tableau_dr.tab_server_connector import TableauServerConnector
import tableau_dr.utils as utils
import os
from copy import deepcopy

# Read example file
test_dir_path = os.path.split(os.path.abspath(__file__))[0]
tableau_dr_path = os.path.split(test_dir_path)[0]
root_path = os.path.split(tableau_dr_path)[0]
config_file_path = os.path.join(root_path,
                                "config",
                                "config.yaml.example")
example_cluster_data = utils.parse_config_file(config_file_path=config_file_path)
example_cluster_data = example_cluster_data.get("prod_to_dr")

# Setup logging
# logging.basicConfig(format='[%(levelname)s] %(asctime)s %(name)s - %(message)s',
#                     level=logging.DEBUG)

class TestConfigParser(unittest.TestCase):

    # Test that config/config.yaml is actually correct (it should be)
    def test_is_config_valid(self):
        try:
            ConfigParser(cluster_data=example_cluster_data)
            self.assert_(True)
        except Exception:
            self.assert_(False)

    # Test that ConfigParser returns TableauServerConnector objects
    def test_server_object_types(self):
        config_object = ConfigParser(cluster_data=example_cluster_data)
        source_server = config_object.get_source_server()
        self.assertIsInstance(source_server, TableauServerConnector)
        target_server = config_object.get_target_server()
        self.assertIsInstance(target_server, TableauServerConnector)

    # Test that reverse switch in the config file actually results in a switch and correct TableauServerConnectors
    # are returned
    def test_reverse_switch(self):
        modified_cluster_data = deepcopy(example_cluster_data)
        modified_cluster_data["reverse"] = True
        reversed_conf_object = ConfigParser(cluster_data=modified_cluster_data)
        self.assertEqual(modified_cluster_data["reverse"],
                         reversed_conf_object.reverse)
        modified_cluster_data["reverse"] = False
        conf_object = ConfigParser(cluster_data=modified_cluster_data)
        self.assertEqual(modified_cluster_data["reverse"],
                         conf_object.reverse)

    # Ensure that the switched TableauServerConnector objects are returned
    def reverse_servers(self):
        modified_cluster_data = deepcopy(example_cluster_data)
        modified_cluster_data["reverse"] = True
        reversed_conf_object = ConfigParser(cluster_data=modified_cluster_data)
        modified_cluster_data["reverse"] = False
        conf_object = ConfigParser(cluster_data=modified_cluster_data)
        self.assertEqual(reversed_conf_object.get_source_server(),
                         conf_object.get_target_server())
        self.assertEqual(reversed_conf_object.get_target_server(),
                         conf_object.get_source_server())

    # Test that exception is raised when domain name is missing when using kerberos protocol for WinRM
    def test_krb_missing_domain_name(self):
        modified_cluster_data = deepcopy(example_cluster_data)
        source_data = modified_cluster_data["servers"]["source"]
        source_data["protocol"] = "kerberos"
        source_data.pop("domain")
        modified_cluster_data["servers"]["source"] = source_data
        with self.assertRaises(ConfigParserException):
            ConfigParser(cluster_data=modified_cluster_data).get_source_server()

    # Test whether it's possible to reverse direction in a single-cluster setting
    def test_reverse_for_single_cluster_setting(self):
        modified_cluster_data = deepcopy(example_cluster_data)
        modified_cluster_data["servers"].pop("target")
        modified_cluster_data["reverse"] = True
        with self.assertRaises(ConfigParserException):
            ConfigParser(cluster_data=modified_cluster_data)
