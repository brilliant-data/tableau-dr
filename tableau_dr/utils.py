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
import yaml

# Clean string (remove excess spaces)
def clean_str(str_to_clean):
    return " ".join(str_to_clean.split())

# Remove trailing slash from path string
def remove_trailing_slash(str_path):
    while str_path[-1] == "/":
        str_path = str_path[:-1]
    return str_path

# Add trailing slash to path string
def add_trailing_slash(str_path):
    if str_path[-1] != "/":
        str_path = str_path + "/"
    return str_path

# Switch direction of an rsync job
def switch_direction_of_rsync(rsync_cmd_str, source_dir, target_dir):
    logging.debug("Reversing the direction of the following replication command: %s" % rsync_cmd_str)
    split_cmd_str = rsync_cmd_str.split(" ")  # Split string on spaces

    source_item = filter(lambda x: source_dir in x, split_cmd_str)[0]  # Get item containing the source directory
    target_item = filter(lambda x: target_dir in x, split_cmd_str)[0]  # Get item containing the target directory

    #  Switching
    source_pos = split_cmd_str.index(source_item)
    target_pos = split_cmd_str.index(target_item)
    split_cmd_str[source_pos] = add_trailing_slash(target_item)
    split_cmd_str[target_pos] = remove_trailing_slash(source_item)

    switched_rsync_cmd_str = " ".join(split_cmd_str)  # Join items of list to get a string again
    logging.debug("Reversed command is: %s" % switched_rsync_cmd_str)

    return switched_rsync_cmd_str

# Function to parse config file
def parse_config_file(config_file_path):
    with open(config_file_path, 'r') as f:
        try:
            config_data = yaml.load(f)
            return config_data
        except yaml.YAMLError, exc:
            raise Exception("Error in configuration file: %s" % exc)