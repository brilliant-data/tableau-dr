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
import tableau_dr.utils as utils

class TestUtils(unittest.TestCase):

    # Testing that clean_str actually cleans strings
    def test_clean_str(self):
        string_to_clean = "Lorem   ipsum    dolor sit amet"
        cleaned_str = utils.clean_str(string_to_clean)
        self.assertEqual(cleaned_str, "Lorem ipsum dolor sit amet")

    # Testing that trailing slash removal is working as intended
    def test_remove_trailing_slash(self):
        string_to_clean = "bla/etc/////"
        cleaned_str = utils.remove_trailing_slash(string_to_clean)
        self.assertEqual(cleaned_str, "bla/etc")

    # Validate that adding trailing slash works as intended
    def test_add_trailing_slash(self):
        string_to_clean = "bla/etc"
        cleaned_str = utils.add_trailing_slash(string_to_clean)
        self.assertEqual(cleaned_str, "bla/etc/")

    # Test that rsync direction switching works
    def test_rsync_direction_switch(self):
        rsync_job_to_switch = "* * * * * rsync -a -v --delete /home/brilliant/clusters/prod/tableau_data/data/tabsvc/httpd/htdocs/webdataconnectors/ /home/brilliant/clusters/prod_sync/tableau_data/data/tabsvc/httpd/htdocs/webdataconnectors"
        switched_job = utils.switch_direction_of_rsync(rsync_job_to_switch,
                                                       "/home/brilliant/clusters/prod/tableau_data/data/tabsvc/httpd/htdocs/webdataconnectors/",
                                                       "/home/brilliant/clusters/prod_sync/tableau_data/data/tabsvc/httpd/htdocs/webdataconnectors")
        self.assertEqual(switched_job,
                         "* * * * * rsync -a -v --delete /home/brilliant/clusters/prod_sync/tableau_data/data/tabsvc/httpd/htdocs/webdataconnectors/ /home/brilliant/clusters/prod/tableau_data/data/tabsvc/httpd/htdocs/webdataconnectors")