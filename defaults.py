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

# Variables for validating the configuration file
CONTEXT_KEYS = ["servers",
                "rescue_env"]
SERVERS_BLOCK_KEYS = ["source"]
SERVER_DATA_KEYS = ["host",
                    "user",
                    "password",
                    "tableau"]
TABLEAU_DATA_KEYS = ["install_dir",
                     "version"]
RESCUE_ENV_KEYS = ["rescue_user",
                     "rescue_dir",
                     "postgres"]
RECOVERY_POSTGRES_KEYS = ["password"]

REMOTE_PG_CONNECT_MAX_RETRIES = 5
NET_SHARE_DATA_CMD = "net share tableau_data=\"{programdata_dir}\" /GRANT:{domain}\\{user},FULL"
NET_SHARE_FILES_CMD = "net share tableau_data=\"{programfiles_dir}\" /GRANT:{domain}\\{user},READ"
NET_SHARE_DELETE = "net share tableau_data /delete /Y"
DATAENGINE_DIR = "data/tabsvc/dataengine"
WEBDATACONNECTORS_DIR = "data/tabsvc/httpd/htdocs/webdataconnectors"
CONFIG_DIR = "config"

# Allowed Linux distro data
ALLOWED_LINUX_DIST_NAMES = ['Ubuntu', 'Red Hat Enterprise Linux Server', 'CentOS Linux']

# Kinit cmd
KINIT_CMD = "echo \"{pwd}\" | kinit -p {user}@{domain}"

# CLuster mountpoint dirs
CLUSTER_SERVER_DIR = 'tableau_data'

# Postgres data
PG_ABSOLUTE_DIR = "/usr/local/tableau_dr_pgsql"
PG_DATA = "{pg_dir}/data".format(pg_dir=PG_ABSOLUTE_DIR)
PG_PORT = 5432
PG_USER = "tblwgadmin"
PG_DATABASE = "workgroup"

# Postgres commands
PGPASS_FILE_CONTENT = "{host}:{port}:{database}:{user}:{pwd}"
WORKGROUP_PG_DUMP_FILE = "workgroup.pg_dump"
CMD_AS_PG_USER = "sudo LD_LIBRARY_PATH={pg_dir}/lib -i -H -u postgresql bash -c '{cmd}'"
CMD_ROOT_AS_PG_USER = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:{pg_dir}/lib"
MANAGE_PG_COMMAND = "{pg_dir}/bin/pg_ctl {operation} -D {pg_data_dir} -l {pg_dir}/logs/postgresql_{pg_data_dir_short}.log"
PG_BASEBACKUP_CMD = "{pg_dir}/bin/pg_basebackup -h {host} -p {pg_port} -D {pg_data_dir} -U {pg_user} --no-password"
BACKUP_SQL_FILE = 'backup.sql'
PG_DUMP_COMMAND = "{pg_dir}/bin/pg_dump -h localhost -U {user} -d {database} -F {dump_format} -Z 0 -c -C"
PG_DUMPALL_COMMAND = "{pg_dir}/bin/pg_dumpall -h localhost -U {user} --roles-only"
# Postgres commands on windows
START_PG_PS = "start-Process -FilePath '{tab_install_dir}\\{tab_version}\\pgsql\\bin\\pg_ctl.exe' " \
              "-ArgumentList 'start -D \"{tableau_app_data}/data/tabsvc/pgsql/data\" " \
              "-o \"-p 8060\" -W' -PassThru"
CHECK_PG_STATUS = "start-Process -FilePath '{tab_install_dir}\\{tab_version}\\pgsql\\bin\\pg_ctl.exe' " \
                  "-ArgumentList 'status -D \"{tableau_app_data}/data/tabsvc/pgsql/data\"' " \
                  "-PassThru -Wait"
REMOTE_PSQL_COMMAND = "start-Process -FilePath '{tab_install_dir}\\{tab_version}\\pgsql\\bin\\psql.exe' " \
                     "-ArgumentList '-d {pg_database} -U {user} -p 8060' " \
                     "-NoNewWindow -PassThru -Wait"

# Contents for Postgres config files
RECOVERY_CONF_CONTENT = """standby_mode = 'on'
primary_conninfo = 'host={host} port={port} user={user} password={password}'
trigger_file = '/this/should/never/exist'
"""

POSTGRESQL_CONF_CONTENT = """hot_standby = on
max_connections = 258
max_locks_per_transaction = 128
log_line_prefix ='%t '
"""
# Directories involved in cron replication
REPLICATION_DIRS = ["data/tabsvc/httpd/htdocs/webdataconnectors",
                    "data/tabsvc/dataengine"]

SYNC_ONLY_REPLICATION_DIRS = ["config"]

#RSYNC_TEMPLATE = "sh -c '/usr/bin/flock -w 1 {rescue_dir}/cron.lock{uuid} rsync -a -v --delete {source_path} {destination_path} | pv -l -s $(rsync -a -v --delete {source_path} {destination_path} --dry-run | wc -l) > /dev/null'"
RSYNC_TEMPLATE = "/usr/bin/flock -w 1 {rescue_dir}/cron.lock{uuid} rsync -a -v --delete {source_path} {destination_path}"

MOUNT_CIFS_CMD_DATA = "sudo mount.cifs -v //{server_host}/tableau_data {mount_abs_path} -o credentials={cred_file_path},uid={rescue_user},gid={rescue_user},{rights}"
MOUNT_CIFS_CMD_FILES = "sudo mount.cifs -v //{server_host}/tableau_files {mount_abs_path} -o credentials={cred_file_path},uid={rescue_user},gid={rescue_user},{rights}"
UMOUNT_CMD = "sudo umount {mount_abs_path}"

# TODO: Handle different Tableau versions!
PG_BUILD_PROCEDURE = [
    "./configure --prefix={prefix} --disable-rpath --enable-thread-safety --enable-integer-datetimes --enable-nls --with-ldap --with-openssl --with-libxml --with-libxslt --with-tcl --with-perl --with-python --enable-float8-byval",
     "make",
     "sudo make install"
]

# TODO: Validate user creation steps! Are the steps below correct?
ADD_PG_USER_CMDS = [
    "sudo useradd -d {pg_abs_path} -g {rescue_user_gid} -m postgresql",
    "sudo usermod -a -G {rescue_user_gid} postgresql",
    "sudo chown -R postgresql:{rescue_user_gid} {pg_abs_path}"
]

LOCALEDEF_CMD = "sudo localedef -f CP1252 -i /usr/share/i18n/locales/en_US English_United\\ States.1252"

JAVA_CHECK_CMD = "which java"
JAVA_INSTALL_CMDS = ["sudo apt-add-repository ppa:openjdk-r/ppa -y",
                     "sudo apt-get update",
                     "sudo apt-get install openjdk-8-jre -y"]

FILESTORE_BIN_CMD = "ps auxw | grep -v grep | grep com.tableausoftware.tdfs.filestore.app.Main || java -Dconnections.properties=file://{conn_prop} -Dconfig.properties=file://{filestore_prop} -Dlog4j.configuration=file://{log4j_xml} -cp \"{bin_path}/*:{bin_path}/repo-jars/*:{bin_path}/repo-migrate-jars:{lib_path}/*\"  com.tableausoftware.tdfs.filestore.app.Main"
FILESTORE_IS_SYNC_DRY_CMD = "rsync -vnrc {src_dataengine_dir}/extract {tgt_dataengine_dir}/extract"
FILESTORE_IS_SYNC_CMD = "rsync -vrc {src_dataengine_dir}/extract {tgt_dataengine_dir}/extract"
SUPPORTED_WINRM_PROTOCOLS = ["kerberos", "ntlm", "basic"]

REMOTE_ACL_COMMAND = "$Acl = Get-Acl \"{path}\"\n" \
                     "$Ar = New-Object  system.security.accesscontrol.filesystemaccessrule(\"{domain}\\{user}\",\"FullControl\",\"ContainerInherit,ObjectInherit\", \"None\", \"Allow\")\n" \
                     "$Acl.SetAccessRule($Ar)\n" \
                     "Set-Acl \"{path}\" $Acl"

TABLEAU_VERSION_LIMIT_LOWER = 10.0
TABLEAU_VERSION_LIMIT_UPPER = 10.2

TEST_PATH_PS_CMD = "$PathExists = Test-Path \"{path}\"\n" \
                   "If ($FileExists -eq $False) {{ exit 1 }} Else {{ exit 0 }}"

PG_SOURCE_URL = "https://ftp.postgresql.org/pub/source/v9.5.3/postgresql-9.5.3.tar.bz2"
PG_VERSION = "9.5.3"

SMB_CREDENTIALS_CONTENT = """username={user}
password={password}
domain={domain}
"""

REPLACE_FILE_CONTENT_CMD = "(Get-Content \"{file_name}\") | ForEach-Object " \
                           "{{ $_ -creplace \"{replace_from}\", \"{replace_to}\"}} | Set-Content \"{file_name}\""

MIN_ALLOWED_WINRM_SHELL_MEMORY = 4096
