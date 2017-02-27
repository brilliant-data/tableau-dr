```
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
``` 

# Features

## Backup Anytime

A user of Tableau DR is able to create a Tableau backup (.tsbak) file on demand without impacting the Tableau Server cluster’s performance. This is achieved by synchronizing the required Tableau Server data to a local disk of the machine running Tableau DR.

Usage:

`python tableau_dr.py backup --rescue_group={NAME_OF_BLOCK_IN_CONFIG_YAML} --config_file={CONFIG_YAML_FILE_WITH_PATH}`
 
on the machine that runs Tableau DR. The backup file will be available in the backups subdirectory in Tableau DR’s home directory as provided in the configuration file.

## Disaster Recovery

Utilizing a secondary Tableau Server cluster, Tableau DR keeps it up-to-date as a warm standby, allowing a system administrator to easily switch roles between the live and the standby cluster.

Usage:

`python tableau_dr.py switchover --rescue_group={NAME_OF_BLOCK_IN_CONFIG_YAML} --config_file={CONFIG_YAML_FILE_WITH_PATH}` 

# Detailed Technical Information

## Backup Anytime

The necessary data for a backup consists of the following:
- Contents of the workgroup database in the Tableau repository
- Roles in the Tableau repository
- Extracts
- Customizations

The .tsbak file format Tableau uses for backup is a zip archive compressed with the deflate method. Tableau DR fetches the required data and then compresses it.


## Disaster Recovery
This feature at the core keeps a secondary Tableau Server cluster up-to-date with a live Tableau Server cluster. Tableau DR Disaster Recovery also allows the user to switch between the live and the standby cluster.

Tableau DR Disaster Recovery only manages the flow of backup data. In order to direct Tableau users to another cluster, further steps might be necessary. 
If users access Tableau Server by hostname, DNS needs to be reconfigured.
If users connect to a virtual IP address managed by a load balancer, it needs to be repointed in the load balancer configuration.

Tableau DR fetches all necessary data and in a separate step saves it to the the standby cluster. There is no direct link established between the two clusters.

Configuration is not synchronized as it contains host and network specific details valid only in the local context.

# Infrastructure Requirements
Requirements for Tableau DR’s Linux machine are the following:

Hardware
One virtual machine with the following parameters:
* 8 cores
* 16 GB RAM
* 30 GB SSD system volume
* 1 TB SSD data volume

Software
* Fedora-based Linux distribution
* SSH access to the machine

Network
* Internet access

# Setup Instructions
## Windows
Open the necessary communication ports
* TCP port access to the Tableau repository (by default 8060)
* TCP port access to SMB (TCP ports 137, 139, 445 and UDP ports 137, 138)
* TCP port access to WinRM (by default 5985)
    * An example for opening WinRM port (5985) would be (from Powershell)  
    `netsh advfirewall firewall add rule name="WinRM 5985 in" protocol=TCP dir=in profile=any localport=5985 remoteip={REMOTE-IP} localip=any action=allow`
* Set execution policy to unrestricted
    * Execute using an elevated Powershell shell: `Set-ExecutionPolicy Unrestricted`  
    You can check the current setting by running: `Get-ExecutionPolicy`
* Enable WinRM access
    * Winrm service start (example: `net start winrm`)
    * Configuration (from Powershell):  
`winrm set winrm/config/service '@{AllowUnencrypted="true"}'`  
`winrm set winrm/config/client '@{TrustedHosts="TABLEAU-DR-LINUX-IP"}’`  
`winrm set winrm/config/winrs ‘@{MaxMemoryPerShellMB="4096"}’`  

    * If cluster is not in AD, enable basic auth by executing:  
`winrm set winrm/config/client/auth '@{Basic="true"}'`  
`winrm set winrm/config/service/auth '@{Basic="true"}'`  
* Run Tableau Server using dedicated runas user
* Grant full control for runas user  
    * Grant full control on Tableau Application Directory  
(default: C:\ProgramData\Tableau) with all files and directories in the folder to Tableau’s runas User.

## Linux
* Configure Kerberos(in case of need) so that Windows machines can be accessed through the Kerberos protocol and mount points can be established

* On Redhat, make sure you can use EPEL:  
`wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && sudo rpm -Uvh epel-release-latest-7*.rpm`

* Linux packages needs to be installed  
`sudo yum install -y perl-ExtUtils-Embed samba-common openssl-devel openldap-devel bzip2 git python-devel krb5-workstation realmd sssd tcl-devel perl-ExtUtils-MakeMaker gcc krb5-devel cifs-utils libxslt-devel zlib-devel readline-devel libxml2-devel libuuid p7zip p7zip-plugins wget`

* PIP needs to be installed  
`wget https://bootstrap.pypa.io/get-pip.py && sudo python get-pip.py`  

* Retrieve the git repository of the Tableau DR

* Install python realted dependencies  
`pip install -r /path/to/the/tableau_dr/git/repository/requirements.txt`  

* A sudoer user or root is needed to run Tableau DR. Please run the software as this user and provide the user’s name in the configuration file (more detail in the next section).  
An example user “brilliant” can be added by executing the following lines:  
`sudo useradd brilliant && sudo passwd brilliant`  
Make sure that the user is not prompted for password during sudo operations by adding the following line using visudo:   
`brilliant        ALL=(ALL)       NOPASSWD: ALL`

* Tableau DR needs a directory for storing replication data, database information, and other configuration data. Due to the potentially large size of the data, this directory should be created on the separate data volume to prevent filling up the system drive. Make sure that the user running Tableau DR is the owner of this directory.  
An example directory (/opt/tableau_dr) can be created by executing the following lines (assuming that the user running the software is “brilliant”):  
`sudo mkdir /opt/tableau_dr && sudo chown brilliant:brilliant /opt/tableau_dr`

# Configuration
Tableau DR obtains the connection data to the Windows servers running Tableau Servers and other parameters relevant for the Linux environment from a YAML configuration file. Please follow the guidelines of formatting YAML documents to write this file (http://ess.khhq.net/wiki/YAML_Tutorial). This file can be located anywhere on Linux as long as it’s readable by Tableau DR. You need to provide its location to Tableau DR using the `--config_file` command-line parameter. Below is an example configuration file with detailed comments on each block.

`prod_to_dr:` # Name of the config block. Provide it to PR using the --rescue_group CLI parameter  
  `servers:` # Block for information on Windows Servers  
    `source:` # Block for data on the source or production Tableau Server  
      `domain:` *recovery.brilliant-data.com* # AD domain name  
      `host:` *demo1-master* # Hostname  
      `user:` *tableau* # Tableau runas user’s name  
      `password:` *PASSWORD1* # Tableau runas user’s password  
      `tableau:` # Details on Tableau installed on this Windows server  
        `install_dir:` *C:/Program Files/Tableau/Tableau Server*  
        `app_data_dir:` *C:/ProgramData/Tableau/Tableau Server*  
        `version:` *10.0*  
    `target:` # Block for data on the target or disaster recovery Tableau Server. Do not include it if running in a single cluster setting  
      `domain:` *recovery.brilliant-data.com*  
      `host:` *demo2-master*  
      `user:` *tableau*  
      `password:` *PASSWORD2*  
      `tableau:`  
        `install_dir:` *C:/Program Files/Tableau/Tableau Server*  
        `app_data_dir:` *C:/ProgramData/Tableau/Tableau Server*  
        `version:` *10.0*  
  `recovery_env:` # Block for information on the Tableau DR Linux environment  
    `ip:` *10.0.1.34* # The Tableau DR machine’s IP address. Optional.  
    `rescue_user:` *brilliant* # Username of sudoer user or root running Tableau DR  
    `is_sudoer:` *true* # A true/false value in order to indicate whether the failover_user is a sudoer or not.  
    `rescue_dir:` */opt/tableau_dr* # Absolute path to the directory for storing data managed by Tableau DR. The user running Tableau DR needs to have a write access to this folder.  
    `postgres:` # Block for details on Postgres replicas  
      `absolute_dir:` */usr/local/pgsql* # Absolute directory for Postgres. Optional, default value is /usr/local/pgsql  
      `port:` *5432* # Port for Postgres to run on. Optional, default value is 5432.   
      `password:` *PASSWORD3* # Password for the Postgres replication using PG user “tableau”  
