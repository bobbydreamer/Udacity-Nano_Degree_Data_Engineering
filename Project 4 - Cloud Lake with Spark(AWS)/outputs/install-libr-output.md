```
./install-libr.sh
Using username "hadoop".
Authenticating with public key "imported-openssh-key"
Last login: Sun Nov  3 14:37:48 2019

       __|  __|_  )
       _|  (     /   Amazon Linux AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-ami/2018.03-release-notes/
29 package(s) needed for security, out of 48 available
Run "sudo yum update" to apply all updates.

EEEEEEEEEEEEEEEEEEEE MMMMMMMM           MMMMMMMM RRRRRRRRRRRRRRR
E::::::::::::::::::E M:::::::M         M:::::::M R::::::::::::::R
EE:::::EEEEEEEEE:::E M::::::::M       M::::::::M R:::::RRRRRR:::::R
  E::::E       EEEEE M:::::::::M     M:::::::::M RR::::R      R::::R
  E::::E             M::::::M:::M   M:::M::::::M   R:::R      R::::R
  E:::::EEEEEEEEEE   M:::::M M:::M M:::M M:::::M   R:::RRRRRR:::::R
  E::::::::::::::E   M:::::M  M:::M:::M  M:::::M   R:::::::::::RR
  E:::::EEEEEEEEEE   M:::::M   M:::::M   M:::::M   R:::RRRRRR::::R
  E::::E             M:::::M    M:::M    M:::::M   R:::R      R::::R
  E::::E       EEEEE M:::::M     MMM     M:::::M   R:::R      R::::R
EE:::::EEEEEEEE::::E M:::::M             M:::::M   R:::R      R::::R
E::::::::::::::::::E M:::::M             M:::::M RR::::R      R::::R
EEEEEEEEEEEEEEEEEEEE MMMMMMM             MMMMMMM RRRRRRR      RRRRRR

[hadoop@ip-172-31-44-57 ~]$ pip freeze
aws-cfn-bootstrap==1.4
awscli==1.15.83
Babel==0.9.4
backports.ssl-match-hostname==3.4.0.2
beautifulsoup4==4.6.3
boto==2.48.0
botocore==1.10.82
chardet==2.0.1
cloud-init==0.7.6
colorama==0.2.5
configobj==4.7.2
docutils==0.11
ecdsa==0.11
futures==3.0.3
hibagent==1.0.0
iniparse==0.3.1
Jinja2==2.7.2
jmespath==0.9.2
jsonpatch==1.2
jsonpointer==1.0
kitchen==1.1.1
lockfile==0.8
lxml==4.2.5
MarkupSafe==0.11
mysqlclient==1.3.14
nltk==3.4
nose==1.3.4
numpy==1.14.5
paramiko==1.15.1
PIL==1.1.6
ply==3.4
pyasn1==0.1.7
pycrypto==2.6.1
pycurl==7.19.0
pygpgme==0.3
pyliblzma==0.5.3
pystache==0.5.3
python-daemon==1.5.2
python-dateutil==2.1
python27-sagemaker-pyspark==1.2.1
pyxattr==0.5.0
PyYAML==3.11
requests==1.2.3
rsa==3.4.1
simplejson==3.6.5
singledispatch==3.4.0.3
six==1.8.0
urlgrabber==3.10
urllib3==1.8.2
virtualenv==15.1.0
windmill==1.6
yum-metadata-parser==1.1.4
You are using pip version 9.0.3, however version 19.3.1 is available.
You should consider upgrading via the 'pip install --upgrade pip' command.
[hadoop@ip-172-31-44-57 ~]$ ls -l
total 0
[hadoop@ip-172-31-44-57 ~]$ aws s3 sync s3://sushanth-dend-datalake-files .
download: s3://sushanth-dend-datalake-files/etl.py to ./etl.py
download: s3://sushanth-dend-datalake-files/etl-emr-hdfs.py to ./etl-emr-hdfs.py
download: s3://sushanth-dend-datalake-files/install-libr.sh to ./install-libr.sh
download: s3://sushanth-dend-datalake-files/merged_log_data.json to ./merged_log_data.json
download: s3://sushanth-dend-datalake-files/merged_song_data.json to ./merged_song_data.json
[hadoop@ip-172-31-44-57 ~]$ ls -ls
total 7900
  20 -rw-rw-r-- 1 hadoop hadoop   17590 Nov  3 14:47 etl-emr-hdfs.py
  16 -rw-rw-r-- 1 hadoop hadoop   16232 Nov  3 14:47 etl.py
   4 -rw-rw-r-- 1 hadoop hadoop     227 Nov  3 14:47 install-libr.sh
3924 -rw-rw-r-- 1 hadoop hadoop 4015187 Nov  3 14:47 merged_log_data.json
3936 -rw-rw-r-- 1 hadoop hadoop 4028468 Nov  3 14:47 merged_song_data.json
[hadoop@ip-172-31-44-57 ~]$ cat install-libr.sh
#!/bin/bash
set -x -e

#Installing required libraries
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then

sudo pip install pyspark
sudo pip install boto3
sudo pip install configparser
sudo pip install pandas

fi[hadoop@ip-172-31-44-57 ~]$ chmod 755 install-libr.sh
[hadoop@ip-172-31-44-57 ~]$ ./install-libr.sh
+ grep isMaster /mnt/var/lib/info/instance.json
+ grep true
  "isMaster": true
+ sudo pip install pyspark
Collecting pyspark
  Downloading https://files.pythonhosted.org/packages/87/21/f05c186f4ddb01d15d0ddc36ef4b7e3cedbeb6412274a41f26b55a650ee5/pyspark-2.4.4.tar.gz (215.7MB)
    100% |████████████████████████████████| 215.7MB 5.5kB/s
Collecting py4j==0.10.7 (from pyspark)
  Downloading https://files.pythonhosted.org/packages/e3/53/c737818eb9a7dc32a7cd4f1396e787bd94200c3997c72c1dbe028587bd76/py4j-0.10.7-py2.py3-none-any.whl (197kB)
    100% |████████████████████████████████| 204kB 5.8MB/s
Installing collected packages: py4j, pyspark
  Running setup.py install for pyspark ... done
Successfully installed py4j-0.10.7 pyspark-2.4.4
You are using pip version 9.0.3, however version 19.3.1 is available.
You should consider upgrading via the 'pip install --upgrade pip' command.
+ sudo pip install boto3
Collecting boto3
  Downloading https://files.pythonhosted.org/packages/ea/be/ce6320d0341936efd97990f62b33aa2717513c12b16a8a2ed4301214e65a/boto3-1.10.8-py2.py3-none-any.whl (128kB)
    100% |████████████████████████████████| 133kB 4.5MB/s
Collecting s3transfer<0.3.0,>=0.2.0 (from boto3)
  Downloading https://files.pythonhosted.org/packages/16/8a/1fc3dba0c4923c2a76e1ff0d52b305c44606da63f718d14d3231e21c51b0/s3transfer-0.2.1-py2.py3-none-any.whl (70kB)
    100% |████████████████████████████████| 71kB 10.7MB/s
Collecting botocore<1.14.0,>=1.13.8 (from boto3)
  Downloading https://files.pythonhosted.org/packages/34/f7/fce50110ae94feed24b5b2796cb2bd376345ceb480b9d4090e10be75d959/botocore-1.13.8-py2.py3-none-any.whl (5.3MB)
    100% |████████████████████████████████| 5.3MB 229kB/s
Requirement already satisfied: jmespath<1.0.0,>=0.7.1 in /usr/lib/python2.7/dist-packages (from boto3)
Requirement already satisfied: futures<4.0.0,>=2.2.0; python_version == "2.6" or python_version == "2.7" in /usr/lib/python2.7/dist-packages (from s3transfer<0.3.0,>=0.2.0->boto3)
Requirement already satisfied: docutils<0.16,>=0.10 in /usr/lib/python2.7/dist-packages (from botocore<1.14.0,>=1.13.8->boto3)
Collecting urllib3<1.26,>=1.20; python_version == "2.7" (from botocore<1.14.0,>=1.13.8->boto3)
  Downloading https://files.pythonhosted.org/packages/e0/da/55f51ea951e1b7c63a579c09dd7db825bb730ec1fe9c0180fc77bfb31448/urllib3-1.25.6-py2.py3-none-any.whl (125kB)
    100% |████████████████████████████████| 133kB 9.4MB/s
Requirement already satisfied: python-dateutil<3.0.0,>=2.1; python_version >= "2.7" in /usr/lib/python2.7/dist-packages (from botocore<1.14.0,>=1.13.8->boto3)
Requirement already satisfied: six in /usr/lib/python2.7/dist-packages (from python-dateutil<3.0.0,>=2.1; python_version >= "2.7"->botocore<1.14.0,>=1.13.8->boto3)
Installing collected packages: urllib3, botocore, s3transfer, boto3
  Found existing installation: urllib3 1.8.2
    Uninstalling urllib3-1.8.2:
      Successfully uninstalled urllib3-1.8.2
  Found existing installation: botocore 1.10.82
    Uninstalling botocore-1.10.82:
      Successfully uninstalled botocore-1.10.82
Successfully installed boto3-1.10.8 botocore-1.13.8 s3transfer-0.2.1 urllib3-1.25.6
You are using pip version 9.0.3, however version 19.3.1 is available.
You should consider upgrading via the 'pip install --upgrade pip' command.
+ sudo pip install configparser
Collecting configparser
  Downloading https://files.pythonhosted.org/packages/7a/2a/95ed0501cf5d8709490b1d3a3f9b5cf340da6c433f896bbe9ce08dbe6785/configparser-4.0.2-py2.py3-none-any.whl
Installing collected packages: configparser
Successfully installed configparser-4.0.2
You are using pip version 9.0.3, however version 19.3.1 is available.
You should consider upgrading via the 'pip install --upgrade pip' command.
+ sudo pip install pandas
Collecting pandas
  Downloading https://files.pythonhosted.org/packages/db/83/7d4008ffc2988066ff37f6a0bb6d7b60822367dcb36ba5e39aa7801fda54/pandas-0.24.2-cp27-cp27mu-manylinux1_x86_64.whl (10.1MB)
    100% |████████████████████████████████| 10.1MB 119kB/s
Collecting python-dateutil>=2.5.0 (from pandas)
  Downloading https://files.pythonhosted.org/packages/d4/70/d60450c3dd48ef87586924207ae8907090de0b306af2bce5d134d78615cb/python_dateutil-2.8.1-py2.py3-none-any.whl (227kB)
    100% |████████████████████████████████| 235kB 5.5MB/s
Collecting pytz>=2011k (from pandas)
  Downloading https://files.pythonhosted.org/packages/e7/f9/f0b53f88060247251bf481fa6ea62cd0d25bf1b11a87888e53ce5b7c8ad2/pytz-2019.3-py2.py3-none-any.whl (509kB)
    100% |████████████████████████████████| 512kB 2.7MB/s
Requirement already satisfied: numpy>=1.12.0 in /usr/local/lib64/python2.7/site-packages (from pandas)
Requirement already satisfied: six>=1.5 in /usr/lib/python2.7/dist-packages (from python-dateutil>=2.5.0->pandas)
Installing collected packages: python-dateutil, pytz, pandas
  Found existing installation: python-dateutil 2.1
    Uninstalling python-dateutil-2.1:
      Successfully uninstalled python-dateutil-2.1
Successfully installed pandas-0.24.2 python-dateutil-2.8.1 pytz-2019.3
You are using pip version 9.0.3, however version 19.3.1 is available.
You should consider upgrading via the 'pip install --upgrade pip' command.
[hadoop@ip-172-31-44-57 ~]$ ./install-libr.sh
```