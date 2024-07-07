Simple python script that provide opportunity to get difference between 2 yum repositories(unique packages, differed by
version and release).
Underhood it uses createrepo_c lib to parse repodata.
Python 3.11
Redis used for caching, yandex cloud storage for results storaging and distribution

Before usage u have to create 2 files:

1. "~/.aws/credentials" with content:
   [default]
   aws_access_key_id = <your-data>
   aws_secret_access_key = <your-data>
2. "~/.aws/config" with content:
   [default]
   region=<your-region>

To start activate venv by executing 'source .venv/bin/activate' from root folder and execute 'python manage.py
runserver'