#!/bin/bash

cd /var/www/superset
source venv/bin/activate
python -m superset.offline -c 'superset.offline.update_permissions_for_all_databases()'
