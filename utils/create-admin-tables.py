import os
import sys

import mysql.connector
import re
import json
import sys
import yaml
import argparse

import pdb

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../../")
from doug.modules.utils.exceptions import *
from doug.modules.authentication.auth import Credentials

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--cfg-file',  default=os.path.dirname(os.path.realpath(__file__)) + "/../cfg/cfg.yml")
# parser.add_argument('-d', '--db-file',   default=os.path.dirname(os.path.realpath(__file__)) + '/../data/database/db-tables.yml')
parser.add_argument('-t', '--table-list', default='all')
parser.add_argument('-C', '--include-creds', action='store_true')
parser.add_argument('-D', '--drop-tables',action='store_true')
parser.add_argument('-d', '--debug',     required=False, action='store_true')
args = parser.parse_args()

if args.debug:
  pdb.set_trace()

with open(args.cfg_file, 'r') as f:
  cfg = yaml.safe_load(f)

with open(cfg['common']['base_data_dir'] + '/database/db-tables.yml') as f:
  table_defs = yaml.safe_load(f)

try:
  creds = Credentials(cfg['authentication'])
  db_creds = creds.read('database')
except MissingDeviceCredentials as e:
  db_creds = dict()
  db_creds['username'] = input("Enter database username: ")
  db_creds['password'] = input("Enter password: ")

mydb = mysql.connector.connect( 
  host      = cfg['database']['host'],
  user      = db_creds['username'],
  password  = db_creds['password'],
  port      = cfg['database']['port']
)

mycursor = mydb.cursor( dictionary=False )
mycursor.execute("SET foreign_key_checks = 0;")
mycursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(cfg['workflow']['database_name']))
mycursor.execute("USE {};".format(cfg['workflow']['database_name']))

if args.table_list == 'all':
  tables = set(list(table_defs['doug_workflow'].keys()))
else:
  tables = set(args.table_list.split(','))

for table in tables:
  print("table={}".format(table))
  if table in ['credentials'] and not args.include_creds:
    continue
  if args.drop_tables:
    try:
      mycursor.execute( "DROP TABLE IF EXISTS {}".format(table) )
    except:
      print(table_defs['doug_workflow'][table] + " failed to delete")
  try:
    mycursor.execute(table_defs['doug_workflow'][table].format(db_name='doug_workflow'))
  except:
    print(table_defs['doug_workflow'][table] + " failed to add")
mycursor.execute("SET foreign_key_checks = 1;")
