import mysql.connector
import re
import sys
import yaml
import argparse
import os
import pdb
import base64

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../../")
from doug.modules.utils.exceptions import *
from doug.modules.authentication.auth import Credentials

##########################################################################
# CONSTANTS
##########################################################################
get_fks_sql = """
  SELECT DISTINCT
    KEY_COLUMN_USAGE.CONSTRAINT_NAME        AS 'name',
    KEY_COLUMN_USAGE.REFERENCED_TABLE_NAME  AS 'parent_table',
    KEY_COLUMN_USAGE.REFERENCED_COLUMN_NAME AS 'parent_table_col',
    KEY_COLUMN_USAGE.TABLE_NAME             AS 'child_table',
    KEY_COLUMN_USAGE.COLUMN_NAME            AS 'child_table_col'

  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
  INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ON TABLE_CONSTRAINTS.CONSTRAINT_NAME=KEY_COLUMN_USAGE.CONSTRAINT_NAME 
  WHERE
    TABLE_CONSTRAINTS.CONSTRAINT_TYPE="FOREIGN KEY" AND
    TABLE_CONSTRAINTS.CONSTRAINT_SCHEMA='{database_name}' AND
    FIND_IN_SET(KEY_COLUMN_USAGE.TABLE_NAME, '{doug_table_list}' ) > 0
  ;
"""

desribe_table_sql = """
  SELECT
    COLUMN_NAME,
    IS_NULLABLE
  FROM information_schema.columns
  WHERE
    table_schema = '{database_name}' AND
    table_name   = '{table_name}'
"""
##########################################################################
# FUNCTIONS
##########################################################################
def set_insert_order(level, tables, table_name, insert_order):
  if len(insert_order) == level:
    insert_order.append( set() )
  insert_order[level].add(table_name)
  for child in tables[table_name]['children']:
    set_insert_order(level+1, tables, child['child_table'], insert_order)
  return

def get_fk_data(database_name, db_connection, doug_table_list):

  db_cursor = db_connection.cursor(dictionary=True)
  db_cursor.execute( get_fks_sql.format( database_name=database_name, doug_table_list=doug_table_list) )
  results = db_cursor.fetchall()

  tables = dict()
  for table in results:
    if not table['parent_table'] in tables:
      tables.update(
        {
          table['parent_table']: {
            'name': table['parent_table'],
            'parents': [],
            'children': [],
            'cols': dict(),
            'post-process': []
          }
        }
      )
      db_cursor.execute(
        desribe_table_sql.format(
          database_name=database_name,
          table_name=table['parent_table']
        )
      )
      tmp = db_cursor.fetchall()
      for col in tmp:
        tables[table['parent_table']]['cols'].update(
          {
            col['COLUMN_NAME']: {
              'is_nullable': col['IS_NULLABLE']
            }
          }
        )

    if not table['child_table'] in tables:
      tables.update(
        {
          table['child_table']: {
            'name': table['child_table'],
            'parents': [],
            'children': [],
            'cols': dict(),
            'post-process': []
          }
        }
      )
      db_cursor.execute(
        desribe_table_sql.format(
          database_name=database_name,
          table_name=table['child_table']
        )
      )
      tmp = db_cursor.fetchall()
      for col in tmp:
        tables[table['child_table']]['cols'].update(
          {
            col['COLUMN_NAME']: {
              'is_nullable': col['IS_NULLABLE']
            }
          }
        )

    constraint = {
      'name':             table['name'],
      'parent_table':     table['parent_table'],
      'parent_table_col': table['parent_table_col'],
      'child_table':      table['child_table'],
      'child_table_col':  table['child_table_col']
    }

    if tables[table['child_table']]['cols'][table['child_table_col']]['is_nullable'] == 'NO':
      tables[table['parent_table']]['children'].append(constraint)
      tables[table['child_table']]['parents'].append(constraint)
    else:
      tables[table['parent_table']]['post-process'].append(constraint)
      tables[table['child_table']]['post-process'].append(constraint)

  return tables

##########################################################################
# MAIN
##########################################################################

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--cfg-file',  default=os.path.dirname(os.path.realpath(__file__)) + "/../cfg/cfg.yml")
# parser.add_argument('-d', '--db-file',    default=os.path.dirname(os.path.realpath(__file__)) + '/../../api/data/admin/resource-types.yml')
parser.add_argument('-t', '--table-list', required=False, default='all')
parser.add_argument('-D', '--db-list',    required=False, default='all')
parser.add_argument(      '--drop-tables',action='store_true')
# parser.add_argument(      '--debug',     required=False, action='store_true')
parser.add_argument('-i',  '--update-insert-order', action='store_true')
parser.add_argument('-C', '--create-dbs', action='store_true')
parser.add_argument('-d', '--debug',     required=False, action='store_true')
args = parser.parse_args()

if args.debug:
  pdb.set_trace()

with open(args.cfg_file, 'r') as f:
  cfg = yaml.safe_load(f)

with open(cfg['common']['base_data_dir'] + '/inventory/resource-types.yml') as f:
  table_defs = yaml.safe_load(f)

if args.db_list == 'all':
  databases = set(list(table_defs.keys()))
else:
  databases = set(args.db_list.split(','))

all_databases = databases

creds = Credentials(cfg['authentication'])
db_creds = creds.read('database')

mydb = mysql.connector.connect( 
  host      = cfg['database']['host'],
  user      = db_creds['username'],
  password  = db_creds['password'],
  port      = cfg['database']['port']
)

insert_order_by_db = dict()
for database in all_databases:
  print(database)
  mycursor = mydb.cursor( dictionary=False )
  mycursor.execute("SET foreign_key_checks = 0;")
  if args.create_dbs:
    mycursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database))
  try:
    mycursor.execute("USE {};".format(database))
  except mysql.connector.errors.ProgrammingError as e:
    if re.search('Unknown database', str(e)):
      print("The '{}' database does not exist, try the --create-dbs option".format(database))
    sys.exit(2)
  
  if args.table_list == 'all':
    tables = set(list(table_defs[database].keys()))
  else:
    tables = set(args.table_list.split(','))

  for table in tables:
    print("{:>30}".format( table), end=' ')
    if args.drop_tables:
      try:
        mycursor.execute("DROP TABLE {};".format(table))
        print("{:>10}".format('DROPPED'), end=' ')
      except mysql.connector.errors.ProgrammingError as e:
        print("{:>10}".format('SKIPPED'), end=' ')

    mycursor.execute(table_defs[database][table]['table_create'])
    print("ADDED")


  if args.update_insert_order:
    insert_order = {
      'insert_order': list()
    }
    fk_data = get_fk_data(database, mydb, ','.join(list(table_defs[database].keys())))
    if len(fk_data) > 0:
      set_insert_order(0, fk_data, 'cluster', insert_order['insert_order'])
      tmp_list = list()
      for level in insert_order['insert_order']:
        tmp_list.append(list(level))
      insert_order['insert_order'] = tmp_list
        # level += 1

      insert_order_by_db.update(
        {
          database: insert_order
        }
      )

      insert_order_file = cfg['common']['base_data_dir'] + '/database/' + cfg['database']['insert_order']
      with open(insert_order_file, mode='w') as f:
        yaml.dump(insert_order_by_db, f)

