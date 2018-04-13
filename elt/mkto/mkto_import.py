#!/usr/bin/python3
import os
import sys
import argparse

from mkto_tools.mkto_leads import write_to_db_from_csv, upsert_to_db_from_csv
from mkto_tools.mkto_utils import db_open
from config import parser_db_conn


def import_csv(args):
    with db_open(host=args.host,
                 port=args.port,
                 database=args.database,
                 password=args.password,
                 user=args.user) as db:
        write_to_db_from_csv(db, args.input_file)


def upsert_csv(args):
    with db_open(host=args.host,
                 port=args.port,
                 database=args.database,
                 password=args.password,
                 user=args.user) as db:
        upsert_to_db_from_csv(db, args.input_file, 'id')


args_func_map = {
    'create': import_csv,
    'update': upsert_csv,
}


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description="Import a CSV file into the dataware house.")

    parser_db_conn(parser)

    parser.add_argument('action', choices=['create', 'update'], default='import',
                        help="""
create: import data in bulk from a CSV file.
update: create/update data in bulk from a CSV file.
""")

    parser.add_argument('-t', '--table', dest='table_name',
                        help="Table to import the data to.")


    parser.add_argument('-s', dest="source", choices=["activities", "leads"], required=True,
                        help="Specifies either leads or activies records.")


    parser.add_argument('input_file',
                        help="Specifies the file to import.")

    args=parser.parse_args()

    if not args.user or not args.password:
        print("User/Password are required.")
        sys.exit(2)

    args_func_map[args.action](args)
