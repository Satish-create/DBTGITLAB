#!/usr/bin/python3

import argparse

from mkto_tools.mkto_bulk import bulk_export

if __name__ == '__main__':

    parser=argparse.ArgumentParser(description="Use the Marketo Bulk Export to get Leads or Activities")
    parser.add_argument('-s', dest="source", choices=["activities", "leads"], required=True,
                        help="Specifies either leads or activies records.")
    parser.add_argument('-t', dest="type", choices=["created", "updated"], default="created",
                        help="Specifies either created or updated. Use updated for incremental pulls. Default is created.")
    parser.add_argument('-d', dest="days", type=int, default=3,
                        help="Specify the number of preceding days from the current time to get incremental records for. Only used for lead records. Default is 3 days.")
    parser.add_argument('-b', dest="start",
                        help="The start date in the isoformat of 2018-01-01. This will be formatted properly downstream.")
    parser.add_argument('-e', dest="end",
                        help="The end date in the isoformat of 2018-02-01. This will be formatted properly downstream.")
    parser.add_argument('--nodelete', action='store_true',
                        help="If argument is provided, the CSV file generated will not be deleted.")
    parser.set_defaults(func=bulk_export)
    args=parser.parse_args()
    args.func(args)