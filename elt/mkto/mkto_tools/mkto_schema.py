import io
import sys
import json
import yaml
import psycopg2
import psycopg2.extras

from typing import Sequence
from enum import Enum
from collections import OrderedDict, namedtuple
from .mkto_leads import describe_leads
from .mkto_utils import db_open
#from .mkto_activities import describe_activities


class SchemaDiff(Enum):
    UNKNOWN = 0,
    COLUMN_OK = 1,
    COLUMN_MISSING = 2,
    COLUMN_CHANGED = 3,


Column = namedtuple('Column', [
    'table_name',
    'column_name',
    'data_type',
    'is_nullable'
])


class Schema:
    def _key(column: Column):
        return (column.column_name, column)

    def __init__(self, columns: Sequence[Column]=[]):
        self.columns = OrderedDict(map(Schema._key, columns))

    def add_column(self, column: Column):
        self.columns.insert((Schema._key(column), column))

    def column_diff(self, column: Column) -> SchemaDiff:
        if not column.column_name in self.columns:
            return SchemaDiff.COLUMN_MISSING

        db_col = self.columns[column.column_name]
        if column.data_type != db_col.data_type:
            return (SchemaDiff.COLUMN_CHANGED, db_col.data_type)

        return SchemaDiff.COLUMN_OK


'''
:db_conn: psycopg2 db_connection
:schema: database schema
'''
def db_schema(db_conn, schema) -> Schema:
    cursor = db_conn.cursor()

    cursor.execute("""
    SELECT table_name, column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = %s
    ORDER BY ordinal_position;
    """, (schema,))

    columns = map(Column._make, cursor.fetchall())
    return Schema(columns)


def mkto_schema(args) -> Schema:
    source = args.source
    schema = schema_func_map[args.source]()
    fields = schema['result']
    table_name = "mkto_%s" % source
    table_pkey = "%s_pkey" % table_name
    table_def = "table %s" % table_name
    columns = list(filter(None,
                          (column(source, field) for field in fields))
              )

    columns.sort(key=lambda c: c.column_name)
    return Schema(columns)


VARCHAR = "character varying"

data_types_map = {
  "date": "date",
  "string": VARCHAR,
  "phone": VARCHAR,
  "text": VARCHAR,
  "percent": "real",
  "integer": "integer",
  "boolean": "boolean",
  "lead_function": VARCHAR,
  "email": VARCHAR,
  "datetime": "timestamp without time zone",
  "currency": VARCHAR,
  "reference": VARCHAR,
  "url": VARCHAR,
  "float": "real"
}

schema_overrides = {
    'leads': {}
}

schema_func_map = {
    'leads': describe_leads
    #'activities': describe_activities,
}

schema_primary_key = ['id']

def schema_export(args):
    output_file = args.output_file or 'schema.yaml'

    with db_open(database=args.database,
                 host=args.host,
                 port=args.port,
                 user=args.user,
                 password=args.password) as db:
        schema = db_schema(db, 'generated')
        #yaml.dump(schema, sys.stdout)

        schema_mkto = mkto_schema(args)
        #yaml.dump(schema_mkto, sys.stdout)

        schema_cursor = db.cursor()
        for name, col in schema_mkto.columns.items():
            schema_apply_column(schema_cursor, schema, col)

    # table(args.source, schema)
    # yaml.dump(output_schema, io.open(output_file, 'w'))


'''
Apply the schema to the current database connection
adapting tables as it goes. Currently only supports
adding new columns.

:cursor: A database connection
:column: the column to apply
'''
def schema_apply_column(db_cursor, schema: Schema, column: Column) -> SchemaDiff:
    diff = schema.column_diff(column)

    print("[%s]: %s" % (column.column_name, diff))

    if diff == SchemaDiff.COLUMN_CHANGED:
        _, dt_type = schema.column_diff(column)
        raise InapplicableChangeError(diff, dt_type)

    if diff == SchemaDiff.COLUMN_MISSING:
        db_cursor.execute("ALTER TABLE {}.{} ADD COLUMN {} {}",
                          column.table_schema,
                          column.table_name,
                          column.column_name,
                          column.data_type)

    return diff


'''
{
    "id": 2,
    "displayName": "Company Name",
    "dataType": "string",
    "length": 255,
    "rest": {
        "name": "company",
        "readOnly": false
    },
    "soap": {
        "name": "Company",
        "readOnly": false
    }
},
'''
def column(table_name, field) -> Column:
    if not 'rest' in field:
        print("REST field not found for '%s'" % field['id'])
        return None

    rest_field = field['rest']
    column_name = rest_field['name']
    column_def = column_name.lower()
    dt_type = data_type(column_name, field['dataType'])
    is_pkey = column_def in schema_primary_key

    print("%s -> %s as %s" % (column_name, column_def, dt_type))
    column = Column(table_name=table_name,
                    column_name=column_def,
                    data_type=dt_type,
                    is_nullable=not is_pkey)

    return column


def data_type(field_name, src_type):
    return data_types_map[src_type]
