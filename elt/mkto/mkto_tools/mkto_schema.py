import io
import sys
import json
import yaml
import psycopg2
import psycopg2.extras

from typing import Sequence, Callable
from enum import Enum
from collections import OrderedDict, namedtuple
from .mkto_leads import describe_leads
from .mkto_utils import db_open
#from .mkto_activities import describe_activities


class SchemaException(Exception):
    """Base exception for schema errors."""

class InapplicableChangeException(SchemaException):
    """Raise for inapplicable schema changes."""

class AggregateException(SchemaException):
    """Aggregate multiple sub-exceptions."""
    def __init__(self, exceptions: Sequence[SchemaException]):
        self.exceptions = exceptions


class ExceptionAggregator:
    def __init__(self, errors=Sequence[Exception]):
        self.success = []
        self.failures = []
        self.errors = errors

    def recognize_exception(self, e: Exception) -> bool:
        EType = type(e)
        return EType in self.errors

    def call(self, callable: Callable, *args, **kwargs):
        params = (args, kwargs)
        try:
            callable(*args, **kwargs)
            self.success.append(params)
        except Exception as e:
            if self.recognize_exception(e):
                self.failures.append((e, params))
            else: raise e

    def raise_aggregate(self) -> AggregateException:
        if len(self.failures):
            exceptions = map(lambda f: f[0], self.failures)
            raise AggregateException(exceptions)


class SchemaDiff(Enum):
    UNKNOWN = 0,
    COLUMN_OK = 1,
    COLUMN_MISSING = 2,
    COLUMN_CHANGED = 3,


Column = namedtuple('Column', [
    'table_schema',
    'table_name',
    'column_name',
    'data_type',
    'is_nullable'
])


class Schema:
    def _key(column: Column):
        return ((column.table_name, column.column_name), column)

    def __init__(self, columns: Sequence[Column]=[]):
        self.columns = OrderedDict(map(Schema._key, columns))

    def column_diff(self, column: Column) -> SchemaDiff:
        key, _ = Schema._key(column)

        if not key in self.columns:
            return SchemaDiff.COLUMN_MISSING

        db_col = self.columns[key]
        if column.data_type != db_col.data_type:
            return SchemaDiff.COLUMN_CHANGED

        return SchemaDiff.COLUMN_OK


'''
:db_conn: psycopg2 db_connection
:schema: database schema
'''
def db_schema(db_conn, schema) -> Schema:
    cursor = db_conn.cursor()

    cursor.execute("""
    SELECT table_schema, table_name, column_name, data_type, is_nullable
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
    table_name = args.table_name or "mkto_%s" % source
    print("Table name is: %s" % table_name)

    columns = (column(args.schema, table_name, field) for field in fields)
    columns = list(filter(None, columns))
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

'''
Tries to apply the schema from the Marketo API into
upon the data warehouse.

:args: sys.argv

Returns True when successful.
'''
def schema_export(args):
    success = False

    with db_open(database=args.database,
                 host=args.host,
                 port=args.port,
                 user=args.user,
                 password=args.password) as db:
        schema = db_schema(db, args.schema)
        schema_mkto = mkto_schema(args)

        results = ExceptionAggregator(errors=[InapplicableChangeException])
        schema_cursor = db.cursor()
        for name, col in schema_mkto.columns.items():
            results.call(schema_apply_column, schema_cursor, schema, col)

        print(results.failures) # TODO: improve formating
        results.raise_aggregate()

        # commit if there are no failure
        db.commit()


'''
Apply the schema to the current database connection
adapting tables as it goes. Currently only supports
adding new columns.

:cursor: A database connection
:column: the column to apply
'''
def schema_apply_column(db_cursor, schema: Schema, column: Column) -> SchemaDiff:
    diff = schema.column_diff(column)

    if diff != SchemaDiff.COLUMN_OK:
        print("[%s]: %s" % (column.column_name, diff))

    if diff == SchemaDiff.COLUMN_CHANGED:
        raise InapplicableChangeException(diff)

    if diff == SchemaDiff.COLUMN_MISSING:
        sql = psycopg2.sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN {} %s" % column.data_type
        ).format(
            psycopg2.sql.Identifier(column.table_schema),
            psycopg2.sql.Identifier(column.table_name),
            psycopg2.sql.Identifier(column.column_name),
        )
        db_cursor.execute(sql)

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
def column(table_schema, table_name, field) -> Column:
    if not 'rest' in field:
        print("REST field not found for '%s'" % field['id'])
        return None

    rest_field = field['rest']
    column_name = rest_field['name']
    column_def = column_name.lower()
    dt_type = data_type(column_name, field['dataType'])
    is_pkey = column_def in schema_primary_key

    print("%s -> %s as %s" % (column_name, column_def, dt_type))
    column = Column(table_schema=table_schema,
                    table_name=table_name,
                    column_name=column_def,
                    data_type=dt_type,
                    is_nullable=not is_pkey)

    return column


def data_type(field_name, src_type):
    return data_types_map[src_type]
