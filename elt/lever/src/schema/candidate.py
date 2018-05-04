from elt.schema import Schema, Column, DBType


PG_SCHEMA = 'lever'
PG_TABLE = 'candidates'
PRIMARY_KEY = 'id'


def describe_schema(args) -> Schema:
    table_name = args.table_name or PG_TABLE
    table_schema = args.schema or PG_SCHEMA

    # curry the Column object
    def column(column_name, data_type, *,
               is_nullable=True,
               is_mapping_key=False):
        return Column(table_schema=table_schema,
                      table_name=table_name,
                      column_name=column_name,
                      data_type=data_type.value,
                      is_nullable=is_nullable,
                      is_mapping_key=is_mapping_key)

    # Let's start with a minimal candidate schema, with no personal info
    return Schema(table_schema, [
        column("id",                     DBType.String, is_mapping_key=True),
        column("stage_id",               DBType.String),
        column("createdAt",              DBType.Date),
        column("archivedAt",             DBType.Date),
        column("archive_reason_id",      DBType.String),
    ])


def table_name(args):
    return args.table_name or PG_TABLE
