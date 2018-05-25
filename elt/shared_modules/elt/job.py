import psycopg2
import json

from psycopg2.sql import Identifier, SQL, Placeholder
from enum import Enum
from elt.schema import Schema, Column, DBType
from functools import partial


PG_SCHEMA = 'meltano'
PG_TABLE = 'job_runs'
PRIMARY_KEY = 'id'


def describe_schema() -> Schema:
    def job_column(name, data_type, is_nullable=False):
        return Column(table_name=PG_TABLE,
                      table_schema=PG_SCHEMA,
                      column_name=name,
                      data_type=data_type.value,
                      is_nullable=is_nullable,
                      is_mapping_key=False)

    return Schema(PG_SCHEMA, [
        job_column('elt_uri', DBType.String),
        job_column('state', DBType.String),
        job_column('started_at', DBType.Timestamp, is_nullable=True),
        job_column('ended_at', DBType.Timestamp, is_nullable=True),
        job_column('payload', DBType.JSON, is_nullable=True),
    ], primary_key_name='id')


class State(Enum):
    SUCCESS = (2, ())
    FAIL = (3, ())
    RUNNING = (1, (SUCCESS, FAIL))
    IDLE = (0, (RUNNING, FAIL))

    def transitions(self):
        return self.value[1]

    def __str__(self):
        return self.name


class Job:
    """
    Represents a Job at a certain state (JobState).
    """
    schema_name = 'execution'
    table_name = 'jobs_run'

    @classmethod
    def identifier(self):
        return map(Identifier, (self.schema_name, self.table_name))

    def __init__(self, elt_uri, state=State.IDLE, payload={}):
        self.elt_uri = elt_uri
        self.state = state
        self.payload = payload

    def next(self, state: State) -> (State, State):
        transition = (self.state, state)

        if state not in self.state.transitions:
            raise ImpossibleTransitionError(transition)

        self.state = state
        return transition

    def __dict__(self):
        return {
            'state': str(self.state),
            'elt_uri': str(self.elt_uri),
            'payload': json.dumps(self.payload),
        }

    @classmethod
    def save(self, cursor, job, commit=False):
        job_serial = job.__dict__()
        columns, values = (job_serial.keys(), job_serial.values())

        insert = SQL(("INSERT INTO {}.{} ({}) "
                      "VALUES ({}) "))

        cursor.execute(
            insert.format(
                *self.identifier(),
                SQL(",").join(map(Identifier, columns)),
                SQL(",").join(Placeholder() * len(values)),
            ),
            list(values),
        )

        if commit:
            cursor.commit()

        return True

    @classmethod
    def for_elt(self, cursor, elt_uri):
        fetch = SQL(("SELECT * FROM {}.{} "
                     "WHERE elt_uri = %s "
                     "ORDER BY started_at DESC ")).format(*self.identifier())

        cursor.execute(fetch, (elt_uri,))

        def as_job(row):
            return Job(row['elt_uri'],
                    state=State[row['state']],
                    payload=json.loads(row['payload'])
            )

        return list(map(as_job, cursor.fetchall()))
