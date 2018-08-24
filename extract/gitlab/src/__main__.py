import os
import argparse
import asyncio
import logging

from enum import Enum
from datetime import datetime
from sqlalchemy import desc
from importer import Importer, schema
from importer.fetcher import Fetcher
from extract.schema import schema_apply
from extract.error import InapplicableChangeError, with_error_exit_code
from extract.cli import parser_db_conn, parser_output, parser_logging
from extract.job import Job, State
from extract.db import DB
from extract.utils import setup_logging, setup_db


GITLAB_ELT_URI = "com.meltano.gitlab:1:*"
GITLAB_CHECK_JOB=bool(os.getenv('GITLAB_CHECK_JOB', False))


def action_export(args):
    fetcher = Fetcher(project=args.project,
                      bucket=args.bucket)

    with DB.default.session() as session:
        last_job = session.query(Job).filter(Job.state != State.FAIL,
                                             Job.elt_uri == GITLAB_ELT_URI) \
                                     .order_by(desc(Job.started_at)) \
                                     .first()
        latest_completed_prefix = last_job and last_job.payload['prefix']

    latest_prefix = fetcher.latest_prefix()
    # latest_prefix = None

    if GITLAB_CHECK_JOB and \
      latest_prefix == latest_completed_prefix:
            logging.info("Export '{}' has already been imported, aborting".format(latest_prefix))
            exit(0)

    job = Job(elt_uri=GITLAB_ELT_URI,
              state=State.RUNNING,
              started_at=datetime.utcnow(),
              payload={'prefix': latest_prefix})

    Job.save(job)
    loop = asyncio.get_event_loop()

    try:
        importer = Importer(args, fetcher=fetcher)
        imported_prefix = loop.run_until_complete(importer.import_all())

        if imported_prefix:
            job.transit(State.SUCCESS)
            job.payload['prefix'] = imported_prefix
        else:
            job.transit(State.FAIL)

    except:
        logging.exception("Import has failed")
        job.transit(State.FAIL)
        raise
    finally:
        job.ended_at = datetime.utcnow()
        Job.save(job)
        loop.close()


def action_schema_apply(args):
    try:
        with DB.default.open() as db:
            target_schema = schema.describe_schema(args)
            schema_apply(db, target_schema)
    except InapplicableChangeError as e:
        for subex in e.exceptions:
            logging.debug(subex)
        logging.error("Schema could not be applied.")
        raise e


class Action(Enum):
    EXPORT = ('export', action_export)
    APPLY_SCHEMA = ('apply_schema', action_schema_apply)

    @classmethod
    def from_str(cls, name):
        print(name)
        return cls[name.upper()]

    def __str__(self):
        return self.value[0]

    def __call__(self, args):
        return self.value[1](args)


def parse():
    parser = argparse.ArgumentParser(
        description="Download GitLab data and send to data warehouse.")

    parser_db_conn(parser)
    parser_output(parser)
    parser_logging(parser)

    parser.add_argument("--project",
                        default=os.getenv("GCP_PROJECT"),
                        help="GCP Project where the bucket is located.")

    parser.add_argument("--bucket",
                        default=os.getenv("GITLAB_BUCKET"),
                        help="GCS bucket name fetch CSV files from.")

    parser.add_argument('action',
                        type=Action.from_str,
                        choices=list(Action),
                        default=Action.EXPORT,
                        help=("export: bulk export data into the output.\n"
                              "apply_schema: export the schema into a schema file."))

    return parser.parse_args()


@with_error_exit_code
def main():
  args = parse()
  setup_db(args)
  setup_logging(args)
  args.action(args)


main()
