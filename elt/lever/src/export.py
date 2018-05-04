import os
import requests
import json
import functools
import csv
import asyncio

from tempfile import NamedTemporaryFile
from datetime import datetime
from requests.auth import HTTPBasicAuth
from elt.cli import DateWindow
from elt.utils import compose, db_open
from elt.schema import DBType, Schema, ExceptionAggregator, AggregateException
from elt.process import write_to_db_from_csv, upsert_to_db_from_csv
import schema.candidate as candidate

# Lever API details: https://hire.lever.co/developer/documentation
USER = os.getenv("LEVER_API_KEY")
TOKEN = ''
ENDPOINT = "https://api.lever.co/v1/"
PAGE_SIZE = 100


def get_auth():
    # https://hire.lever.co/developer/documentation#authentication
    # provide API key as the basic auth username (leave the password blank)
    return HTTPBasicAuth(USER, TOKEN)


def extract(args):
    window = DateWindow(args, formatter=datetime.timestamp)
    exporter = export_file(args, *window.formatted_range())
    importer = import_file(args, exporter)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(importer)
    loop.close()


@asyncio.coroutine
async def import_file(args, exporter):
    try:
        for csv_file in exporter:
            with db_open(**vars(args)) as db:
                upsert_to_db_from_csv(db, csv_file,
                                      primary_key=candidate.PRIMARY_KEY,
                                      table_name=candidate.table_name(args),
                                      table_schema=args.schema)
    except GeneratorExit:
        print("Import finished.")


def export_file(args, start_time, end_time):
    envelope = None

    def transform_candidates_response(envelope):
        # Flatten and transform the response we get from the Lever API
        #   to a representation useful for storing it to the datawarehouse
        # The Lever API expands some of the results we want in included sub-structures
        # This method extracts what is needed, renames the attributes
        #   converts the timestamp format provided by Lever to a proper datetime and
        #   returns a flat result for all data entries
        flat_envelope = {
            "hasNext": envelope['hasNext'],
            "data": []
        }

        if envelope['hasNext']:
            # Next attribute is only there when a result has next page
            flat_envelope['next'] = envelope['next']

        for entry in envelope['data']:
            flat_entry = {
                "id": entry['id'],
                "stage_id": entry['stage']['id'],
                "stage_text": entry['stage']['text'],
                "created_at": datetime.fromtimestamp(entry['createdAt'] / 1000).isoformat(),
            }

            if entry['archived'] is not None:
                flat_entry["archived_at"] = \
                  datetime.fromtimestamp(entry['archived']['archivedAt'] / 1000).isoformat()
                flat_entry["archive_reason_id"] = entry['archived']['reason']['id']
                flat_entry["archive_reason_text"] = entry['archived']['reason']['text']
            else:
                flat_entry["archived_at"] = None
                flat_entry["archive_reason_id"] = None
                flat_entry["archive_reason_text"] = None

            flat_envelope['data'].append(flat_entry)

        return flat_envelope


    def get_candidates(envelope):
        # Pagination for Lever's API:
        #   https://hire.lever.co/developer/documentation#pagination
        # Candidates API:
        #   https://hire.lever.co/developer/documentation#candidates
        candidates_url = "{}/candidates".format(ENDPOINT)

        # Only include the required fields
        candidates_included_fields = ['id', 'stage', 'createdAt', 'archived']
        candidates_expanded_fields = ['archived', 'stage']

        payload = {
            "limit": PAGE_SIZE,
            'include': candidates_included_fields,
            "expand": candidates_expanded_fields,
            "created_at_start": start_time * 1000,
            "created_at_end": end_time * 1000
        }

        if envelope is not None and envelope['hasNext']:
            # Add an offset in case we are not at the first page
            payload['offset'] = envelope['next']

        return requests.get(candidates_url,
                            params=payload,
                            auth=get_auth())

    def finished(envelope):
        if envelope is None: return False

        return envelope['hasNext'] == False

    while not finished(envelope):
        api_response = get_candidates(envelope).json()

        # ToDo: Do some Error Code checking that everything is OK
        #        and that this is a valid response with data inside it

        envelope = transform_candidates_response(api_response)

        with NamedTemporaryFile(mode="w", delete=not args.nodelete) as f:
            f.write(json.dumps(envelope))
            print("Wrote response at {}".format(f.name))

        try:
            schema = candidate.describe_schema(args)
            yield flatten_csv(args, schema, envelope['data'])
        except AggregateException as e:
            [print(ex) for ex in e.exceptions]


def flatten_csv(args, schema, entries):
    """
    Flatten a list of objects according to the specfified schema.

    Returns the output filename
    """
    table_name = candidate.table_name(args)
    output_file = args.output_file or "candidates-{}.csv".format(datetime.utcnow().timestamp())
    flatten_entry = functools.partial(flatten, schema, table_name)

    header = entries[0]
    with open(output_file, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header.keys())
        writer.writeheader()
        writer.writerows(map(flatten_entry, entries))

    return output_file


def flatten(schema: Schema, table_name, entry):
    flat = {}

    results = ExceptionAggregator(errors=[KeyError])

    for k, v in entry.items():
        column_key = (table_name, k)
        db_type = results.call(schema.columns.__getitem__, column_key).data_type
        flat[k] = flatten_value(db_type, v)
        # print("{} -[{}]-> {}".format(v, db_type, flat[k]))

    results.raise_aggregate()

    return flat


def flatten_value(db_type: DBType, value):
    null = lambda x: x if x is not None else 'null'
    # X -> 'wx(q|w)'
    around = lambda w, x, q=None: ''.join((str(w), str(x), str(q or w)))
    quote = functools.partial(around, "'")
    array = compose(functools.partial(around, "{", q="}"),
                    ",".join,
                    functools.partial(map, str))

    strategies = {
        DBType.JSON: json.dumps,
        # [x0, ..., xN] -> '{x0, ..., xN}'
        DBType.ArrayOfInteger: array,
        DBType.ArrayOfLong: array,
        # [x0, ..., xN] -> '{'x0', ..., 'xN'}'
        DBType.ArrayOfString: compose(array,
                                      functools.partial(map, quote)),
    }

    strategy = strategies.get(db_type, null)

    return compose(null, strategy)(value)
