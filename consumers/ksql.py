"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = """

CREATE STREAM turnstile_missing_key (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INT
)
  WITH (
      KAFKA_TOPIC='transit.turnstile', VALUE_FORMAT='AVRO'
  );


  CREATE STREAM turnstile_with_key
  WITH(KAFKA_TOPIC='transit.turnstile.withkey', VALUE_FORMAT='AVRO') AS
  SELECT CAST(ROWKEY as VARCHAR) as timesptamp_key, 
  station_id, station_name, line, num_entries
  FROM turnstile_missing_key
  PARTITION BY timesptamp_key;


CREATE TABLE turnstile (
    timesptamp_key VARCHAR,
    station_id INT,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INT
) WITH (
    KAFKA_TOPIC='transit.turnstile.withkey', VALUE_FORMAT='AVRO', KEY='timesptamp_key'
);

CREATE TABLE turnstile_summary
    WITH (VALUE_FORMAT = 'JSON') AS
    SELECT station_id, COUNT(station_id) as count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)

    print('ksql working')


if __name__ == "__main__":
    execute_statement()
