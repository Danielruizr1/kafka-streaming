"""Defines trends calculations for stations"""
import logging
from dataclasses import asdict, dataclass

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

def fortmatStation(station):
    # stationDict = dir(station)
    line = 'red' if station.red else 'blue' if station.blue else 'green'
    formattedStation = TransformedStation( 
        station_id=station.station_id, 
        station_name=station.station_name,
        order= station.order,
        line=line
    )
    return formattedStation


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://", stream_wait_empty=False)
in_topic = app.topic("fromconnect.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1, value_type=TransformedStation)

@app.agent(in_topic)
async def onNewStation(stations):
    stations.add_processor(fortmatStation)
    async for stat in stations:
        print('working')
        await out_topic.send(value=stat)


table = app.Table(
   "station_table",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
)



if __name__ == "__main__":
    app.main()
