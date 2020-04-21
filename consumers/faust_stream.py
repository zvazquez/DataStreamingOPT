from utils import prepare_logging
import logging
logger = prepare_logging(__name__, logging.INFO)

import faust


# Faust will ingest records from Kafka in this format
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
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

station_topic = app.topic("com.opt.stations", value_type=Station)
station_topic_lean = app.topic("com.opt.stations.lean", value_type=TransformedStation)
station_table = app.Table("station_mod", default=TransformedStation)

def get_line(station):
    if station.red:
        return "red"
    elif station.blue:
        return "blue"
    elif station.green:
        return "green"
    else:
        return "other"

@app.agent(station_topic)
async def read_stations(stations):
    async for station in stations:
        line = get_line(station)
        station_lean = TransformedStation(
            station_id=station.stop_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )
        await station_topic_lean.send(key=str(station_lean.station_id), value=station_lean)





if __name__ == "__main__":
    app.main()