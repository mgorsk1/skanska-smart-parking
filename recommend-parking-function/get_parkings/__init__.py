import logging
from json import dumps
from os import getenv

import azure.functions as func
import googlemaps as gm
from azure.storage.table import TableService
from jsonschema import validate


def main(req: func.HttpRequest) -> func.HttpResponse:
    TABLE_NAME = getenv('table_name', 'parking')

    SCHEMA = {
        "type": "object",
        "properties": {
            "lat": {"type": "number"},
            "lon": {"type": "number"},
            "country": {"type": "string"},
            "city": {"type": "string"},
            "radius": {"type": "number"},
            "time_from": {"type": "number"},
            "time_from": {"type": "number"},
        },
        "required": ["lat", "lon", "country", "city", "time_from", "time_to"]
    }

    DEFAULT_RADIUS = 1000  # in meters
    TOP_RESULTS = 3

    def calculate_distance(geo_from: dict, geo_to: dict) -> float:  # in meters
        from math import sin, cos, sqrt, atan2, radians

        # approximate radius of earth in km
        R = 6373.0

        lat1 = radians(geo_from.get('lat'))
        lon1 = radians(geo_from.get('lon'))

        lat2 = radians(geo_to.get('lat'))
        lon2 = radians(geo_to.get('lon'))

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = 1000 * R * c

        return round(distance, 2)

    logging.info('Python HTTP trigger function processed a request.')

    # REQUIRED ARGS ----------------------------------------------------------------------------------------------------

    req_body = req.get_json()

    validate(instance=req_body, schema=SCHEMA)

    logging.info(f"Succesfully validated request {req_body}")

    from_lon = float(req_body.get('lon'))
    from_lat = float(req_body.get('lat'))

    time_from = int(req_body.get('time_from'))
    time_to = int(req_body.get('time_to'))

    partition_key = req_body.get('country') + '_' + req_body.get('city')

    request_body_distance = float(req_body.get('radius', DEFAULT_RADIUS))

    top_results = req_body.get('top_results', TOP_RESULTS)

    # ------------------------------------------------------------------------------------------------------------------

    result = list()

    ts = TableService(account_name=getenv('TABLE_SERVICE_ACCOUNT_NAME'),
                      account_key=getenv('TABLE_SERVICE_ACCOUNT_KEY'))

    if not ts.exists(TABLE_NAME):
        ts.create_table(TABLE_NAME)
        logging.info(f"Table {TABLE_NAME} created.")
    else:
        logging.info(f"Table {TABLE_NAME} already exists.")

    try:
        filter = f"PartitionKey eq '{partition_key}' and free_spots ne 0 and time_from ge {time_from} and time_to le {time_to}"
        entities = ts.query_entities(TABLE_NAME, filter=filter)
        logging.info(f"Results obtained {list(entities)}")
    except Exception:
        logging.error("Error obtaining entities", exc_info=True)

    for e in entities:
        logging.info(f"Entity received {e}")

        to_lon = e.lon
        to_lat = e.lat

        distance = calculate_distance(dict(lat=from_lat, lon=from_lon), dict(lat=to_lat, lon=to_lon))

        if distance <= request_body_distance:
            id = e.id
            description = e.description

            g = gm.Client(getenv('GOOGLE_DIRECTIONS_API'))

            d = g.directions(f'{to_lat}, {to_lon}', f'{from_lat}, {from_lon}', mode="transit")

            if len(d) > 0:
                directions = d[0]

                l = directions['legs']

                if len(l) > 0:
                    legs = l[0]

                    duration = legs.get('duration').get('text')
                    steps = legs.get('steps')

                    result.append(dict(id=id, description=description, duration=duration, steps=steps, distance=distance))
        else:
            pass

    result = sorted(result, key=lambda k: k['distance'])

    if len(result) > top_results:
        body = dumps(result[:min(top_results, len(result))])
    else:
        body = dumps(result)

    return func.HttpResponse(body=body, status_code=200)


