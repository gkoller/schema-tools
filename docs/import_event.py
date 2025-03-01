import json

from sqlalchemy import create_engine

from schematools.events import EventsProcessor
from schematools.utils import dataset_schema_from_url

SCHEMA_URL = "http://schemas.data.amsterdam.nl/datasets/"

# Hier dus de DSN voor de Azure postgreSQL
DATABASE_URL = "postgresql://dataservices:insecure@localhost:5416/dataservices"

# Event hier als een string, met | als scheidingsteken. Dat komt normaal binnen via Kafka
kafka_event_data = (
    '{9EB35544-5CBB-4F2E-A659-F05203CC2736}.1|{"event_type": "ADD", "event_id'
    '": 102259, "source_id": "{9EB35544-5CBB-4F2E-A659-F05203CC2736}.1", "las'
    't_event": null, "catalog": "gebieden", "collection": "bouwblokken", "sou'
    'rce": "AMSBI"}|{"entity": {"_id": "03630012096976", "code": "AA02", "_ha'
    'sh": "c39aaf7924a274741b179a75030ae916", "_version": "0.1", "geometrie":'
    ' "POLYGON ((119836.994 489477.607, 119833.955 489476.746, 119833.154 489'
    "480.545, 119850.550 489485.145, 119846.877 489498.753, 119842.844 489497"
    ".656, 119835.385 489522.960, 119837.408 489562.391, 119742.464 489565.39"
    "6, 119742.222 489560.145, 119739.857 489560.228, 119739.708 489555.715, "
    "119739.518 489555.939, 119739.617 489558.816, 119738.482 489558.856, 119"
    "738.600 489562.223, 119741.008 489562.135, 119741.080 489564.654, 119738"
    ".642 489564.720, 119738.756 489568.184, 119719.117 489568.830, 119719.06"
    "9 489568.631, 119710.365 489566.114, 119710.245 489566.250, 119693.956 4"
    "89555.246, 119695.980 489552.284, 119693.960 489550.958, 119695.288 4895"
    "48.946, 119697.316 489550.285, 119700.025 489546.291, 119694.647 489542."
    "595, 119691.899 489546.687, 119685.171 489554.377, 119672.878 489548.005"
    ", 119664.930 489542.653, 119663.666 489541.887, 119652.879 489536.136, 1"
    "19657.343 489530.693, 119658.494 489529.509, 119659.484 489528.635, 1196"
    "84.915 489509.632, 119682.911 489506.924, 119679.776 489502.379, 119717."
    "110 489473.123, 119721.701 489469.682, 119731.342 489462.818, 119748.709"
    " 489449.670, 119788.893 489418.520, 119795.947 489428.113, 119796.348 48"
    "9428.289, 119827.524 489404.641, 119834.441 489405.944, 119835.817 48939"
    "5.211, 119845.942 489387.569, 119881.908 489360.874, 119885.954 489362.4"
    "81, 119884.811 489365.012, 119883.478 489368.467, 119882.623 489371.109,"
    " 119881.681 489374.690, 119879.665 489376.184, 119875.549 489391.688, 11"
    "9870.826 489408.936, 119868.654 489409.057, 119867.422 489413.488, 11986"
    "9.264 489414.623, 119865.903 489426.788, 119864.771 489427.377, 119863.4"
    "73 489433.040, 119860.515 489432.540, 119859.909 489436.384, 119859.989 "
    "489436.397, 119859.978 489436.745, 119858.887 489437.524, 119857.776 489"
    "445.174, 119853.507 489448.332, 119855.330 489450.715, 119854.824 489454"
    ".280, 119852.046 489455.965, 119851.906 489456.904, 119855.095 489461.83"
    "7, 119851.044 489476.270, 119852.359 489478.674, 119851.556 489481.731, "
    "119839.230 489478.240, 119840.772 489470.446, 119838.260 489470.089, 119"
    '837.698 489474.049, 119836.994 489477.607))", "_source_id": "{9EB35544-5'
    'CBB-4F2E-A659-F05203CC2736}.1", "volgnummer": 1, "identificatie": "03630'
    '012096976", "ligt_in_buurt": {"bronwaarde": "geometrie"}, "eind_geldighe'
    'id": null, "_expiration_date": null, "begin_geldigheid": "2006-06-12", "'
    'registratiedatum": "2006-06-12T00:00:00.000000"}, "_source_id": "{9EB355'
    '44-5CBB-4F2E-A659-F05203CC2736}.1", "_entity_source_id": "{9EB35544-5CBB'
    '-4F2E-A659-F05203CC2736}.1"}\n'
)


def main():
    dataset_schema = dataset_schema_from_url(SCHEMA_URL, "gebieden")
    srid = dataset_schema["crs"].split(":")[-1]
    engine = create_engine(DATABASE_URL)

    source_id, event_meta_str, data_str = kafka_event_data.split("|", maxsplit=2)
    event_meta = json.loads(event_meta_str)
    event_data = json.loads(data_str)

    with engine.begin() as connection:
        importer = EventsProcessor([dataset_schema], srid, connection, truncate=True)
        importer.process_event(
            source_id,
            event_meta,
            event_data,
            event_meta["catalog"] == "rel",
        )


if __name__ == "__main__":
    main()
