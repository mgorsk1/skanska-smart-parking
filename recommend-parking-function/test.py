from os import getenv

from azure.storage.table import TableService
from faker import Faker
from time import sleep

chet = Faker()

ts = TableService(account_name=getenv('TABLE_SERVICE_ACCOUNT_NAME'),
                  account_key=getenv('TABLE_SERVICE_ACCOUNT_KEY'))

table_name = 'parking'
# entities = ts.query_entities(table_name, filter="PartitionKey eq 'poland_warsaw' and time_from ge 12 and time_to le 13")
#
# for e in entities:
#     print(e)

# ts.delete_table(table_name)
# ts.create_table(table_name)
# sleep(2)
z = 0
# #
companies = [
    dict(name="Skanska", lat=52.235704, lon=20.978249, price="7.48"),
    dict(name="Microsoft", lat=52.200437, lon=20.936103, price="7.52"),
    dict(name="Sage", lat=52.221016, lon=20.973410, price="7.53"),
    dict(name="Ilmet", lat=52.232243, lon=20.998048 ,price="4.20")]

for i, c in enumerate(companies):
    lat = c.get('lat')
    lon = c.get('lon')

    name = c.get('name')

    for j in range(24):
        data = dict(PartitionKey='poland_warsaw', RowKey=str(z + 1), lat=lat, lon=lon,
                    description=name, id=str(i), time_from=j, time_to=j + 1,
                    free_spots=chet.random.randint(0, 90))
        print(data)

        ts.insert_entity('parking', data)
        z += 1
