
from pymongo import MongoClient
import pprint
client = MongoClient('localhost', 27017)


def stringify_keys(d):
    """Convert a dict's keys to strings if they are not."""
    for key in d.keys():

        # check inner dict
        if isinstance(d[key], dict):
            value = stringify_keys(d[key])
        else:
            value = d[key]

        # convert nonstring to string if needed
        if not isinstance(key, str):
            try:
                d[str(key)] = value
            except Exception:
                try:
                    d[repr(key)] = value
                except Exception:
                    raise

            # delete old key
            del d[key]
    return d


pp = pprint.PrettyPrinter(indent=4)

db = client.quevotan


wnominate_collection = db["new_wnominate"]
votaciones_collection = db['votaciones']
new_votaciones_collection = db['new_votaciones']

wnominates = wnominate_collection.find({})
votaciones = votaciones_collection.find({})

for votacion in votaciones:
    del votacion["_id"]
    wnominate = list(wnominate_collection.find({"id": votacion['id']}))
    if(wnominate):
        if(len(wnominate) >= 2):
            print(wnominate['id'])
        for w in wnominate:
            del w['_id']
        new_votaciones_collection.insert_one(
            {**votacion, "wnominate": [*wnominate]})
    else:
        new_votaciones_collection.insert_one(
            {**votacion, "wnominate": None})
