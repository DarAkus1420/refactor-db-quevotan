
from pymongo import MongoClient
import pprint
client = MongoClient('localhost', 27017)

pp = pprint.PrettyPrinter(indent=4)

db = client.quevotan


wnominate_collection = db["wnominate"]
votaciones_collection = db['votaciones']

wnominates = wnominate_collection.find({})
votaciones = votaciones_collection.find_one({})


counter = 0


def diputado_fix(diputado, votacion):
    obj = {**diputado, "participacion": votacion[str(diputado["ID"])]}
    return obj


nominate2 = []

for a in wnominates:

    diputados = [*map(lambda diputado: {
        "ID": diputado.get("ID"), "Nombre": diputado.get('Nombre'), "partido": diputado.get('party'), 'coordX': diputado.get("coord1D"), 'coordY': diputado.get("coord2D"),  "participacion": a["votacion"][0][str(diputado["ID"])]}, a["wnominate"])]
    # diputados = [
    #     *diputados, *[diputado_fix(diputado, a["votacion"][0]) for diputado in a["wnominate"]]]
    print(a.get("id"))
    votacion = votaciones_collection.find_one({"id": a.get("id")})
    print(votacion)
    nominate2 = [
        *nominate2, {"id": a.get("id"), "periodoLegis": a.get("periodoLegis"), "diputados": diputados, "votacion": {"Nombre": votacion.get("nombre"), "boletin": votacion.get('boletin'), "fechaInicio": votacion.get('fechaIngresoBoletin'), 'fechaFin': votacion.get('fecha')}}]

new_wnominate_collection = db['new_wnominate']
new_wnominate_collection.insert_many(nominate2)
