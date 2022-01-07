from pymongo import MongoClient
import pprint
client = MongoClient('localhost', 27017)

pp = pprint.PrettyPrinter(indent=4)

db = client.quevotan
boletin_collection = db["BoletinFromDiputados"]
boletines = boletin_collection.find(
    {"VotacionesAsoc": {"$exists": True, "$not": {"$size": 0}}, 'legislatura': {"$gte": 365}, "camaraOrigen": "Cámara de Diputados"})

counter = 0
boletines_sorted = sorted(boletines, key=lambda x: x['legislatura'])
votaciones = []


for boletin in boletines_sorted:
    votaciones = [*votaciones, *
                  map(lambda votacion: {**votacion, "fechaIngresoBoletin": boletin["fechaIngreso"], "fecha": votacion["date"], "boletin": votacion['bol'], 'totalNo': votacion["totalNO"], "nombre": votacion["materia"], "materias": boletin["Materias"]}, boletin["VotacionesAsoc"])]


for votacion in votaciones:
    del votacion['date']
    del votacion['bol']
    del votacion['totalNO']
    del votacion['tipoVotacion']
    del votacion['sesion']
    del votacion['materia']


print(len(votaciones))

votaciones_collection = db['votaciones']
votaciones_collection.insert_many(votaciones)


# pp.pprint(votaciones)
