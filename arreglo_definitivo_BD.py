
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

#colecciones existentes
wnominate_collection = db["wnominate"]
boletin_collection = db["BoletinFromDiputados"]
perfiles_parlamentarios = db["PerfilParlamentario"]
materias_collection = db["MateriasIndex"]
votaciones_collection = db["votaciones"]
#colecciones a crear
new_votaciones_collection = db['votaciones']
new_wnominate_collection = db['new_wnominate']
new_parlamentarios_collection = db['parlamentarios']
new_materias_collection = db['materiasEnUso']
new_parlamentarioAutor_collection = db['parlamentarioAutor']

#--------------------------Genera Votaciones-------------------------------#
if "votaciones" in db.list_collection_names():
    print("la coleccion de votaciones ya existe")
else:
    boletines = boletin_collection.find({"VotacionesAsoc": {"$exists": True, "$not": {"$size": 0}}, 'legislatura': {"$gte": 366}, "camaraOrigen": "Cámara de Diputados"})

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

    for votacion in votaciones:
        wnominate = list(wnominate_collection.find({"id": votacion["id"]}))
        if(wnominate):
            if(len(wnominate) >= 2):
                print(wnominate['id'])
            for w in wnominate:
                del w['_id']
            new_votaciones_collection.insert_one(votacion)
    print("la coleccion de votaciones fue creada con exito")
#--------------------------Genera Votaciones-------------------------------#
#--------------------------Genera Wnominate--------------------------------#
if "new_wnominate" in db.list_collection_names():
    print("la coleccion new wnominate ya existe")
else:
    wnominates = wnominate_collection.find({})
    nominate2 = []

    for a in wnominates:

        diputados = [*map(lambda diputado: {
            "ID": diputado.get("ID"), "Nombre": diputado.get('Nombre'), "partido": diputado.get('party'), 'coordX': diputado.get("coord1D"), 'coordY': diputado.get("coord2D"),  "participacion": a["votacion"][0][str(diputado["ID"])]}, a["wnominate"])]

        votacion = new_votaciones_collection.find_one({"id": a.get("id")})
        if(votacion):
            nominate2 = [
                *nominate2, {"id": a.get("id"), "periodoLegis": a.get("periodolegis"), "diputados": diputados, "votacion": {"Nombre": votacion.get("nombre"), "boletin": votacion.get('boletin'), "fechaInicio": votacion.get('fechaIngresoBoletin'), 'fechaFin': votacion.get('fecha')}}]

    new_wnominate_collection.insert_many(nominate2)
    print("la coleccion new wnominate fue creada con exito")
#--------------------------Genera Wnominate--------------------------------#
#--------------------------Genera parlamentarios---------------------------#
if "parlamentarios" in db.list_collection_names():
    print("la coleccion de parlamentarios ya existe")
else:

    perfiles = perfiles_parlamentarios.find(
    {"periodolegis":9,"distrito":{"$ne": None}})

    perfiles_sorted = sorted(perfiles, key=lambda x: x['id_b'])
    parlamentarios = []


    for parlamentario in perfiles_sorted:
        parlamentario["id"] = int(parlamentario["id_b"])

        boletines = boletin_collection.find(
        {"VotacionesAsoc": {"$exists": True, "$not": {"$size": 0}}, "ParlamentarioAutor":{"$all":[parlamentario["id"]]},'legislatura': {"$gte": 366}, "camaraOrigen": "Cámara de Diputados"})
        boletines_sorted = sorted(boletines, key=lambda x: x['legislatura'])
        ids=[]

        for boletin in boletines_sorted:
            for votacion in boletin["VotacionesAsoc"]:
                ids.append(votacion["id"])
        
        if len(ids) > 0:
            
            votaciones = new_votaciones_collection.find(
                {"id": {"$in":ids}})
            votaciones_sorted = sorted(votaciones, key=lambda x: x['id'])
            parlamentario["votaciones"] = votaciones_sorted

    for perfil in perfiles_sorted:
        del perfil['id_b']

    new_parlamentarios_collection.insert_many(perfiles_sorted)
    print("la coleccion de parlamentarios fue creada con exito")
#--------------------------Genera parlamentarios---------------------------#
#--------------------------Genera materias en uso--------------------------#
if "materiasEnUso" in db.list_collection_names():
    print("la coleccion de materias en uso ya existe")
else:
    boletines = boletin_collection.find(
        {"VotacionesAsoc": {"$exists": True, "$not": {"$size": 0}}, "Materias": {"$exists": True, "$not": {"$size": 0}},'legislatura': {"$gte": 366}, "camaraOrigen": "Cámara de Diputados"})


    boletines_sorted = sorted(boletines, key=lambda x: x['legislatura'])
    ids = []

    for boletin in boletines_sorted:
        for materia in boletin["Materias"]:
            ids.append(materia)

    ids = list(set(ids))

    materias = materias_collection.find(
        {"id":{"$in":ids}})
    
    new_materias_collection.insert_many(materias)

    print("la coleccion de materias en uso fue creada con exito")
    #--------------------------Genera materias en uso--------------------------#