# -*- coding: utf-8 -*-
"""
@author: Pierre Cottais
@title: Computer science for big data - MongoDB
"""

from pymongo import MongoClient
import pprint as pp

client = MongoClient(host="127.0.0.1:27017")
db = client["testdb"] # choix de la DB
# print(client)
# print(db)

print("Collections contenues dans la base testdb :",
      db.list_collection_names())

# # Exercice 1 : Requêtes sur la base MongoDB à partir d'un programme Python

# # affichage des resto de nom "Ihop"
# cursor = db.resto.find({"name": "Ihop"})