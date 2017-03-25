from courtutils.databases.postgres import PostgresDatabase

db = PostgresDatabase('circuit')
db.set_person_id([7,9])
db.disconnect()
