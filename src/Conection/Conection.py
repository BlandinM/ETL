import pyodbc
from Conection.ConectionBD import ConectionBD

class Conection:
    def __init__(self, conexionBD):
        self.conexionBD = conexionBD

    def conect(self, database, query):
        conexion = self.conexionBD.getConection(database)
        if not conexion:
            return {}

        cursor = conexion.cursor()
        cursor.execute(query)

        values = {i: str(row[0]) for i, row in enumerate(cursor.fetchall())}

        cursor.close()
        conexion.close()
        return values
  