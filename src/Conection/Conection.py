from sqlalchemy import text
import pandas as pd

from Conection.ConectionBD import ConectionBD

class Conection:
    def __init__(self, conexionBD):
        self.conexionBD = conexionBD

    def conect(self, database, query):
        conexion = self.conexionBD.getConection(database)
        if not conexion:
            return {}
        con = conexion.connect()
        result = con.execute(text(query)) 


        values = {i: str(row[0]) for i, row in enumerate(result.fetchall())}

       
        return values
    

    def getDataTables(self,database,query):

        engine = self.conexionBD.getConection(database)
        if not engine:
            return None
        
        conexion = engine.connect()
        result = pd.read_sql(query,conexion)
        
        return result

    def getengine(self,database):
        return self.conexionBD.getConection(database)
  