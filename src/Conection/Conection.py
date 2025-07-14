import pyodbc
from Conection.ConectionBD import ConectionBD

class Conection :

    def conect(self,database,q):

        conexioBD = ConectionBD()
        conexion = conexioBD.getConection(database)    
     
        cursor = conexion.cursor()
        cursor.execute(q)
        
        valuesTables = {}
        cont = 0

        for row in cursor.fetchall():
            table = str(row[0])
            valuesTables[cont] = table
            cont += 1    
       
      
        cursor.close()
        conexion.close()
        return valuesTables


  