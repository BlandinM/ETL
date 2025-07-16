from Conection.ConectionBD import ConectionBD
from Conection.Conection import Conection

class Extract : 

    def __init__(self,database,query,tableOLAP,conexion):
        self.queryOLTP = query
        self.tableOLAP = tableOLAP
        self.databaseOLTP = database
        self.conexion = conexion

    def getData(self):
        print(self.databaseOLTP + self.queryOLTP)
        try:
           cursor = self.conexion.getDataTables(self.databaseOLTP,self.queryOLTP)

           for i in cursor.fetchall():
            print(i)
        
       
        except  Exception as e:
           print(e)

      



