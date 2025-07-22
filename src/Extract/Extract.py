from Conection.ConectionBD import ConectionBD
from Conection.Conection import Conection

class Extract : 

    def __init__(self,database,databaseOLAP,query,tableOLAP,conexion):
        self.queryOLTP = query
        self.tableOLAP = tableOLAP
        self.databaseOLTP = database
        self.conexion = conexion
        self.dbOLAP = databaseOLAP
        

    def getData(self):
      
        try:
           conexion =self.conexion.getengine(self.dbOLAP).connect()
           
           df = self.conexion.getDataTables(self.databaseOLTP,self.queryOLTP)
          
           df.to_sql(name = self.tableOLAP,con=conexion,if_exists= "append", index=False)
           
       
        except  Exception as e:
           print(e)

      



