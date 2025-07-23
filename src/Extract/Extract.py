from Conection.ConectionBD import ConectionBD
from Conection.Conection import Conection
from Transform.Transform import Transform

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
           print(len(df.columns.to_list()))
           transform = Transform()
           transform.processData(df,self.conexion,self.dbOLAP,self.tableOLAP)
        
          
           
       
        except  Exception as e:
           print(e)

      



