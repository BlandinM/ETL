from Conection.ConectionBD import ConectionBD

class Extract :

    def __init__(self,database,query,tableOLAP):
        self.queryOLTP = query
        self.tableOLAP = tableOLAP
        self.database = database
        self.conexion = ConectionBD()

    def getData(self):
        try:
           con = self.conexion.getConection(self.database)
           cursor = con.cursor()
           cursor.execute(self.queryOLTP)

           for i in cursor.fetchall():
            print(i)
        
       
        except:
           print(self.queryOLTP)

      



