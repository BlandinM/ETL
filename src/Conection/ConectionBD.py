from sqlalchemy import create_engine
import sqlalchemy

class ConectionBD:
    def __init__(self):
        self.user = None
        self.passwd = None
        self.server = None
        

    def getData(self):
        self.user = input("👤 Usuario: ")
        self.passwd = input("🔐 Contraseña: ")
        self.server = input("🖥️ Servidor: ")

    def getConection(self, database):Testing
       try:
           driver = 'ODBC+Driver+17+for+SQL+Server'
           connection_str = (
            f"mssql+pyodbc://{self.user}:{self.passwd}@{self.server}/{database}"
            f"?driver={driver}"
            "&Encrypt=no&TrustServerCertificate=yes"
          )
           engine = create_engine(connection_str)
           print(connection_str)
           return engine
       except Exception as e:
         print(f"❌ Error al conectar: {e}")
         return None

     
