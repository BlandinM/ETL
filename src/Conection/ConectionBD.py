import pyodbc

class ConectionBD:
    def __init__(self):
        self.user = None
        self.passwd = None
        self.server = None
        

    def getData(self):
        self.user = input("👤 Usuario: ")
        self.passwd = input("🔐 Contraseña: ")
        self.server = input("🖥️ Servidor: ")

    def getConection(self, database):
      
        try:
            print(database + " 1")
            driver = '{ODBC Driver 17 for SQL Server}'
            connection_str = (
                f'DRIVER={driver};'
                f'SERVER={self.server};'
                f'DATABASE={database};'
                f'UID={self.user};PWD={self.passwd};'
                f'Encrypt=no;TrustServerCertificate=yes'
            )
            conexion = pyodbc.connect(connection_str)
            return conexion
        except pyodbc.Error as e:
            print(f"❌ Error al conectar: {e}")
            return None
