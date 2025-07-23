from sqlalchemy import create_engine
import sqlalchemy

class ConectionBD:
    """
    Clase responsable de manejar las credenciales de acceso y establecer 
    la conexión a una base de datos SQL Server utilizando SQLAlchemy.
    """

    def __init__(self):
        """
        Inicializa los atributos necesarios para establecer la conexión.
        """
        self.user = None
        self.passwd = None
        self.server = None

    def getData(self):
        """
        Solicita al usuario sus credenciales de acceso y el nombre del servidor.
        """
        self.user = input("👤 Usuario: ")
        self.passwd = input("🔐 Contraseña: ")
        self.server = input("🖥️ Servidor: ")

    def getConection(self, database):
        """
        Establece y retorna una conexión SQLAlchemy a la base de datos especificada.

        Args:
            database (str): Nombre de la base de datos a la que se desea conectar.

        Returns:
            sqlalchemy.engine.Engine: Objeto Engine de SQLAlchemy si la conexión es exitosa.
        """
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

