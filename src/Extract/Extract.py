from Conection.ConectionBD import ConectionBD
from Conection.Conection import Conection
from Transform.Transform import Transform
from Conection.Exceptions import Exceptions
from sqlalchemy.exc import SQLAlchemyError

class Extract:
    """
    Clase encargada de realizar la extracción de datos desde la base OLTP 
    y enviarlos a tranformación.
    """

    def __init__(self, database, databaseOLAP, query, tableOLAP, conexion):
        """
        Inicializa los parámetros necesarios para el proceso de extracción.

        Args:
            database (str): Nombre de la base de datos OLTP (origen).
            databaseOLAP (str): Nombre de la base de datos OLAP (destino).
            query (str): Consulta SQL para extraer los datos.
            tableOLAP (str): Nombre de la tabla destino en la base OLAP.
            conexion (Conection): Objeto de conexión para interactuar con las bases de datos.
        """
        self.queryOLTP = query
        self.tableOLAP = tableOLAP
        self.databaseOLTP = database
        self.conexion = conexion
        self.dbOLAP = databaseOLAP
        

    def getData(self):
        """
        Realiza la extracción de datos desde la base OLTP utilizando la consulta proporcionada,
         y llama al proceso de transformación .
        
        """
        try:
            
            conexion = self.conexion.getengine(self.dbOLAP).connect()

            df = self.conexion.getDataTables(self.databaseOLTP, self.queryOLTP)
           
            return df

        except SQLAlchemyError as e:
            Exceptions.handle_sql_error(e)
            return None
           
            
