from sqlalchemy import text
import pandas as pd

from Conection.ConectionBD import ConectionBD

class Conection:
    """
    Clase que gestiona las conexiones a bases de datos y la ejecución de consultas.
    """

    def __init__(self, conexionBD):
        """
        Inicializa la clase con una instancia de ConectionBD.

        Args:
            conexionBD (ConectionBD): Objeto que gestiona las conexiones a bases de datos.
        """
        self.conexionBD = conexionBD

    def conect(self, database, query):
        """
        Ejecuta una consulta SQL  sobre una base de datos y devuelve los resultados en un diccionario.

        Args:
            database (str): Nombre de la base de datos a conectar.
            query (str): Consulta SQL a ejecutar.

        Returns:
            dict: Diccionario con los resultados de la consulta, indexados por número.
        """
        conexion = self.conexionBD.getConection(database)
        if not conexion:
            return {}
        con = conexion.connect()
        result = con.execute(text(query))

        values = {i: str(row[0]) for i, row in enumerate(result.fetchall())}

        return values

    def getDataTables(self, database, query):
        """
        Ejecuta una consulta SQL y devuelve los resultados en un DataFrame de pandas.

        Args:
            database (str): Nombre de la base de datos a conectar.
            query (str): Consulta SQL a ejecutar.

        Returns:
            pandas.DataFrame: Resultado de la consulta.
        """
        engine = self.conexionBD.getConection(database)
        if not engine:
            return None

        conexion = engine.connect()
        result = pd.read_sql(query, conexion)

        return result

    def getengine(self, database):
        """
        Obtiene el motor de conexión a una base de datos específica.

        Args:
            database (str): Nombre de la base de datos.

        Returns:
            sqlalchemy.Engine: Motor de conexión a la base de datos.
        """
        return self.conexionBD.getConection(database)
