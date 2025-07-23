class Load:

    def __init__(self):
        pass

    def insertData(self, data, bdOLAP, table, conexion):
        """
        Inserta los datos transformados en la base OLAP.

        Args:
            data (pd.DataFrame): Los datos transformados a insertar.
            bdOLAP (str): Nombre de la base OLAP.
            table (str): Nombre de la tabla destino en la base OLAP.
            conexion (sqlalchemy.Engine): Objeto de conexión a la base de datos.
        """
        try:
            print(data)
            con = conexion.getengine(bdOLAP).connect()
            data.to_sql(table, con, if_exists="append", index=False)
        except Exception as e:
            print(e)
