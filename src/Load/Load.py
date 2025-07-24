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
            if data.empty:
                print(f"⚠️  No hay nuevos registros para insertar en la tabla '{table}'.")
                return

            print(f"\n🔄 Migrando datos a la tabla '{table}'...")

            con = conexion.getengine(bdOLAP).connect()
            data.to_sql(table, con, if_exists="append", index=False)

            print(f"✅ Se insertaron {len(data)} nuevos registros en 🗂️ '{table}'\n")
        
        except Exception as e:
            print(f"❌ Error al insertar datos en la tabla '{table}': {e}")
