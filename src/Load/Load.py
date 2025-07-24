import pandas as pd

class Load:

    def __init__(self):
        pass
    
    def insertData(self, data, bdOLAP, table, conexion):
        try:
            engine = conexion.getengine(bdOLAP)

            with engine.begin() as con:
                # Leer todos los datos existentes de la tabla OLAP para comparar
                olap_data = pd.read_sql(f"SELECT * FROM {table}", con)

                # Si la tabla está vacía, insertar todo
                if olap_data.empty:
                    print(f"🔄 La tabla {table} está vacía. Insertando {len(data)} registros...")
                    data.to_sql(table, con, if_exists="append", index=False, schema='dbo')
                    print(f"✅ {len(data)} registros insertados en {table}.")
                    return

                # Crear una lista para filas nuevas
                new_rows = []

                # Convertir ambos DataFrames a listas de tuplas para comparación rápida
                olap_tuples = [tuple(x) for x in olap_data.values]
                data_tuples = [tuple(x) for x in data.values]

                for row in data_tuples:
                    if row not in olap_tuples:
                        new_rows.append(row)

                if not new_rows:
                    print(f"✅ No hay registros nuevos para insertar en {table}.")
                    return
                
                # Crear DataFrame con solo las filas nuevas
                new_df = pd.DataFrame(new_rows, columns=data.columns)

                print(f"🟢 Insertando {len(new_df)} nuevos registros en {table}...")
                new_df.to_sql(table, con, if_exists="append", index=False, schema='dbo')
                print(f"✅ {len(new_df)} registros insertados en {table}.")

        except Exception as e:
            print(f"❌ Error al insertar datos en {table}: {e}")
