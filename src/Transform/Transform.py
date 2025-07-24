from Conection.Conection import Conection
from Load.Load import Load
import pandas as pd


class Transform:

    def __init__(self):
        pass

    def processData(self, data, conexion, bdOLAP, table):
        """
        Procesa y transforma los datos extraídos.

        Args:
            data (pd.DataFrame): Son los datos extraídos de la base OLTP.
            conexion (Alchemy.Engine): Objeto de conexión a la base de datos.
            bdOLAP (str): Nombre de la base de datos OLAP.
            table (str): Nombre de la tabla destino en la base OLAP.
        """
        if table == 'Customers':
            data = self.concatText(data)
        elif table == 'tblTiempo':
            data = self.converDate(data)

        data = data.drop_duplicates()

        engine = conexion.getengine(bdOLAP).connect()
        df = pd.read_sql( f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'", engine)
        colsOLAP = len(df['COLUMN_NAME'].tolist())
        colsOLTP = len(data.columns.to_list())

        if colsOLAP == colsOLTP:
            data.columns = df['COLUMN_NAME'].to_list()
            columna_pk = df['COLUMN_NAME'].iloc[0] 
            data[columna_pk] = pd.to_datetime(data[columna_pk])
            valores_existentes = pd.read_sql(f"SELECT {columna_pk} FROM {table}", engine)
            valores_existentes[columna_pk] = pd.to_datetime(valores_existentes[columna_pk])

            values = data[columna_pk].isin(valores_existentes[columna_pk])
            newdata = data[values == False]
            print(newdata)

            load = Load()
            load.insertData(newdata, bdOLAP, table, conexion)
        else:
            print(f"Está tratando de migrar más columnas de las permitidas en la tabla {table}")
            print(data)

    def concatText(self, dfValues):
        """
        Une 2 columnas  en una sola columna.

        Args:
            dfValues (pd.DataFrame): Datos a transformar.

        Returns:
            pd.DataFrame: Datos con los valores ya unidos.
        """
        dfValues['FirstName'] = dfValues['FirstName'] + " " + dfValues['LastName']
        dfValues = dfValues.drop(columns=['LastName'])
        return dfValues

    def converDate(self, data):
        """
        Crea columnas derivadas a partir de una columna date:
        año, mes, semana, día de la semana, trimestre y cuatrimestre.

        Args:
            data (pd.DataFrame): Datos con columna tipo date.

        Returns:
            pd.DataFrame: Datos con las nuevas columnas  añadidas.
        """
        date = pd.to_datetime(data['OrderDate'])
        data['year'] = date.dt.year
        data['mes'] = date.dt.month_name()
        data['semana'] = date.dt.isocalendar().week
        data['diaSemana'] = date.dt.day_name()
        data['trimestre'] = date.dt.quarter
        data['cuatrimestre'] = ((date.dt.month - 1) // 4) + 1
        return data
