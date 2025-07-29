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
        data = self.printOpTransform(data)

        engine = conexion.getengine(bdOLAP).connect()
        df = pd.read_sql(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'", engine)
        colsOLAP = len(df['COLUMN_NAME'].tolist())
        colsOLTP = len(data.columns.to_list())
        print(colsOLTP)
        if (colsOLAP == colsOLTP ):
            data.columns = df['COLUMN_NAME'].to_list()
            columna_pk = df['COLUMN_NAME'].iloc[0]

            valores_existentes = pd.read_sql(f"SELECT {columna_pk} FROM {table}", engine)

            if (table == 'tblTiempo'):
                data.loc[:,columna_pk ]= pd.to_datetime(data[columna_pk])
                valores_existentes.loc[:,columna_pk] = pd.to_datetime(valores_existentes[columna_pk])

            values = data[columna_pk].isin(valores_existentes[columna_pk])
            newdata = data[~values]
    
            return newdata
        elif(table == 'Hechos_ventas'):
           
               
          df = df.iloc[1:,:]
          colOlapName = df['COLUMN_NAME'].to_list()
          print(colOlapName)
          data.columns = colOlapName


          existing_data = pd.read_sql(f"SELECT {','.join(colOlapName)} FROM {table}", engine)

          merged = data.merge(existing_data.drop_duplicates(), on=colOlapName, how='left', indicator=True)
          new_data = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

          return new_data

            

        else:
            print("❌ Error: El número de columnas no coincide entre la tabla OLTP y la OLAP.")
            print(f"Tabla OLTP tiene {colsOLTP} columnas, mientras que la tabla OLAP tiene {colsOLAP} columnas.")
            print("❌ No se puede realizar la transformación.")
            return None


    def printOpTransform(self, data):
        while True:
            print("\nVisualización de cómo están los datos extraídos:\n")
            print(data.head())
            
            print("Escoja una opción:")
            print("1. Guardar como mayúsculas")
            print("2. Guardar como minúsculas")
            print("3. Tratar campo tipo fecha")
            print("4. Unir columnas")
            print("5. Avanzar")

            op = input("Ingrese el número de la opción: ").strip()

            print("\nColumnas disponibles para aplicar el cambio:\n")
            print(list(data.columns))

            if op == '4':
                col1 = input("Ingrese la primera columna a fusionar: ").strip()
                col2 = input("Ingrese la segunda columna a fusionar: ").strip()
            elif op == '3':
                col = input("Ingrese el nombre del campo fecha: ").strip()
            elif op == '5':
                return data 
            else:
                cols = input("Ingrese el/los nombre(s) de las columnas separados por coma: ").strip().split(",")

            if op == '1':
                self.convertTextUpper(data, cols)
            elif op == '2':
                self.convertTextLower(data, cols)
            elif op == '3':
                data = self.converDate(data, col)
            elif op == '4':
                data = self.concatText(data, col1, col2)

            salir = input("Desea seguir aplicando transformaciones (no para salir) ").strip().lower()
            if salir == 'no':
                break
        
        return data
            
        


           

       
    def concatText(self, dfValues,col1,col2):
        
        """
        Une 2 columnas en una sola columna.

        Args:
            dfValues (pd.DataFrame): Datos a transformar.

        Returns:
            pd.DataFrame: Datos con los valores ya unidos.
        """
        dfValues[col1] = dfValues[col1] + " " + dfValues[col2]
        dfValues = dfValues.drop(columns=[col2])
        return dfValues

    def converDate(self, data,col):
        """
        Crea columnas derivadas a partir de una columna date:
        año, mes, semana, día de la semana, trimestre y cuatrimestre.

        Args:
            data (pd.DataFrame): Datos con columna tipo date.

        Returns:
            pd.DataFrame: Datos con las nuevas columnas añadidas.
        """
        date = pd.to_datetime(data[col])
        
        data['year'] = date.dt.year
        data['mes'] = date.dt.month_name()
        data['semana'] = date.dt.isocalendar().week
        data['diaSemana'] = date.dt.day_name()
        data['trimestre'] = date.dt.quarter
        data['cuatrimestre'] = ((date.dt.month - 1) // 4) + 1
        print(data)
        return data

    def convertTextUpper(self, data, cols):
     """
     Convierte el texto de las columnas especificadas a mayúsculas.

     Args:

        data (pandas.DataFrame): El DataFrame sobre el que se aplicará la transformación.
        cols (list): Lista con los nombres de las columnas que se desean convertir.

     Returns:
        pandas.DataFrame: DataFrame con las columnas transformadas a mayúsculas.
    """
     for col in cols:
      data.loc[:, col] = data[col].str.upper()
      return data

    def convertTextLower(self, data, cols):
     """
     Convierte el texto de las columnas especificadas a minúsculas.

     Args:
        data (pandas.DataFrame): El DataFrame sobre el que se aplicará la transformación.
        cols (list): Lista con los nombres de las columnas que se desean convertir.

     Returns:
        pandas.DataFrame: DataFrame con las columnas transformadas a minúsculas.
     """
     for col in cols:
        data.loc[:, col] = data[col].str.lower()
     return data
