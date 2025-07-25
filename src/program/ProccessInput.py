from Conection.Conection import Conection
from Extract.Extract import Extract
from Transform.Transform import Transform
from Load.Load import Load

class Process:
    """
    Clase encargada de gestionar la interacción con el usuario para seleccionar 
    bases de datos, tablas o consultas para ejecutar un proceso ETL.
    """

    def __init__(self, conexion):
        """
        Inicializa el proceso solicitando las bases de datos de origen (OLTP)
        y destino (OLAP).

        Args:
            conexion (Conection): Objeto de conexión para bases de datos.
        """
        self.con = conexion
        print("🔷 Base de datos ORIGEN (OLTP)")
        self.databaseOLTP = self.seleccionarBaseDatos()
        print("🔶 Base de datos DESTINO (OLAP)")
        self.databaseOLAP = self.seleccionarBaseDatos()

    def printOptions(self):
        """
        Muestra las opciones para seleccionar la fuente de datos: tabla o consulta,
        y maneja la elección del usuario.

        Returns:
            int: 1 si el usuario selecciona "Tabla", 2 si selecciona "Consulta".
        """
        print("Ingrese el número del método:\n ")
        print("1. Tabla ")
        print("2. Consulta")
        
        while True:
            try:
                option = int(input())
                print(option, "pu")
                if option == 1:
                    return 1
                elif option == 2:
                    return 2
            except:
                print("\n❌ Ingrese una opción válida")

    def printOptionsTables(self):
        """
        Muestra las tablas disponibles en la base OLTP para que el usuario
        seleccione una. Luego permite seleccionar columnas específicas 
        y construir una consulta personalizada.

        Returns:
            dict: Diccionario con los parámetros del proceso ETL:
                - databaseOLTP (str): Base de datos origen.
                - databaseOLAP (str): Base de datos destino.
                - query (str): Consulta SQL generada.
                - tableOLAP (str): Tabla destino en la base OLAP.
        """
        print("\nSeleccione una tabla de la base origen:\n")

        tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        tablesNames = self.con.conect(self.databaseOLTP, tables)
        self.print(tablesNames)

        while True:
            try:
                value = int(input("\nIngrese el número de la tabla que desea seleccionar: "))
                if 0 <= value <= len(tablesNames) - 1:
                    campo = tablesNames[value]
                    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{campo}'"
                    result = self.con.conect(self.databaseOLTP, query)
                    print(f"\n📋 Columnas de la tabla '{campo}':\n")
                    self.print(result)

                    cols = input("\n🧩 Ingrese el número de campo separado por coma: ")
                    cols = cols.split(",")
                    print(cols)

                    colsName = ""
                    count = 0
                    for i in cols:
                        colsName += result[int(i)]
                        if count < len(cols) - 1:
                            colsName += ","
                        count += 1

                    queryOLTP = self.createQuery(campo, colsName)
                    tableOLAP = self.printTbalesOLAP()

                    return {
                        "databaseOLTP": self.databaseOLTP,
                        "databaseOLAP": self.databaseOLAP,
                        "query": queryOLTP,
                        "tableOLAP": tableOLAP
                    }
                else:
                    print("\n❌ Ingrese un número dentro del rango.\n")
            except:
                print("❌ Ingrese un número válido.\n")

    def print(self, values):
        """
        Imprime un diccionario con índice y valor.

        Args:
            values (dict): Diccionario con índices y valores a imprimir.
        """
        for i in values:
            print(f"{i}._ {values[i]}")

    def createQuery(self, table, cols):
        """
        Crea una consulta SQL SELECT para las columnas seleccionadas de una tabla.

        Args:
            table (str): Nombre de la tabla.
            cols (str): Columnas separadas por coma para seleccionar.

        Returns:
            str: Consulta SQL generada.
        """
        query = f"SELECT {cols} FROM SalesLT.{table}"
        return query

    def printTbalesOLAP(self):
        """
        Muestra las tablas disponibles en la base OLAP para seleccionar 
        la tabla destino.

        Returns:
            str: Nombre de la tabla destino seleccionada.
        """
        print("\nSeleccione una tabla de la base destino:\n")
        tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        tablesNames = self.con.conect(self.databaseOLAP, tables)
        self.print(tablesNames)

        while True:
            try:
                value = int(input())
                table = tablesNames[value]
                print("🟢 Comenzando extracción...\n")
                print(table)
                break
            except:
                print("\n❌ Seleccione una tabla válida:\n")   

        return table

    def printQuery(self):
        """
        Solicita al usuario una consulta SQL, luego solicita la tabla destino y
        devuelve los parámetros necesarios para el proceso de extracción.

        Returns:
            dict: Diccionario con:
                - databaseOLTP (str): Base de datos origen.
                - databaseOLAP (str): Base de datos destino.
                - query (str): Consulta SQL ingresada.
                - tableOLAP (str): Tabla destino.
        """
        print("\n🧩 Ingrese una consulta personalizada SQL:\n")
        query = str(input())
        tableOLAP = self.printTbalesOLAP()

        return {
            "databaseOLTP": self.databaseOLTP,
            "databaseOLAP": self.databaseOLAP,
            "query": query,
            "tableOLAP": tableOLAP
        }

    def seleccionarBaseDatos(self):
        """
        Muestra las bases de datos disponibles y permite al usuario seleccionar una.

        Returns:
            str: Nombre de la base de datos seleccionada.
        """
        tipo = ""
        query = "SELECT name FROM sys.databases"
        bases = self.con.conect("master", query)

        print(f"\n📚 Seleccione la base de datos {tipo}:\n")
        self.print(bases)

        while True:
            try:
                opcion = int(input("Ingrese el número de la base de datos: "))
                if opcion in bases:
                    return bases[opcion]
                else:
                    print("❌ Opción fuera de rango.")
            except:
                print("❌ Ingrese un número válido.")
                return None
