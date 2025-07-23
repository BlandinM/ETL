from Conection.Conection import Conection
from Extract.Extract import Extract

class Process:
    """
    Clase encargada de gestionar la interacción con el usuario para seleccionar 
    bases de datos, tablas o consultas.
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
        """
        print("Ingrese el numero del metodo:\n ")
        print("1. Tabla ")
        print("2. Consulta")
        
        while True:
            try:
                option = int(input())
                if option == 1:
                    self.printOptionsTables()
                    break
                elif option == 2:
                    self.printQuery()
                    break
            except:
                print("\n ingrese una opción valida")

    def printOptionsTables(self):
        """
        Muestra las tablas disponibles en la base OLTP para que el usuario
        seleccione una, y permite seleccionar columnas específicas para 
        formar una consulta, y finalmente inicia la extracción.
        """
        print("\n Seleccione una tabla de la base origen  \n")

        tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        tablesNames = self.con.conect(self.databaseOLTP, tables)
        self.print(tablesNames)
        while True:
            try:
                value = int(input("\n Ingrese el numero de la tabla que desea seleccionar"))
                if 0 <= value <= len(tablesNames) - 1:
                    campo = tablesNames[value]
                    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{campo}'"
                    result = self.con.conect(self.databaseOLTP, query)
                    print(f"\n 📋 Columnas de la tabla '{campo}': \n")
                    self.print(result)

                    cols = input("\n 🧩 Ingrese el numero de campo separado por coma: ")
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
                    print(queryOLTP + " query")
                    tableOLAP = self.printTbalesOLAP()

                    extract = Extract(self.databaseOLTP, self.databaseOLAP, queryOLTP, tableOLAP, self.con)
                    extract.getData()
                    break
                else:
                    print("\n 🧩 Ingrese un numero en rango correcto \n ")
            except:
                print("Ingrese un numero correcto  \n")

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
        query = f"Select {cols} from SalesLT.{table} "
        return query

    def printTbalesOLAP(self):
        """
        Muestra las tablas disponibles en la base OLAP para seleccionar la tabla destino.

        Returns:
            str: Nombre de la tabla destino seleccionada.
        """
        print("\n Seleccione una tabla de la base destino  \n")
        tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        tablesNames = self.con.conect(self.databaseOLAP, tables)
        self.print(tablesNames)

        while True:
            try:
                value = int(input())
                table = tablesNames[value]
                print("comenzando extraccion... \n")
                break
            except:
                print("\nselecione una tabla correcta: \n")   

        return table

    def printQuery(self):
        """
        Solicita al usuario una consulta SQL, luego solicita la tabla destino y
        comienza el proceso de extracción con esa consulta.
        """
        print("\n 🧩 Ingrese una consulta \n")
        query = str(input())

        tableOLAP = self.printTbalesOLAP()
        extract = Extract(self.databaseOLTP, self.databaseOLAP, query, tableOLAP, self.con)
        extract.getData()

    def seleccionarBaseDatos(self):
        """
        Muestra las bases de datos disponibles para que el usuario seleccione una.

        Returns:
            str: Nombre de la base de datos seleccionada.
        """
        tipo=""
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
