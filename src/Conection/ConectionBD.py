import pyodbc

class ConectionBD:

    def getConection(self,database):
        driver = 'ODBC Driver 18 for SQL Server'
        server = 'Angel\\SQLEXPRESS1'
        user = 'sa'
        password = '1234'
       
       

        conexion = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password};ENCRYPT=NO;")
        return conexion
    