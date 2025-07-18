
from Conection.Conection import Conection
from Extract.Extract import Extract

class Process :

   def __init__(self,conexion):
    self.con = conexion
    print("🔷 Base de datos ORIGEN (OLTP)")
    self.databaseOLTP = self.seleccionarBaseDatos()
    print("🔶 Base de datos DESTINO (OLAP)")
    self.databaseOLAP = self.seleccionarBaseDatos()


   def printOptions(self):
      print("Ingrese el numero del metodo:\n ")
      print("1. Tabla ")
      print("2. Consulta ")
      
      while True:

         try:
           option = int(input())
           if(option == 1):
              self.printOptionsTables()
              break
           elif(option == 2): 
              self.printQuery()
              break
              
           
         except:
            print("\n ingrese una opción valida")


   

   def printOptionsTables(self) :

      print("\n Seleccione una tabla de la base origen  \n")

      tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
      tablesNames = self.con.conect(self.databaseOLTP,tables);
      self.print(tablesNames)
      while True :

         try:
            value = int(input("\n Ingrese el numero de la tabla que desea seleccionar"))

            if (value >=  0 and value <= len(tablesNames) - 1):
               
               campo = tablesNames[value]
            
               query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{campo}'"
               result = self.con.conect(self.databaseOLTP,query)
               print(f"\n 📋 Columnas de la tabla '{campo}': \n")
               self.print(result)

               cols = input("\n 🧩 Ingrese el numero de campo separado por coma: ")
               cols = cols.split(",")
               print(cols)
               colsName = ""

               for i in cols :
                  colsName +=result[int(i)]
                  if(int(i)<len(cols) - 1):
                     colsName += ","

               queryOLTP = self.createQuery(campo,colsName)
               print(queryOLTP + " query")
               tableOLAP = self.printTbalesOLAP()
               
               
               extract = Extract(self.databaseOLTP,queryOLTP,tableOLAP,self.con)
              
               extract.getData()
              


               break  
            else:
                print("\n 🧩 Ingrese un numero en rango correcto \n ")

         except: 
           print("Ingrese un numero correcto  \n")

 
   def print(self,values):
      
      for i in values :
         print(f"{i}._ {values[i]}")


   def createQuery(self,table,cols):
      
      query = f"Select {cols} from SalesLT.{table} "
      return query

   
   def printTbalesOLAP(self):
      print("\n Seleccione una tabla de la base destino  \n")
      tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
      tablesNames = self.con.conect(self.databaseOLAP,tables);
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
      print("\n 🧩 Ingrese una consulta \n")
      query = str(input())

      tableOLAP = self.printTbalesOLAP()
      extract = Extract(self.databaseOLTP,query,tableOLAP,self.con)
      extract.getData()
      
      print(query,tableOLAP)


   def seleccionarBaseDatos(self, tipo=""):
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




   

      

      
     
      



      




      


  
     
      
         









