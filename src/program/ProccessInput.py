
from Conection.Conection import Conection
from Extract.Extract import Extract

class Process :

   def __init__(self):
      self.con = Conection()
      self.databaseOLTP = 'AdventureWorksLT2022'
      self.databaseOLAP = 'VENTASPROYECTO'

   def printOptions(self):
      print("Ingrese el numero del metodo:\n ")
      print("1. Tabla ")
      print("2. Consulta")
      
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
               self.print(result)

               cols = input("\n ingrese el numero de campo separado por coma")
               cols = cols.split(",")
               print(cols)
               colsName = ""

               for i in cols :
                  colsName += result[int(i)] 
                  if(int(i)<len(cols)):
                     colsName += ","

               queryOLTP = self.createQuery(campo,colsName)
               tableOLAP = self.printTbalesOLAP()
               print(colsName)
               
               extract = Extract(self.databaseOLTP,queryOLTP,tableOLAP)
               extract.getData()
              


               break  
            else:
                print("\n Ingrese un numero en rango correcto \n ")

         except: 
           print("Ingrese un numero correcto  \n")

 
   def print(self,values):
      
      for i in values :
         print(f"{i}._ {values[i]}")


   def createQuery(self,table,cols):
      
      query = f"Select {cols} from {table} "
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
            print("\nselecione una tabla correcta\n")   
      
      return table
   
   def printQuery(self):
      print("\n Ingrese una consulta \n")
      query = str(input())

      tableOLAP = self.printTbalesOLAP()
      extract = Extract(self.databaseOLTP,query,tableOLAP)
      extract.getData()
      
      print(query,tableOLAP)










      


      

      
     
      



      




      


  
     
      
         









