from Extract.Extract import Extract
from Transform.Transform import Transform
from program.ProccessInput import Process
from Load.Load import Load

class Controller:
    """
    Clase principal que orquesta el proceso ETL (Extracción, Transformación y Carga).
    Se encarga de recibir las entradas del usuario, ejecutar la extracción de datos,
    transformarlos y cargarlos en la base de datos OLAP.
    """

    def __init__(self, con):
        """
        Inicializa el controlador con una conexión a las bases de datos.

        Args:
            con (Conection): Objeto de conexión para interactuar con las bases de datos.
        """
        self.con = con

    def controllerProcces(self):
        """
        Método principal que ejecuta el flujo completo del proceso ETL.
        """

        
        process = Process(self.con)
        option = process.printOptions()

        if (option == 1):
            print(option)
            dictData = process.printOptionsTables()
        else:
            dictData = process.printQuery()

        db_oltp = dictData["databaseOLTP"]
        db_olap = dictData["databaseOLAP"]
        query = dictData["query"]
        table = dictData["tableOLAP"]

        
        extract = Extract(db_oltp, db_olap, query, table, self.con)
        df = extract.getData()
        

        
        if (df is not None):
            
            transform = Transform()
            dataTransform = transform.processData(df, self.con, db_olap, table)

            if (dataTransform is not None):
                
                load = Load()
                insert = load.insertData(dataTransform, db_olap, table, self.con)

                if( insert is True):
                    print("Insertado con éxito")
                    print("Desea seguir ")
                    print("1 si ")
                    print("2 no ")
                    op = str(input("Ingrese una opción: "))
                    if (op == '1' or op.lower() == 'si'):
                        self.controllerProcces()
                    else:
                        print("Saliendo...")
                else:
                    print("\nFalló la tranformación\n")
                    self.controllerProcces()
        else:
            print("\nFalló la extracción\n")
            self.controllerProcces()
