from Conection.ConectionBD import ConectionBD
from Conection.Conection import Conection
from program.ProccessInput import Process

class Menu:
    @staticmethod
    def principal_menu():
        print("=== Sistema ETL del Grupo 4 ===")
        print("🟢 Por favor inicie sesión:")
        
        try:
            
            conexion_bd = ConectionBD() 
            conexion_bd.getData()
            conexion = Conection(conexion_bd)
            print("🔵 Conexión a la base de datos establecida correctamente.")
        except Exception as e:
            print(f"❌ Error al conectar a la base de datos: {e}")
            return

        while True:
            print("\n1. Iniciar Proceso ETL")
            print("2. Salir")
            choice = input("Seleccione una opción: ")

            if choice == '1':
                proceso = Process(conexion)
                proceso.printOptions()
                break
            elif choice == '2':
                print("👋 Saliendo...")
                break
            else:
                print("❌ Opción no válida.")
