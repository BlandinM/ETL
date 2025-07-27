# src/Conenction/Exceptions.py

class Exceptions:
    """
    Clase para manejar excepciones comunes de SQL Server.
    """

    @staticmethod
    def handle_sql_error(exception):
        """
        Maneja los errores de SQL Server.

        Args:
            exception (Exception): Excepción generada por SQLAlchemy.
        """
        if hasattr(exception, 'orig'):
            error = exception.orig
            print(f"❌ Error de SQL: Código: {error.args[0]}, Mensaje: {error.args[1]}")
        else:
            print(f"❌ Error desconocido: {str(exception)}")
