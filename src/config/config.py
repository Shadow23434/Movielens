import os

class DatabaseConfig:
    """Database connection configuration"""
    HOST = os.getenv('DB_HOST')
    DATABASE = os.getenv('DB_NAME')
    USER = os.getenv('DB_USER')
    PASSWORD = os.getenv('DB_PASSWORD')
    PORT = os.getenv('DB_PORT')

    @classmethod
    def get_connection_params(cls):
        """Returns connection parameters as a dictionary"""
        return {
            'host': cls.HOST,
            'database': cls.DATABASE,
            'user': cls.USER,
            'password': cls.PASSWORD,
            'port': cls.PORT
        }

    @classmethod
    def get_instance(cls):
        """Singleton pattern to get DatabaseConfig instance"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance 