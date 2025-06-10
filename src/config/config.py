import os

class DatabaseConfig:
    @classmethod
    def get_connection_params(cls):
        params = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT')
        }
        print("[DatabaseConfig] Loaded params:", params)
        return params

    @classmethod
    def get_instance(cls):
        """Singleton pattern to get DatabaseConfig instance"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance
