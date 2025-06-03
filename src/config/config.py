import os

class DatabaseConfig:
    """Cấu hình kết nối database"""
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.database = os.getenv('DB_NAME', 'movielens_db')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', 'password')
        self.port = os.getenv('DB_PORT', '5432')

    @classmethod
    def get_instance(cls):
        """Singleton pattern để lấy instance của DatabaseConfig"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance 