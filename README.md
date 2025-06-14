# MovieLens Database Partitioning Project

### Overview
This project implements database partitioning strategies for the MovieLens dataset using PostgreSQL. It demonstrates both range partitioning and round-robin partitioning techniques for handling movie ratings data.

### Prerequisites
- Ubuntu 16.04
- Python 3.12.3
- PostgreSQL 17.5 or later
- Required Python packages:
  - psycopg2
  - python-dotenv
  - requests

### Installation Steps

#### 1. Install PostgreSQL
```bash
   sudo apt update
   sudo apt install postgresql postgresql-contrib
```

#### 2. Create Database
```bash
   sudo systemctl start postgresql
   sudo systemctl enable postgresql
   sudo -u postgres createdb your_database_name
```

#### 3. Set up Python Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install required packages
pip install psycopg2-binary python-dotenv requests
# Or
   pip install -r requirements.txt
```

#### 4. Configure the `.env` File

Create a `.env` file in the project root directory with the following content. Replace the values with your actual PostgreSQL configuration:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_postgres_user
DB_PASSWORD=your_postgres_password
```

- `DB_HOST`: Hostname of your PostgreSQL server (usually `localhost`)
- `DB_PORT`: Port number (default is `5432`)
- `DB_NAME`: Name of the database you created
- `DB_USER`: PostgreSQL username
- `DB_PASSWORD`: PostgreSQL password

### Project Structure
```
movielens/
├── src/
│   ├── main.py                  # Main application entry point
│   ├── database/
│   │   ├── __init__.py
│   │   └── database.py          # Database connection and loadratings implementation
│   ├── partitioning/
│   │   ├── __init__.py
│   │   └── partitioning.py      # Range and round-robin partitioning implementation
│   └── utils/
│       ├── __init__.py
│       └── utils.py             # Utility functions for data handling
├── tests/
│   ├── Assignment1Tester.py     # Automated tester script
│   ├── Interface.py             # Interface for tester
│   ├── testHelper.py            # Helper functions for testing
│   └── test_data.dat            # Test data file
├── .env                         # Environment variables configuration
├── requirements.txt             # Python package dependencies
└── README.md                    # Project documentation
```

### Features
- Range partitioning of ratings data
- Round-robin partitioning
- Data insertion with both partitioning methods

### Usage
1. Ensure PostgreSQL is running
2. Activate virtual environment
3. Run the main script:
```bash
python src/main.py
```

### Troubleshooting
If you encounter database connection errors:
1. Verify PostgreSQL is running: `sudo systemctl status postgresql`
2. Check connection parameters in `.env` file
3. Ensure database exists: `psql -U postgres -c "\l"`
4. Verify user permissions: `psql -U postgres -d dds_assgn1`

### Contributing
Feel free to submit issues and enhancement requests

### License
This project is licensed under the MIT License