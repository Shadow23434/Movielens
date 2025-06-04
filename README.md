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
   sudo -u postgres createdb dds_assgn1
   sudo -u postgres psql
   ALTER USER postgres WITH PASSWORD '1234';
```

#### 3. Set up Python Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Ubuntu:
source venv/bin/activate

# Install required packages
pip install psycopg2-binary python-dotenv requests
# Or
   pip install -r requirements.txt
```

### Project Structure
```
csdlpt/
├── src/
│   ├── main.py              # Main application entry point
│   ├── database/
│   │   ├── __init__.py
│   │   └── database.py      # Database connection and operations
│   ├── partitioning/
│   │   ├── __init__.py
│   │   └── partitioning.py  # Range and round-robin partitioning implementation
│   └── utils/
│       ├── __init__.py
│       └── utils.py         # Utility functions for data handling
├── tests/
│   └── test_data.dat       # Test data files
├── .env                    # Environment variables configuration
├── requirements.txt        # Python package dependencies
└── README.md              # Project documentation
```

### Features
- Range partitioning of ratings data
- Round-robin partitioning
- Data insertion with both partitioning methods
- Partition statistics generation
- Data integrity verification

### Usage
1. Ensure PostgreSQL is running
2. Activate virtual environment
3. Run the main script:
```bash
python src/main.py
```

### Troubleshooting
If you encounter database connection errors:
1. Verify PostgreSQL is running:
   - Windows: Check Services app for "PostgreSQL" service
   - Ubuntu: `sudo systemctl status postgresql`
2. Check connection parameters in `.env` file
3. Ensure database exists: `psql -U postgres -c "\l"`
4. Verify user permissions: `psql -U postgres -d dds_assgn1`

### Contributing
Feel free to submit issues and enhancement requests

### License
This project is licensed under the MIT License 