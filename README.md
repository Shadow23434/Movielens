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
1. Download PostgreSQL from https://www.postgresql.org/download/windows/
2. Run the installer and follow these steps:
   - Choose installation directory
   - Select components (keep all default)
   - Choose data directory
   - Set password for database superuser (postgres)
   - Keep default port (5432)
   - Choose locale
   - **Important**: Check "Add PostgreSQL to PATH" during installation

#### 2. Create Database
After installation, open Command Prompt and run:
```bash
# Connect to PostgreSQL
psql -U postgres

# In psql console:
CREATE DATABASE dds_assgn1;
\q
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
pip install psycopg2-binary python-dotenv
```

#### 4. Configure Environment Variables
Create a `.env` file in the project root with:
```
DB_HOST=localhost
DB_NAME=dds_assgn1
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_PORT=5432
```

### Project Structure
```
movielens/
├── src/
│   ├── main.py
│   ├── database/
│   ├── partitioning/
│   └── utils/
└── tests/
    └── test_data.dat
```

### Features
- Range partitioning of ratings data
- Round-robin partitioning
- Data insertion with both partitioning methods
- Partition statistics generation

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