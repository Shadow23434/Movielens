import sys
import os
from dotenv import load_dotenv

# Add the src directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Load environment variables
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)

from database.database import loadratings as _loadratings
from partitioning.partitioning import rangepartition,roundrobinpartition, rangeinsert, roundrobininsert

# Re-export the functions that the tester expects
__all__ = ['loadratings', 'rangepartition', 'roundrobinpartition', 'rangeinsert', 'roundrobininsert']

def loadratings(ratingstablename, filepath, openconnection):
    if not os.path.isabs(filepath):
        filepath = os.path.join(os.path.dirname(__file__), filepath)
    return _loadratings(ratingstablename, filepath, openconnection)
