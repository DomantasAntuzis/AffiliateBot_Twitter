"""
MySQL database connection management
"""
import mysql.connector
from mysql.connector import Error
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from utils.logger import logger

def get_connection():
    """
    Create and return a MySQL database connection
    
    Returns:
        connection: MySQL connection object or None if failed
    """
    try:
        connection = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME
        )
        
        if connection.is_connected():
            return connection
            
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        return None

def close_connection(connection):
    """
    Close a MySQL database connection
    
    Args:
        connection: MySQL connection object
    """
    if connection and connection.is_connected():
        connection.close()

def execute_query(query, params=None, fetch=False, connection=None):
    """
    Execute a SQL query
    
    Args:
        query: SQL query string
        params: Query parameters (tuple)
        fetch: Whether to fetch results (default: False)
        connection: Database connection (required)
    
    Returns:
        Results if fetch=True, rowcount if fetch=False
    """
    if connection is None:
        logger.error("Connection parameter is required")
        return None

    try:
        cursor = connection.cursor()
        cursor.execute(query, params or ())
        
        if fetch:
            results = cursor.fetchall()
            cursor.close()
            return results
        else:
            rowcount = cursor.rowcount
            cursor.close()
            return rowcount
            
    except Error as e:
        logger.error(f"Error executing query: {e}")
        if 'cursor' in locals():
            cursor.close()
        return None

