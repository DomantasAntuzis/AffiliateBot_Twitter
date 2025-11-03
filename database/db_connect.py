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
            logger.info(f"Successfully connected to MySQL database: {config.DB_NAME}")
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
        logger.info("MySQL connection closed")

def execute_query(query, params=None, fetch=False):
    """
    Execute a SQL query
    
    Args:
        query: SQL query string
        params: Query parameters (tuple)
        fetch: Whether to fetch results (default: False)
    
    Returns:
        Results if fetch=True, rowcount if fetch=False
    """
    connection = get_connection()
    if not connection:
        return None
    
    try:
        cursor = connection.cursor()
        cursor.execute(query, params or ())
        
        if fetch:
            results = cursor.fetchall()
            cursor.close()
            close_connection(connection)
            return results
        else:
            connection.commit()
            rowcount = cursor.rowcount
            cursor.close()
            close_connection(connection)
            return rowcount
            
    except Error as e:
        logger.error(f"Error executing query: {e}")
        close_connection(connection)
        return None

