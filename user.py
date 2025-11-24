import bcrypt
from database.db_connect import get_connection

username = "admin"
password = "admin123"
role = "admin"

# Hash password
password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Insert
conn = get_connection()
cursor = conn.cursor()
cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)", (username, password_hash, role))
conn.commit()
cursor.close()
conn.close()