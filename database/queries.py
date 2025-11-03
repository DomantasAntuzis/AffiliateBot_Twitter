"""
SQL queries for the Affiliate Bot
Centralized location for all database queries
"""

# User queries
INSERT_USER = """
    INSERT INTO users (username, password_hash, role) 
    VALUES (%s, %s, %s)
"""

GET_USER_BY_USERNAME = """
    SELECT * FROM users WHERE username = %s
"""

# Posted games queries
INSERT_POSTED_GAME = """
    INSERT INTO posted_games (title, source, discount, sale_price, posted_at) 
    VALUES (%s, %s, %s, %s, NOW())
"""

GET_RECENT_POSTED_GAMES = """
    SELECT title FROM posted_games 
    ORDER BY posted_at DESC 
    LIMIT %s
"""

DELETE_OLD_POSTED_GAMES = """
    DELETE FROM posted_games 
    WHERE posted_at < DATE_SUB(NOW(), INTERVAL 30 DAY)
"""

# Deals queries
INSERT_DEAL = """
    INSERT INTO deals (title, source, link, image_link, price, discount, created_at) 
    VALUES (%s, %s, %s, %s, %s, %s, NOW())
"""

GET_VALID_DEALS = """
    SELECT * FROM deals 
    WHERE discount > %s 
    AND title NOT IN (
        SELECT title FROM posted_games 
        ORDER BY posted_at DESC 
        LIMIT %s
    )
"""

DELETE_OLD_DEALS = """
    DELETE FROM deals 
    WHERE created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)
"""

