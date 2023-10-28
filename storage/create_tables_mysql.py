import mysql.connector

db_conn = mysql.connector.connect(host="acit3855-system.eastus.cloudapp.azure.com", user="user",
password="password", database="events")

db_cursor = db_conn.cursor()


db_cursor.execute('''
    CREATE TABLE book_hold 
       (id INT NOT NULL AUTO_INCREMENT,
        book_id VARCHAR(250) NOT NULL,
        user_id VARCHAR(250) NOT NULL,
        branch_id INTEGER NOT NULL,
        availability INTEGER NOT NULL,
        timestamp VARCHAR(100) NOT NULL,
        date_created VARCHAR(100) NOT NULL,
        trace_id VARCHAR(250) NOT NULL,
        CONSTRAINT booK_hold_pk PRIMARY KEY (id) )
''')

db_cursor.execute('''
    CREATE TABLE movie_hold 
       (id INT NOT NULL AUTO_INCREMENT,
        movie_id VARCHAR(250) NOT NULL,
        user_id VARCHAR(250) NOT NULL,
        branch_id INTEGER NOT NULL,
        availability INTEGER NOT NULL,
        timestamp VARCHAR(100) NOT NULL,
        date_created VARCHAR(100) NOT NULL,
        trace_id VARCHAR(250) NOT NULL,
        CONSTRAINT booK_hold_pk PRIMARY KEY (id) )
''')

db_conn.commit()
db_conn.close()
