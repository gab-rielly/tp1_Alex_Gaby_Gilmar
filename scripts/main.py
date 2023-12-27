import time
# from create_database import create_db |FAZER ALTERAÇÕES|
from table_creation import create_tables
from config import connect

if _name_ == '_main_':

    start_time = time.time()
    
    #create_db()
    create_tables()
    #connect()

    #drop_db()
    
    end_time = time.time()
    
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    
    print(f"All operations completed successfully in {minutes} minutes and {seconds} seconds.")