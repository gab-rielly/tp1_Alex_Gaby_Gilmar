import time
from create_base import create_db
from create_tables import create_tables
from config import connect
from file_reader import inserir_dados_arquivo

if __name__ == '__main__':

    start_time = time.time()
    arquivo = 'amazon-meta.txt'

    #create_db()      # ESTES DOIS PRIMEIRO
    #create_tables()  # ESTES DOIS PRIMEIRO

    connect()                        # DEPOIS ESTES
    inserir_dados_arquivo(arquivo)   # DEPOIS ESTES

    #drop_db()
    
    end_time = time.time()
    
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    
    print(f"All operations completed successfully in {minutes} minutes and {seconds} seconds.")