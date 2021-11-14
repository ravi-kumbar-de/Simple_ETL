import sqlite3
from sqlite3 import Error
import os
import pandas as pd
from datetime import date
import configparser as config


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print('Connected Database =>', db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def check_file(file,path):
    """ 
     this function returns length of file
    """
    print("FILE =>",path+file)
    with open(path+file,"r") as f:
        try:
            return(len(f.readlines()))
        except:
            return 0
        

def read_files(table_name,path,con):
    """
      This funtion reads different file formats into dataframe 
    """
    
    print("READING FILE FROM SOURCE LOCATION")

    files_list = os.listdir(path)
    today = date.today()
    today = today.strftime("%d%m%Y")
    
    if len(files_list) == 0:
        print('No files at server')
        return 0
        
    for file in files_list:
        
        
        if file.endswith(today+'.csv'): 
            
            if check_file(file,path) > 0:
                df = pd.read_csv(path+file)
                df['inserted_on'] = pd.to_datetime('today').strftime("%d%m%Y")
                print('FILE READ')
                insert_into_stage(table_name,df,con)
            else:
                print('File is empty')
                return 0
        
        elif file.endswith(today+'.txt'): 
            
            if check_file(file,path) > 0:
                df = pd.read_csv(path+file, sep='|')
                df['inserted_on'] = pd.to_datetime('today').strftime("%d%m%Y")
                print('FILE READ')
                insert_into_stage(table_name,df,con)
            else:
                print('File is empty')
                return 0
            
            
        elif file.endswith(today+'.json'):
            if check_file(file,path) > 0:
                 df = pd.read_json(path+file)
                 df['inserted_on'] = pd.to_datetime('today').strftime("%d%m%Y")
                 print('FILE READ')
                 insert_into_stage(table_name,df,con)
            else:
                print('File is empty')
                return 0
                
        elif file.endswith(today+'.xlsx'):
            if check_file(file,path) > 0:
                df = pd.read_excel(path+file)
                df['inserted_on'] = pd.to_datetime('today').strftime("%d%m%Y")
                print('FILE READ')
                insert_into_stage(table_name,df,con)
            else:
                print('Failed : File is Empty')
                return 0
            
        else:
                print(file,'File format and source need to be added')
                return 0
    

def insert_into_stage(table,df,conn):
    """
    This is simple way to insert into stage table
    if needed we can generate insert queries as well , schema check can be added
    
    """
    print('INSERTING INTO SATGE')
    df.drop('H',axis='columns', inplace=True)
    df.rename(columns={'Customer_Id': 'CustomerId', 'Customer_Name': 'CustomerName','Dr_Name': 'DoctorConsulted',"Open_Date":"CustomerOpenDate","Last_Consulted_Date":"LastConsultedDate","Vaccination_Id":"VaccinationType","DOB":"DateofBirth" , "Is_Active":"ActiveCustomer"}, inplace=True)  
    df.to_sql(table, conn,if_exists='append', index=False)
    print('Successful')
    print("================================================")
    
    

def  transformAndLoad_into_target(stage,conn,table_ddl):
     """
        This funtion creates countrywise tables with schema based on distinct country values in stage table
        also generates insert queries and load data to target tables
     """
     print("=========================================================")
     columns ="CustomerName,CustomerID,CustomerOpenDate,LastConsultedDate,VaccinationType,DoctorConsulted,State,Country,PostCode,DateofBirth,ActiveCustomer,Inserted_on"
     print('LOADING COUNTRYWISE DATA INTO TARGET')
     query = 'select distinct Country from {}'.format(stage)
        
     res = run_query(conn,query)
     for Country in res:
        table_name = "med_users_"+Country[0]
        table_ddl=table_ddl.replace('med_users_master','{0}')
        new_ddl=str(table_ddl).format(table_name)
        if conn is not None:
            # create country table and load
            try:
                res =run_query(conn,new_ddl)

            except Error as e:
                print(e)  
                
            #Generate insert queries and load
            try:
                print("Target Table => ",table_name)
                insert_query= """Insert into med_users_{0} ({1})
                                select {1} from med_users_master
                                where Country ='{0}'
                                """.format(Country[0],columns)
                run_query(conn,insert_query)
                print("TABLE CREATED & DATA LOADED")
                print('----------------------------')
                
            except Error as e:
                 print(e)

                    
def run_query(conn,query):
    
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param query: query which you want to execute
    :return: result
    """
    
    try:
        c = conn.cursor()
        c.execute(query)
        return(c.fetchall())
    
    except Error as e:
        print(e)

def validate(conn, stage_table):
    """
    validate record count between stage and target table on job run date 
    
    """
    print('=============================================')
    print('COUNT Validation between Stage and Target')
    country_query = "select distinct country from {0}".format(stage_table)
    res = run_query(conn,country_query)

    for country in res:

        stage_query =  """select count(*) table_count from {0}
        where Inserted_on =STRFTIME('%d%m%Y', date('now')) and country ='{1}' """.format(stage_table, country[0])
        stage_count_res = run_query(conn,stage_query )
        stage_count = stage_count_res[0][0]
        target_query = """select count(*) from {0}
                        where Inserted_on =STRFTIME('%d%m%Y', date('now'))""".format('med_users_'+country[0])
        target_count_res = run_query(conn,target_query)
        target_count = target_count_res[0][0]

        print('Country','\t',stage_table,'\t','med_users_'+country[0],'\t', "Result")
        print(country[0],'\t',stage_count,"\t",target_count,'\t', 'MATCH' if stage_count == target_count else "MISMATCH")

def main():

    today= date.today()
    print("===================== ",today,"==============================")
    
    database = r"C:\Users\ravi.kumbar\Desktop\BIProjects\assignments\sqlite\db\DEV.db"
    path ="C:/Users/ravi.kumbar/Desktop/BIProjects/assignments/input/"
    
    # create a database connection
    conn = create_connection(database)
    
    # create stage table
    table_name = 'med_users_master'
    table_ddl = """ CREATE TABLE IF NOT EXISTS {} (
                                    CustomerName VARCHAR(255) NOT NULL PRIMARY KEY,
                                    CustomerID	VARCHAR(18) NOT NULL,
                                    CustomerOpenDate DATE(18) NOT NULL,
                                    LastConsultedDate DATE(18),
                                    VaccinationType	 CHAR(5),
                                    DoctorConsulted	CHAR(255),
                                    State CHAR(5),
                                    Country	CHAR(5),
                                    PostCode INT(5),
                                    DateofBirth	DATE(8),
                                    ActiveCustomer CHAR(1),
                                    Inserted_on DATE(18) NOT NULL
                                    ); """.format(table_name)
    
    if conn is not None:
        # create stage table
        try:
            run_query(conn,table_ddl)
            print("Stage Table => ",table_name)
            print("================================================")
        except Error as e:
            print(e)
            
    else:
        print("Error! cannot create the database connection.")
    
    # ETL starts by reading input files
    read_files(table_name,path,conn)
    transformAndLoad_into_target(table_name,conn,table_ddl)
    validate(conn,table_name)
    conn.close()
    print("==========================================================")

if __name__ == '__main__':
    main()