
Problem definition : This can be checked in Data Engineering asessment pdf file.


How to Run =  python3 <file_name>.py <environment> <config_file>.conf


Approach -

Single working ETL script written in python which can be scheduled daily that has following features:

	Note : Assuming no duplicate files are arriving in source location server.
		   Sqlite is used for easy development
	
	1. Read files from source location
	2. Dynamically create stage table and insert into stage table 
	3. Dynamically creates target tables and generate insert scripts and load into tables.
	4. Test driven ETL development you can switch between DEV and PROD environment easily using config file.
	5. Validate counts between stage and target table inorder to ensure daily ingestion is completed.
	6. Everyday logs are saved in log folder.
	
Tests and functionalities -


	1. Read data from files
		- Checks if source location is empty
		- Checks files are present but no data present in files
		- Reads different formats (json,csv,txt,xml,excel) and dditional formats can be added(sql,s3,file server)
		- Once data read in dataframes datacount can be checked for validation
		
	2. Insert into stage table 
		stage table : med_users_master (single stage table for worldwide hospital customers)
		- Checks if read operation successful
		- schema is defined and ddl can be generated dynamically for all tables.
		- Validates columns and datatypes and then inserted into stage table
		
	3. Transform and load into target tables
		- Checks if insert into stage table completed
		- Based on unique countries that present in stage tables , tables are created
		- Insert queries are generated on latest date and loaded into target tables
		
	4. Validation
		- This funtion checks count from stage table on job run date by country 
		and compare with target table counts to ensure ETL process successful.
		- We can maintain seperate table for daily runs.
		
	5. Logging (not completed due to busy schedule)
		- In order to check job logs incase of failure, everyday logs are saved on logs folder
		- 
	6. configuration (not completed due to busy schedule)
	
		-You just need use config file to switch between UAT , DEV, PROD environments.
		- add environment , connection string , location , stage_table name , schema info 
		
		

