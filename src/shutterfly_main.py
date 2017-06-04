'''
ShutterFly Data Challenge.

Program to Ingest and Process Shutterfly Events data and to find top X customers by life time value

Current Implementation: Events are consumed from a single input source and processed record by record based on type
                        and placed into database record by record ,further referential integrity is enforced while 
                        lading records to database to ensure that only for existing customers the events are loaded to 
                        orders,site visits,Images tables.
                        After the Ingestion of events,Customer life time value is calculated and maintained in analytics 
                        table.This table is used for querying the top X customers by LTV.    
                        
                        
Further improvements:   Events can be processed in parallel by different threads.The database should be able to
                        accept and cater to such multi connections.Referential integrity constraint on Customer Id 
                        should not be enforced at database level. It must be taken care by the system providing the 
                        data.
                        While building analytics table. Delta update can be done only to update records for which
                        customer activity happened recently.
                        
Procedure call structure

main()
    |-Ingest()- create_tables()
    |           customer_ingest()
    |           sitevisit_ingest()
    |           imageuploded_ingest()
    |           order_ingest()
    |           build_LTV_table()
    |
    |-TopXSimpleLTVCustomers()
                                  

INPUT_FILE_PATH= '.\input\input.txt'   -Contains Events
OUTPUT_FILE_PATH='.\output\output.txt' -Contains Customers and their LTV values 

Python version: Python 2.7
sqlite version:'3.8.11'
Logging version:'0.5.1.2'
json version   :'2.0.9'

'''
import json
import datetime
import sqlite3
import logging

#These variables can be used for configuration purposes
INPUT_FILE_PATH= '.\input\input.txt'
OUTPUT_FILE_PATH='.\output\output.txt'
TOP_N = 3

def create_tables(db_cursor):
    """
    :param db_cursor: This holds the cursor for the data base 
    :return: none
    :This module creates the base tables(Customer,SiteVisit,ImageUploaded,Orders,customer_LTV) that are used to store the 
    :event and analytical details.
    :Schema:
            Customer Table:
                            customer_id  TEXT PRIMARY KEY ,
                            event_time DATE, 
                            last_name TEXT,
                            adr_city TEXT, 
                            adr_state TEXT  
            
            SiteVisit Table:            
                            page_id TEXT PRIMARY KEY ,
                            event_time DATE,
                            customer_id TEXT,
                            tags TEXT, FOREIGN KEY(customer_id) REFERENCES customer(customer_id)
                            
            Orders Table:                            order_id TEXT PRIMARY KEY ,
                            event_time DATE,
                            customer_id TEXT,
                            total_amount REAL,FOREIGN KEY(customer_id) REFERENCES Customer(customer_id))
            
            ImageUploaded Table:
                            image_id TEXT PRIMARY KEY ,
                            event_time DATE,
                            customer_id TEXT,
                            camera_make TEXT,
                            camera_model TEXT, FOREIGN KEY(customer_id) REFERENCES customer(customer_id))
            
            customer_LTV:
                            Customer_id TEXT PRIMARY KEY,
                            NoofVisits INTEGER,
                            total_expenditure REAL,
                            ExpenditurePerVisit REAL,
                            SiteVisitsPerWeek INTEGER,
                            NoOfWeeks REAL,CLV REAL
                            
                            
    """
    try:
        logging.info('Table creation - Started')
        db_cursor.execute(
        '''CREATE TABLE Customer (customer_id  TEXT PRIMARY KEY , event_time DATE, last_name TEXT, adr_city TEXT, adr_state TEXT)''')
        db_cursor.execute(
        '''CREATE TABLE SiteVisit (page_id TEXT PRIMARY KEY , event_time DATE, customer_id TEXT, tags TEXT, FOREIGN KEY(customer_id) REFERENCES customer(customer_id))''')
        db_cursor.execute(
        '''CREATE TABLE ImageUploaded(image_id TEXT PRIMARY KEY , event_time DATE, customer_id TEXT, camera_make TEXT, camera_model TEXT, FOREIGN KEY(customer_id) REFERENCES customer(customer_id))''')
        db_cursor.execute(
        '''CREATE TABLE Orders (order_id TEXT PRIMARY KEY , event_time DATE, customer_id TEXT, total_amount REAL, FOREIGN KEY(customer_id) REFERENCES Customer(customer_id))''')
        db_cursor.execute(
            '''CREATE table customer_LTV (Customer_id TEXT PRIMARY KEY,NoofVisits INTEGER,total_expenditure REAL,ExpenditurePerVisit REAL, SiteVisitsPerWeek INTEGER,NoOfWeeks REAL,CLV REAL)''')
        logging.info('Table creation - Closed')
    except Exception as ex:
        warning_string='Unable to create tables - '+str(ex)
        logging.CRITICAL(warning_string)

def customer_ingest(db_cursor, customer_json, verb):
    """
    This module ingests the customer events in to the Customer table.
    For NEW record a new customer record will be inserted
    For UPDATE record the old one will be modified 
    Assumption: update record contains change only in the last name ,address and timestamp.
                Rest of the values remain same.
                and database is expected to hold only the latest customer event records including old extra information
     
    :param db_cursor: Contains cursor to the in memory database
    :param customer_json:Contains the Customer record 
    :param verb: Contains the record type-NEW/UPDATE 
    :return:none
    """
    try:
        if(verb=='NEW'):

            db_cursor.execute('INSERT INTO customer VALUES (?,?,?,?,?)', (
                customer_json['key'], customer_json['event_time'], customer_json['last_name'], customer_json['adr_city'],
            customer_json['adr_state']))

        elif(verb=='UPDATE'):

            query = "Update customer SET "

            for key, value in customer_json.items():
                if (value != '' and key != 'type' and key != 'verb' and key != 'key'):
                    query = query + str(key) + "= '" + value + "',"

            query = query[:-1]
            query = query + ' where customer_id="'+customer_json['key']+'"'
            db_cursor.execute(query)

    except Exception as e:
        warning_string='Unable to insert the customer record- '+customer_json['key']+'-'+str(e)
        logging.warning(warning_string)

def sitevisit_ingest(db_cursor, sitevisit_json):
    """
    This module ingests the site visits events into the SiteVisit table ,the customer need to be present 
    in the customer table to be inserted into Site Visits table.If the customer is not there then that
    record will be logged  and the execution proceed to next record.
    
    :param db_cursor: Contains cursor to the in memory database
    :param sitevisit_json: Contains the site Visits record
    :return: none
    """
    try:
        db_cursor.execute('INSERT INTO SiteVisit VALUES (?,?,?,?)', (sitevisit_json['key'],sitevisit_json['event_time'], sitevisit_json['customer_id'], json.dumps(str(sitevisit_json['tags']))))
    except Exception as e:
        warning_string='Unable to insert the SiteVisit record- '+ sitevisit_json['key']+'-'+str(e)
        logging.warning(warning_string)

def imageuploded_ingest(db_cursor, imageuploaded_json):
    """
    This module ingests the image events in to the table ,the customer need to be present in the customer table
    to be inserted into image table.If the customer is not there then that record will be logged  and the 
    execution proceed to next record.
    
        
    :param db_cursor: Contains cursor to the in memory database
    :param imageuploaded_json: Contains the image record
    :return: none
    """
    try:
        db_cursor.execute('INSERT INTO ImageUploaded VALUES (?,?,?,?,?)', (imageuploaded_json['key'], imageuploaded_json['event_time'], imageuploaded_json['customer_id'], imageuploaded_json['camera_make'], imageuploaded_json['camera_model']))
    except Exception as e:
        warning_string = 'Unable to insert the image record- ' + imageuploaded_json['key']+'-'+str(e)
        logging.warning(warning_string)

def order_ingest(db_cursor, orders_json, verb):
    """
    This module ingests the Order events in to the table ,the customer need to be present in the customer table
    to be inserted into orders table.If the customer is not there then that record will be logged  and the 
    execution proceed to next record.
    
    For NEW record a new record will be inserted
    For UPDATE record the old one will be modified 
    Assumption: update record contains change  only in the amount and time stamp.Rest of the values remain same.
                and database is expected to hold only the latest event records
     
    :param db_cursor: Contains cursor to the in memory database
    :param orders_json: Contains the order record
    :param verb: Contains the record type-NEW/UPDATE
    :return: none
    """
    try:
        if(verb=='NEW'):
            db_cursor.execute('INSERT INTO orders VALUES (?,?,?,?)', (orders_json['key'], orders_json['event_time'], orders_json['customer_id'], orders_json['total_amount']))
        elif(verb=='UPDATE'):
            query = "Update orders SET "

            for key, value in orders_json.items():
                if (value != '' and key != 'type' and key != 'verb' and key != 'key' and key!='customer_id'):
                    query = query + str(key) + "= '" + value + "',"

            query = query[:-1]
            query = query + ' where order_id="' + orders_json['key'] + '"'
            db_cursor.execute(query)

    except Exception as e:
        warning_string = 'Unable to insert the order record- ' + orders_json['key']+'-'+str(e)
        logging.warning(warning_string)

def build_LTV_table(db_cursor):
    """
    This module calculates the metrics like total expenditure,number of visits,site visits per week,
    expenditure per visit and Customer Life Time Value for each of the customer and loads the analytics table 
    -"customer_LTV"
    
    :param db_cursor:a cursor for the already opened database having all the events 
    :return:none
    """
    distinct_Customers = []#list to hold distinct customers
    for row in db_cursor.execute('Select distinct(customer_id) from customer'):
        distinct_Customers.append(row[0])

    logging.info('Building  Analytics table-Started')


    #This loop calculates the Life time value for each customer
    # loops for each customer
    for Current_Customer in distinct_Customers:
        #query to extract total number of site visits
        for row_customer in db_cursor.execute('Select count(*) from SiteVisit where customer_id="%s"' % Current_Customer):
            NoOfVisits = row_customer[0]
        #query to extract sum of the order amounts
        for row_orders in db_cursor.execute('Select sum(total_amount) from Orders where customer_id="%s"' % Current_Customer):
            total_expenditure = row_orders[0]
        #Query to extract number of weeks:
        #Minimum and maximum functions on (site visited)event date is used to find
        #when a customer started  and last used site website.By taking difference
        #of min and max ,number of days is obtained and later converted
        #to number of weeks by dividing by 7.
        #
        for row_week in db_cursor.execute('SELECT (julianday(max(event_time)) - julianday(min(event_time)))/ 7 FROM SiteVisit where customer_id="%s"' % Current_Customer):
            NoOfWeeks = row_week[0]

        #If condition   :prevent further calculation if site visits is zero
        #               :proceeds only for users who have site visits
        # for shutter fly, average customers lifetime is 10 years
        # Customer Life time value=52*(expenditure per visit * site visits per week) * average customers lifetime

        if (NoOfVisits != 0 and NoOfWeeks != float(0) and (type(total_expenditure)==float)):

            try:
                expenditure_pervisit = total_expenditure / NoOfVisits
                site_visits_perweek = NoOfVisits / NoOfWeeks
                shutterfly_average_customer_lifespan=10
                CLV = (52*(int(expenditure_pervisit)) * site_visits_perweek) * shutterfly_average_customer_lifespan
                db_cursor.execute("INSERT INTO customer_LTV values(?,?,?,?,?,?,?)", (
                Current_Customer, NoOfVisits, total_expenditure, expenditure_pervisit, site_visits_perweek, NoOfWeeks,
                CLV))

            except Exception as ex:
                logging.critical(str(ex))
    logging.info('Building  Analytics table-Ended')

def TopXSimpleLTVCustomers(top_n, db_cursor):
    """
    This module uses already built "customer_LTV" data table and queries for top N customers and their LTVs.
    :param top_n: 
    :param db_cursor: contains cursor to the in memory database
    :return: A json file containing top 10 customers and their Life time values
    """
    logging.info('Calculation of top n customers by LTV- Started')
    try:
        json_data = json.dumps({})
        customer_LTV_list=[]
        for row_top in db_cursor.execute("select customer_id,CLV from customer_LTV order by CLV desc limit '%s'" %top_n) :
            customer_LTV_list.append(row_top)

        json_data=json.dumps(customer_LTV_list)
        logging.info('Calculation of top n customers by LTV- Ended')
        return json_data

    except Exception as ex:
        logging.critical(str(ex))

def Ingest(events, db_cursor):
    """
    :param events:contains json having events 
    :param db_cursor: contains cursor to the in memory database
    :return: none
    
    This module classifies and ingests the events data  into 4 tables (Customer,Orders,SiteVisit,ImageUploaded) using customer_ingest(),
    sitevisit_ingest(),imageuploded_ingest(),order_ingest() modules respectively and 
    builds analytical table -customer_LTV by calling build_LTV_table() module. 
    """

    try:
        create_tables(db_cursor)

        logging.info('Ingestion of Events started')

        with open(events) as fp:
            event_dict = json.load(fp)
            #for each event appropriate insert/update statements are called through different functions
            for i in range(0, len(event_dict)):
                if (event_dict[i].get('type') == 'CUSTOMER'):
                    customer_ingest(db_cursor,event_dict[i], event_dict[i]['verb'])

                elif (event_dict[i].get('type') == 'SITE_VISIT'):
                    sitevisit_ingest(db_cursor,event_dict[i])

                elif (event_dict[i].get('type') == 'IMAGE'):
                    imageuploded_ingest(db_cursor,event_dict[i])

                elif (event_dict[i].get('type') == 'ORDER'):
                    order_ingest(db_cursor,event_dict[i], event_dict[i]['verb'])

        logging.info('Ingestion of Events Ended')

        #This module builds analytics table containing Customer LTV and other metrics
        build_LTV_table(db_cursor)

    except Exception as ex:
        logging.critical('Exiting the program...' + str(ex))
        exit(100)

def main():
    """
    This is the entry point for the program.
    
    For storage purposes an in memory database is used (library-SQLITE).Referential integrity constrains(primary and foreign key)
    are enforced to ensure that events such as orders,site visits,image uploads happen only for existing customer.
    
    Logging is taken care by python Logging library with different criticality levels INFO,DEBUG,WARNING,CRITICAL
    1.For CRITICAL errors such as 'Unable to connect to database','unable to load files' the program  
    exits with error code 100 and appropriate error messages. 
    2.For Warning errors such as 'Unable to insert record in to table, because of Foreign key constraint'.The program
    will log appropriate error messages and continues execution of next steps.    
     
    """

    try:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    except Exception as ex:
        print str(ex)

    try:
        #in memory database
        in_memory_db = ':memory:'

        #for persistent storage use , in_memory_db='shutterfly.db'

        conn = sqlite3.connect(in_memory_db)
        #Enable referential integrity constraint .By default it will be disabled
        conn.execute('pragma foreign_keys = 1')
        db_cursor = conn.cursor()
        logging.info('Database Connection Established')

    except Exception as ex:
        #Exit program saying
        logging.critical('Unable to make connection to Database')
        logging.critical('Exiting the program...'+str(ex))
        exit(100)

    #cursor to in memory database
    D = db_cursor

    #location of input file
    e = INPUT_FILE_PATH

    #Top X customers by life time value
    x = TOP_N

    try:
        #Module to ingest the event into database
        Ingest(e, D)

        #Module to fetch the top x number of customers by lifetime value
        #returns dictionary with customer and LTV values
        topX_LTV_customers=TopXSimpleLTVCustomers(x, D)
        print topX_LTV_customers

        # After reading and writing from tables the database is is committed
        conn.commit()
        # Closing the connection to database
        conn.close()
        logging.info('Database Connection Closed')

        #output file to load the top X number of customers by Life time value
        output_file = open(OUTPUT_FILE_PATH, 'wb')
        output_file.write(topX_LTV_customers)
        output_file.close

    except Exception as ex:

        logging.critical('Exiting the program...'+str(ex))
        #if any exception occur:
        #   closing the connection to database to prevent it from getting locked for other users
        #   without committing data tables
        conn.close()
        logging.info('Database Connection Closed')
        exit(100)


if __name__ == "__main__":
    main()


