#! usr/bin/env python3
from postgreslib.create_tables import create_tables
from postgreslib.queries import get_base_table_descriptions
from postgreslib.postgres_cursor import get_connection,get_cursor, close_cursor, close_connection, commit_connection
from datetime import datetime,timedelta
import random as rd
from fake_data import mockData, random_sales_order,random_purchase_order

def daterange(start_date,end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def main():
    mocker = mockData()
    now = datetime.utcnow()
    year = 365
    interval = timedelta(days = 365)
    start = now - interval
    sales_orders_per_day =  1000
    purchase_orders_per_day =  500
    first = True
    get_cursor()
    for i, day in enumerate(daterange(start,now)):
        for o in range(sales_orders_per_day):
            mocker.current_table = "sales_orders"
            sales_order_record = random_sales_order(order_creation_date = day)
            stmt = mocker.generate_insert_statement(**sales_order_record)
            mocker.insert_statements.append(stmt)

        for o in range(purchase_orders_per_day):
            mocker.current_table = "purchase_orders"
            purchase_order_record = random_purchase_order(order_creation_date = day)
            stmt = mocker.generate_insert_statement(**purchase_order_record)
            mocker.insert_statements.append(stmt)

        if i % 10 == 0:
            print ("on day {}".format(day.strftime("%d/%m/%y")))
            if first:
                first = False
            else:
                print( "running {} inserts".format(len(mocker.insert_statements)))
                mocker.run_insert_statements()
    rem = len(mocker.insert_statements)
    if rem > 0:
        print("{} inserts left".format(rem))
        mocker.run_insert_statements()
        commit_connection()
    close_cursor()
    close_connection()

if __name__ == '__main__':
    get_base_table_descriptions()
    main()
