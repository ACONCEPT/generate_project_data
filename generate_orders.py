#! usr/bin/env python3
from datetime import datetime, timedelta
from fake_data import random_sales_order,random_purchase_order, mockData
from postgres_cursor import get_cursor, close_cursor
from queries import get_base_table_descriptions

def daterange(start_date,end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def main():
    mocker = mockData()
    now = datetime.utcnow()
    year = 365
#    interval = timedelta(days = year *2)
    interval = timedelta(days = 2)
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
                mocker.run_insert_statements()
                get_cursor()
    close_cursor()

if __name__ == '__main__':
    get_base_table_descriptions()
    main()
