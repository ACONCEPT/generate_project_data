#! usr/bin/env python3
from datetime import datetime, timedelta
from fake_data import random_sales_order,random_purchase_order, mockData
from postgres_cursor import get_cursor, close_cursor

def daterange(start_date,end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def main():
    get_cursor()
    mocker = mockData()
    now = datetime.utcnow()
    year = 365
    interval = timedelta(days = year *2)
    start = now - interval
    sales_orders_per_day =  1000
    purchase_orders_per_day =  500
    first = True
    for i, day in enumerate(daterange(start,now)):
        for o in range(sales_orders_per_day):
            mocker.insert_statements.append(random_sales_order(order_creation_date = day))
        for o in range(purchase_orders_per_day):
            mocker.insert_statements.append(random_purchase_order(order_creation_date = day))
        if i % 10 == 0:
            print ("on day {}".format(day.strftime("%d/%m/%y")))
            if first:
                mocker.run_insert_statements()
                first = False
    close_cursor()

if __name__ == '__main__':
	main()
