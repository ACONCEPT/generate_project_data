from postgreslib.create_tables import create_tables
from postgreslib.queries import get_base_table_descriptions
from postgreslib.postgres_cursor import get_connection,get_cursor, close_cursor, close_connection

from fake_data import mockData

if __name__ == "__main__":
    print("starting main")
    create_tables()
    print("tables created")
    mocker = mockData()
    print("opened mocker")
    get_connection()
    get_cursor()
    get_base_table_descriptions()
    mocker.commands = ["sites","parts","customers","suppliers"]
    print("setting quantities")
    mocker.set_quantity("sites",10)
    mocker.set_quantity("parts",50)
    mocker.set_quantity("customers",20)
    mocker.set_quantity("suppliers",15)
    print("mocking data from config")
    mocker.mock_data_from_config()
    print("running inserts")
    mocker.run_insert_statements()
    print("generating partcust")
    mocker.part_customer_data()
    print("generating supplier")
    mocker.part_supplier_data()
    print("running inserts")
    mocker.run_insert_statements()
    close_cursor()
    close_connection()

