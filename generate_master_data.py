from postgres_cursor import get_conn_string
from create_tables import create_tables
from fake_data import mockData

if __name__ == "__main__":
    conn_string = get_conn_string()
    create_tables(conn_string)
    mocker = mockData()
    mocker.commands = ["sites","parts","customers","suppliers"]
    mocker.set_quantity("sites",10)
    mocker.set_quantity("parts",50)
    mocker.set_quantity("customers",20)
    mocker.set_quantity("suppliers",15)
    mocker.mock_data_from_config()
    mocker.run_insert_statements()
    mocker.part_customer_data()
    mocker.part_supplier_data()
    mocker.run_insert_statements()
