import sys
import os
sys.path.append(os.environ["PROJECT_HOME"])
from fake_data import mockData
from postgreslib.create_tables import create_tables


if __name__ == "__main__":

    if "joe" in os.environ["HOME"]:
        db = "test_database"
    else:
        db = "postgres_rds"

    mocker = mockData("test_database")
    create_tables(mocker.dbc)
    print("tables created on {}".format(db))

    print("opened mocker")
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

    mocker.dbc.close_connection()
