from sqlalchemy import create_engine, text
import unittest
import pandas as pd

DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/superstore"

class TestDatabaseImport(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(DATABASE_URL)
        self.connection = self.engine.connect()

        query_string = """
        SELECT
            Global_Orders_ID,
            Order_Date,
            Ship_Date,
            Order_Priority,
            Ship_Mode,
            Discount,
            Profit,
            Quantity,
            Sales,
            Shipping_Cost,
            Customer_Name,
            Product_Name,
            Category,
            Sub_Category,
            Region,
            City,
            Country,
            State,
            Market,
            Segment
        FROM orders
        ORDER BY Global_Orders_ID ASC
        """

        query = text(query_string)
        self.result_df = pd.read_sql(query, self.connection)
        self.expected_df = pd.read_csv("/superstore_80_PROCESSED.csv")

    def tearDown(self):
        self.connection.close()
        self.engine.dispose()

    def test_columns(self):
        self.assertListEqual(list(self.result_df.columns), list(self.expected_df.columns))

    def test_data(self):
        pd.testing.assert_frame_equal(self.result_df, self.expected_df)

if __name__ == '__main__':
    unittest.main()
