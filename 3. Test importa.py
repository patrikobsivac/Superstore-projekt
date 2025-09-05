import unittest
import pandas as pd
import sqlalchemy
from pandas.testing import assert_frame_equal

"""
3. Skripta za testiranje importa u bazu podataka

Testira se da li su kolone i podaci u bazi isti kao u predprocesiranom CSV fajlu.
"""

class TestDatabaseImport(unittest.TestCase):
    def setUp(self):
        # Konekcija na bazu
        self.engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/superstore')
        self.connection = self.engine.connect()

        # Učitavanje predprocesiranog CSV datoteke
        self.df = pd.read_csv("superstore_80_processed.csv")

        # Prilagodite SELECT upit da odgovara bazi i tablicama
        query = """
        SELECT 
            o.id,
            o.order_date,
            o.ship_date,
            o.order_priority,
            o.ship_mode,
            o.discount,
            o.profit,
            o.quantity,
            o.sales,
            o.shipping_cost,
            c.customer_name,
            p.product_name,
            cat.category_name,
            sub.sub_category_name,
            l.region_name,
            l.city,
            l.country,
            l.state,
            m.market_value,
            s.segment_type
        FROM orders o
        JOIN customer c ON o.customer_id = c.id
        JOIN product p ON o.product_id = p.id
        JOIN category cat ON o.category_id = cat.id
        JOIN subCategory sub ON p.subCategory_id = sub.id
        JOIN location l ON o.location_id = l.id
        JOIN market m ON c.market_id = m.id
        JOIN segment s ON c.segment_id = s.id
        ORDER BY o.id ASC
        """

        result = self.connection.execute(query)
        self.db_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        self.df['Order_Date'] = pd.to_datetime(self.df['Order_Date'])
        self.df['Ship_Date'] = pd.to_datetime(self.df['Ship_Date'])

        # Sortirajte oba DataFrame-a po istom ključu za usporedba
        self.df = self.df.sort_values(by=['Order_Date', 'Customer_Name', 'Product_Name']).reset_index(drop=True)
        self.db_df = self.db_df.sort_values(by=['order_date', 'customer_name', 'product_name']).reset_index(drop=True)
        self.df.rename(columns={
            'Order_Date': 'order_date',
            'Ship_Date': 'ship_date',
            'Order_Priority': 'order_priority',
            'Ship_Mode': 'ship_mode',
            'Discount': 'discount',
            'Profit': 'profit',
            'Quantity': 'quantity',
            'Sales': 'sales',
            'Shipping_Cost': 'shipping_cost',
            'Customer_Name': 'customer_name',
            'Product_Name': 'product_name',
            'Category': 'category_name',
            'Sub_Category': 'sub_category_name',
            'Region': 'region_name',
            'City': 'city',
            'Country': 'country',
            'State': 'state',
            'Market': 'market_value',
            'Segment': 'segment_type'
        }, inplace=True)

    def test_columns(self):
        # Testira da li su kolone iste
        self.assertListEqual(list(self.df.columns), list(self.db_df.columns))

    def test_data(self):
        # Testira da li su podaci isti
        assert_frame_equal(self.df, self.db_df, check_dtype=False)

    def tearDown(self):
        self.connection.close()

if __name__ == '__main__':
    unittest.main()
