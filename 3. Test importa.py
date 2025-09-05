import unittest
import pandas as pd
from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.orm import sessionmaker

class TestDatabaseImport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.csv_file_path = "superstore_80_PROCESSED.csv"
        cls.df_csv = pd.read_csv(cls.csv_file_path, delimiter=',')

        cls.df_csv['Order_Date'] = pd.to_datetime(cls.df_csv['Order_Date']).dt.date
        cls.df_csv['Ship_Date'] = pd.to_datetime(cls.df_csv['Ship_Date']).dt.date
        cls.df_csv['Product_Name'] = cls.df_csv['Product_Name'].astype(str).str.slice(0, 255)

        cls.engine = create_engine('mysql+pymysql://root:root@localhost:3306/superstore', echo=False)
        cls.connection = cls.engine.connect()
        cls.metadata = MetaData()
        cls.metadata.reflect(bind=cls.engine)

        Session = sessionmaker(bind=cls.engine)
        cls.session = Session()

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        cls.connection.close()

    def test_01_column_names_orders(self):
        orders_table = self.metadata.tables.get('orders')
        self.assertIsNotNone(orders_table, "Tablica 'orders' ne postoji u bazi podataka.")
        db_columns = [col.name for col in orders_table.columns]

        expected_columns = [
            'id', 'order_date', 'ship_date', 'order_priority', 'ship_mode',
            'discount', 'profit', 'quantity', 'sales', 'shipping_cost',
            'category_id', 'customer_id', 'product_id', 'location_id'
        ]

        self.assertCountEqual(db_columns, expected_columns,
                              "Nazivi stupaca u tablici 'orders' se ne podudaraju s očekivanim.")

    def test_02_column_names_related_tables(self):
        expected_tables_columns = {
            'category': ['id', 'category_name'],
            'subcategory': ['id', 'sub_category_name'],
            'market': ['id', 'market_value'],
            'segment': ['id', 'segment_type'],
            'customer': ['id', 'customer_name', 'market_id', 'segment_id'],
            'location': ['id', 'region_name', 'city', 'country', 'state'],
            'product': ['id', 'product_name', 'category_id', 'subCategory_id']  # VELIKO 'C'
        }

        for table_name, expected_cols in expected_tables_columns.items():
            table = self.metadata.tables.get(table_name)
            self.assertIsNotNone(table, f"Tablica '{table_name}' ne postoji u bazi podataka.")
            db_columns = [col.name for col in table.columns]
            self.assertCountEqual(db_columns, expected_cols,
                                  f"Nazivi stupaca u tablici '{table_name}' se ne podudaraju s očekivanim.")

def test_03_data_integrity(self):
    category = self.metadata.tables['category']
    subcategory = self.metadata.tables['subcategory']
    market = self.metadata.tables['market']
    segment = self.metadata.tables['segment']
    customer = self.metadata.tables['customer']
    location = self.metadata.tables['location']
    product = self.metadata.tables['product']
    orders = self.metadata.tables['orders']

    join_stmt = orders.join(category, orders.c.category_id == category.c.id) \
                      .join(customer, orders.c.customer_id == customer.c.id) \
                      .join(product, orders.c.product_id == product.c.id) \
                      .join(location, orders.c.location_id == location.c.id) \
                      .join(market, customer.c.market_id == market.c.id) \
                      .join(segment, customer.c.segment_id == segment.c.id) \
                      .join(subcategory, product.c.subCategory_id == subcategory.c.id)

    sel = select(
        category.c.category_name.label('Category'),
        location.c.city.label('City'),
        location.c.country.label('Country'),
        customer.c.customer_name.label('Customer_Name'),
        market.c.market_value.label('Market'),
        orders.c.order_date.label('Order_Date'),
        orders.c.ship_date.label('Ship_Date'),
        orders.c.order_priority.label('Order_Priority'),
        product.c.product_name.label('Product_Name'),
        location.c.region_name.label('Region'),
        segment.c.segment_type.label('Segment'),
        orders.c.ship_mode.label('Ship_Mode'),
        location.c.state.label('State'),
        subcategory.c.sub_category_name.label('Sub_Category'),
        orders.c.discount.label('Discount'),
        orders.c.profit.label('Profit'),
        orders.c.quantity.label('Quantity'),
        orders.c.sales.label('Sales'),
        orders.c.shipping_cost.label('Shipping_Cost')
    ).select_from(join_stmt)

    result = self.connection.execute(sel)
    rows = result.fetchall()
    df_db = pd.DataFrame(rows, columns=result.keys())

    df_db['Order_Date'] = pd.to_datetime(df_db['Order_Date']).dt.date
    df_db['Ship_Date'] = pd.to_datetime(df_db['Ship_Date']).dt.date
    df_db['Product_Name'] = df_db['Product_Name'].astype(str).str.slice(0, 255)

    sort_cols = ['Order_Date', 'Customer_Name', 'Product_Name', 'Sales']
    df_csv_sorted = self.df_csv.sort_values(by=sort_cols).reset_index(drop=True)
    df_db_sorted = df_db.sort_values(by=sort_cols).reset_index(drop=True)

    self.assertEqual(df_csv_sorted.shape, df_db_sorted.shape,
                     "Dimenzije CSV i baze podataka se ne podudaraju.")

    try:
        pd.testing.assert_frame_equal(df_csv_sorted, df_db_sorted,
                                      check_dtype=False, check_exact=False, rtol=1e-4, atol=1e-4)
    except AssertionError as e:
        self.fail(f"Podaci u bazi i CSV-u se ne podudaraju: {e}")

if __name__ == '__main__':
    unittest.main()
