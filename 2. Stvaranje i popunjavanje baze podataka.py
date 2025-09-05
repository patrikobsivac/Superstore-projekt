# Imports
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Date, String, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

"""
2. Skripta za stvaranje sheme baze podataka i popunjavanje tablica
Koristi se SQLAlchemy ORM za kreiranje sheme i ubacivanje podataka iz predprocesiranog CSV-a.
"""

CSV_FILE_PATH = "superstore_80_PROCESSED.csv"  # Putanja do CSV datoteke
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size: ", df.shape)
print(df.head())

Base = declarative_base()

# Definicija modela

class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    category_name = Column(String(45), nullable=False, unique=True)

class SubCategory(Base):
    __tablename__ = 'subCategory'
    id = Column(Integer, primary_key=True)
    sub_category_name = Column(String(45), nullable=False, unique=True)

class Market(Base):
    __tablename__ = 'market'
    id = Column(Integer, primary_key=True)
    market_value = Column(String(30), nullable=False, unique=True)

class Segment(Base):
    __tablename__ = 'segment'
    id = Column(Integer, primary_key=True)
    segment_type = Column(String(45), nullable=False, unique=True)

class Customer(Base):
    __tablename__ = 'customer'
    id = Column(Integer, primary_key=True)
    customer_name = Column(String(60), nullable=False, unique=True)
    market_id = Column(Integer, ForeignKey('market.id'))
    segment_id = Column(Integer, ForeignKey('segment.id'))

class Location(Base):
    __tablename__ = 'location'
    id = Column(Integer, primary_key=True)
    region_name = Column(String(45), nullable=False)
    city = Column(String(60), nullable=False)
    country = Column(String(60), nullable=False)
    state = Column(String(60), nullable=False)
    __table_args__ = (
        UniqueConstraint('region_name', 'city', 'country', 'state', name='unique_location'),
    )

class Product(Base):
    __tablename__ = 'product'
    id = Column(Integer, primary_key=True)
    product_name = Column(String(255), nullable=False, unique=True)
    category_id = Column(Integer, ForeignKey('category.id'))
    subCategory_id = Column(Integer, ForeignKey('subCategory.id'))

class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True)
    order_date = Column(Date, nullable=False)
    ship_date = Column(Date, nullable=False)
    order_priority = Column(String(45), nullable=False)
    ship_mode = Column(String(45), nullable=False)
    discount = Column(Float, nullable=False)
    profit = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    sales = Column(Float, nullable=False)
    shipping_cost = Column(Float, nullable=False)
    category_id = Column(Integer, ForeignKey('category.id'))
    customer_id = Column(Integer, ForeignKey('customer.id'))
    product_id = Column(Integer, ForeignKey('product.id'))
    location_id = Column(Integer, ForeignKey('location.id'))

# Kreiranje konekcije na bazu
engine = create_engine('mysql+pymysql://root:root@localhost:3306/superstore', echo=False)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Funkcija za konverziju datuma
def convert_date(date_str):
    return datetime.fromisoformat(date_str)

# --- Popunjavanje referentnih tabela sa jedinstvenim vrednostima ---

# Kategorije
unique_categories = df['Category'].dropna().unique()
for cat in unique_categories:
    if not session.query(Category).filter_by(category_name=cat).first():
        session.add(Category(category_name=cat))

# Podkategorije
unique_subcategories = df['Sub_Category'].dropna().unique()
for subcat in unique_subcategories:
    if not session.query(SubCategory).filter_by(sub_category_name=subcat).first():
        session.add(SubCategory(sub_category_name=subcat))

# Tržišta
unique_markets = df['Market'].dropna().unique()
for market in unique_markets:
    if not session.query(Market).filter_by(market_value=market).first():
        session.add(Market(market_value=market))

# Segmenti
unique_segments = df['Segment'].dropna().unique()
for segment in unique_segments:
    if not session.query(Segment).filter_by(segment_type=segment).first():
        session.add(Segment(segment_type=segment))

session.commit()

# Učitavanje referentnih podataka u mape za brzu pretragu
category_map = {c.category_name: c.id for c in session.query(Category).all()}
subCategory_map = {s.sub_category_name: s.id for s in session.query(SubCategory).all()}
market_map = {m.market_value: m.id for m in session.query(Market).all()}
segment_map = {s.segment_type: s.id for s in session.query(Segment).all()}

# Kupci
unique_customers = df[['Customer_Name', 'Market', 'Segment']].drop_duplicates()
for _, row in unique_customers.iterrows():
    if not session.query(Customer).filter_by(customer_name=row['Customer_Name']).first():
        session.add(Customer(
            customer_name=row['Customer_Name'],
            market_id=market_map.get(row['Market']),
            segment_id=segment_map.get(row['Segment'])
        ))
session.commit()
customer_map = {c.customer_name: c.id for c in session.query(Customer).all()}

# Lokacije
unique_locations = df[['Region', 'City', 'Country', 'State']].drop_duplicates()
for _, row in unique_locations.iterrows():
    if not session.query(Location).filter_by(
        region_name=row['Region'],
        city=row['City'],
        country=row['Country'],
        state=row['State']
    ).first():
        session.add(Location(
            region_name=row['Region'],
            city=row['City'],
            country=row['Country'],
            state=row['State']
        ))
session.commit()
location_map = {
    (loc.region_name, loc.city, loc.country, loc.state): loc.id
    for loc in session.query(Location).all()
}

# Proizvodi
unique_products = df[['Product_Name', 'Category', 'Sub_Category']].drop_duplicates()
for _, row in unique_products.iterrows():
    if not session.query(Product).filter_by(product_name=row['Product_Name']).first():
        session.add(Product(
            product_name=row['Product_Name'],
            category_id=category_map.get(row['Category']),
            subCategory_id=subCategory_map.get(row['Sub_Category'])
        ))
session.commit()
product_map = {p.product_name: p.id for p in session.query(Product).all()}

# --- Popunjavanje tabele narudžbina ---

for _, row in df.iterrows():
    order = Order(
        order_date=convert_date(row['Order_Date']),
        ship_date=convert_date(row['Ship_Date']),
        order_priority=row['Order_Priority'],
        ship_mode=row['Ship_Mode'],
        discount=row['Discount'],
        profit=row['Profit'],
        quantity=int(row['Quantity']),
        sales=row['Sales'],
        shipping_cost=row['Shipping_Cost'],
        category_id=category_map.get(row['Category']),
        customer_id=customer_map.get(row['Customer_Name']),
        product_id=product_map.get(row['Product_Name']),
        location_id=location_map.get((row['Region'], row['City'], row['Country'], row['State']))
    )
    session.add(order)

session.commit()
session.close()

print("Uspješno kreirana shema i popunjene tablice.")
