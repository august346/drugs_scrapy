from sqlalchemy import Column, Integer, Text, Float, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class OzDrug(Base):
    __tablename__ = 'oz_drug'

    id = Column(Integer, primary_key=True)
    data = Column(JSON)


class AsnaDrug(Base):
    __tablename__ = 'asna_drug'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(Text)
    info = Column(JSON)
    instructions = Column(JSON)
    price = Column(Float)
    images = Column(JSON)
    is_receipt = Column(Boolean)
