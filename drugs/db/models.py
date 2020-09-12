from sqlalchemy import Column, Integer
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class OzDrug(Base):
    __tablename__ = 'oz_drug'

    id = Column(Integer, primary_key=True)
    data = Column(JSON)
