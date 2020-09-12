from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class SQLAlchemy:

    def __init__(self, url):
        self.url = url

        self._engine = None
        self._session = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self.url)
        return self._engine

    @property
    def session(self):
        if self._session is None:
            session_maker = sessionmaker(bind=self.engine)
            self._session = session_maker()
        return self._session

    def close(self):
        if self._session is not None:
            self.session.close()
