import os

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, scoped_session, sessionmaker

from etc.config import config

db_url = f"postgresql://{config['DB']['USER']}:{os.getenv('PGPASSWORD', config['DB']['PASS'])}@{config['DB']['HOST']}:{config['DB']['PORT']}/{config['DB']['NAME']}"

conn_str = f"host={config['DB']['HOST']} port={config['DB']['PORT']} dbname={config['DB']['NAME']} user={config['DB']['USER']} password={config['DB']['PASS']}"
conn = psycopg2.connect(conn_str)
curr = conn.cursor()

engine = create_engine(
    db_url,
    echo=config['DB']['ECHO'] and config['DB']['ECHO'] == 'True',
    isolation_level=config['DB']['ISOLATION_LEVEL'],
    future=True,
)

Session = scoped_session(
    sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=True,
        future=True,
    )
)


class _Base:
    def save(self):
        Session.add(self)
        Session.commit()

    def delete(self):
        Session.delete(self)
        Session.commit()

    def to_dict(self):
        return {key: value for key, value in vars(self).items() if not key.startswith('_sa_')}

    def __repr__(self):
        try:
            params = ', '.join(f'{attr}={getattr(self, attr)!r}' for attr in getattr(self, '_repr_'))
            return f'{self.__class__.__name__}({params})'
        except AttributeError:
            return object.__repr__(self)


Base = declarative_base(cls=_Base)
