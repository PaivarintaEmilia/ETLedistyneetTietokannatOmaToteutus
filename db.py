import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@contextlib.contextmanager
def get_db(cnx_type = 'oltp'):
    _db = None
    try:

        if cnx_type == 'oltp':
            # OLTP tietokanta
            cnx_str = 'mysql+mysqlconnector://root:@localhost/laplanduas_rental'
        else:
            # OLAP tietokanta
            cnx_str = 'mysql+mysqlconnector://root:@localhost/rental_db_olap'
        engine = create_engine(cnx_str)
        db_session = sessionmaker(bind=engine)
        _db = db_session()
        yield _db

    except Exception as e:
        print(e)

    finally:
        if _db is not None:
            # Yhteys katkaistaan täällä eikä kyselyä tehdessä
            _db.close()
