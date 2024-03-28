# FUNKTIOT DATE_DIM-TAULUN DATAN HAKEMISEEN JA TAULUN TÄYTTÖÖN
from sqlalchemy import text

from db import get_db


# Haetaan created_at data OLTP-tietokannan taulusta rental_transactions
def _get_transactions_dates(_db):
    _query = text('SELECT DISTINCT created_at AS day_data FROM rental_transactions')

    rows = _db.execute(_query)

    dates_rows = rows.mappings().all()

    dates = []

    for row in dates_rows:
        dates.append(row['day_data'])

    return dates

# Haetaan created_at data OLTP-tietokannan taulusta rental_items
def _get_rental_items_dates(_db):
    _query = text('SELECT DISTINCT created_at AS day_data FROM rental_items') # DISTINCT päällekkäisyyksien varalta

    rows = _db.execute(_query)

    dates_rows = rows.mappings().all()

    dates = []

    for row in dates_rows:
        dates.append(row['day_data'])

    return dates


# Funktio datan poistoa varten, kun ohjelmaa ajetaan useasti
def _clear_dates(_dw):
    _dw.execute(text('DELETE * FROM date_dim'))
    _dw.commit()


# Datan siirto date_dim tauluun
def date_dim_etl():

    with get_db() as _db:

        # Haetaan päivämäärät, jotka haettiin OLTP-tietokannasta
        transactions_dates = _get_transactions_dates(_db)
        rental_items_dates = _get_rental_items_dates(_db)

        # Yhddistetään kerätty data
        all_dates = transactions_dates + rental_items_dates
        # Tarkistus itelle
        print("############## all_dates", len(all_dates))

        # Ensin tehdään set ja setistä tehdään lista duplikaattien varalta (tämä tehdään all_dates muuttujalle)
        unique_dates = list(set(all_dates))
        print("Unique dates", len(unique_dates))


    # Yhteys OLAP-tietokantaan
    with get_db(cnx_type='olap') as _dw:

        try:

            # Tyhjennetään dim-taulu aina ajon yhteydessä
            #_clear_dates()

            # Datan lisäys date_dim tauluun
            _query = text('INSERT INTO date_dim(year, month, week, day, hour, min, sec) '
                          'VALUES(:year, :month, :week, :day, :hour, :minutes, :seconds )')

            for date in unique_dates:
                _dw.execute(_query, {'year': date.year, 'month': date.month, 'week': date.isocalendar().week,
                                     'day': date.day, 'hour': date.hour, 'minutes': date.minute, 'seconds': date.second})

            _dw.commit()

        except Exception as e:
            _dw.rollback()
            print(e)

