# FUNKTIOT RENTAL_ITEM_DIM-TAULUN DATAN HAKUUN JA TAULUN TÄYTTÖÖN (HUOM DIM-TAULU TYHJENNETÄÄN, KOSKA OHJELMAA AJETAAN USEAAN KERTAAN)
# Importit
from sqlalchemy import text

from db import get_db


# Funktio tarvittavan datan hakemiseen OLTP-tietokannasta
def _get_rental_items(_db):
    _query = text('SELECT rental_items.name AS rental_items_name_oltp, rental_items.id AS rental_items_id_oltp '
                  'FROM rental_items')

    rows = _db.execute(_query)

    items = rows.mappings().all()

    return items


# rental_items_dim-taulun tyhjennys
def _clear_rental_item_dim(_dw):
    try:
        _query = text("DELETE FROM rental_item_dim")
        _dw.execute(_query)
        _dw.commit()
    except Exception as e:
        print(e)


# haetun datan siirto OLAP-tietokantaan
def rental_item_etl():

    # Yhdistys OLTP-tietokantaan niin saadaan siirrettävät tiedot
    with get_db() as _db:
        items = _get_rental_items(_db)


    # Avataan yhteys OLAP-tietokantaan datan lisäämiseksi
    with get_db(cnx_type='olap') as _dw:
        try:
            # datan tyhjennys TEE FUNKTIO
            _clear_rental_item_dim(_dw)

            for item in items:
                _query = text('INSERT INTO rental_item_dim'
                              '(rental_item_id, renta_item_name) '
                              'VALUES(:rental_items_id, :rental_items_name)')

                _dw.execute(_query, {'rental_items_id': item['rental_items_id_oltp'], 'rental_items_name': item['rental_items_name_oltp']})

            _dw.commit()

        except Exception as e:
            # Peruutetaan tulokset, jos kysely epäonnistuu
            _dw.rollback()
            print(e)