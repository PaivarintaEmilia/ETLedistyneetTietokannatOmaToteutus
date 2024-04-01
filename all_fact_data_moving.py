# Tänne lisätään funktiot, joilla lisätään data transaction_fact-tauluun
from sqlalchemy import text

from db import get_db


# Haetaan tarvittava data OLTP-tietokannasta transactions_fact-taulua varten


def _get_rental_transaction_for_fact(_db):
    _query = text('SELECT id, rental_items_id, created_at FROM rental_transactions')
    rows = _db.execute(_query)
    #print(rows)
    return rows.mappings().all()



# HAETAAN KAIKKI DATA DIM-TAULUISTA


# Haetaan data rental_item_dim-taulusta
def _get_rental_item_dims(_dw):
    _query = text('SELECT item_key, rental_item_id FROM rental_item_dim')
    rows = _dw.execute(_query)
    return rows.mappings().all()



# Haetaan data date_dim-taulusta
def _get_date_dims(_dw):
    _query = text('SELECT * FROM date_dim')
    rows = _dw.execute(_query)
    return rows.mappings().all()



 # HAETAAN KAIKKI KEYT DIM-TAULUISTA

 # Haetaan keyt rental_item_dim-taulusta
def _get_rental_item_key(oltp_item, items):
    for item in items:
        if oltp_item['rental_items_id'] == item['rental_item_id']:
            return item['item_key']

    return None



 # Haetaan keyt date_dim-taulusta
def _get_date_key(oltp_item, dates, key='created_at'):
    date = oltp_item[key]
    for d_dim in dates:
        if date.year == d_dim['year'] and date.month == d_dim['month'] and date.isocalendar().week == d_dim['week'
        ] and date.minute == d_dim['min'] and date.second == d_dim['sec']:
            return d_dim['date_key']

    return None



 # Taulun tyhjennys
def _clear_rental_transaction_fact(_dw):
    _dw.execute(text('DELETE FROM rental_transaction_fact'))
    _dw.commit()


# Datan siirto rental_transaction_fact-tauluun
def rental_transaction_fact_etl():

    with get_db() as _db:
        transactions = _get_rental_transaction_for_fact(_db)
        #print(transactions)


    with get_db(cnx_type='olap') as _dw:

     try:

         rental_item_dims = _get_rental_item_dims(_dw)
         date_dims = _get_date_dims(_dw)

         # Loopataan kaikki keyt
         for transaction in transactions:
             _item_key = _get_rental_item_key(transaction, rental_item_dims)
             _date_key = _get_date_key(transaction, date_dims)


             if _date_key is None or _item_key is None:
                 continue

             # INSERTTI
             _query = text('INSERT INTO rental_transaction_fact(rental_item_key, date_dim_date_key) '
                           'VALUES (:rental_item, :created_at)')

             _dw.execute(_query, {'rental_item': _item_key, 'created_at': _date_key})

         _dw.commit()

     except Exception as e:
         _dw.rollback()
         print(e)



# Haetaan tarvittava data OLTP-tietokannasta rental_item_fact-taulua varten

def _get_rental_items_for_fact(_db):
    _query = text('SELECT id, created_at FROM rental_items')
    rows = _db.execute(_query)
    return rows.mappings().all()


# Taulun tyhjennys
def _clear_rental_item_fact(_dw):
    _dw.execute(text('DELETE FROM rental_item_fact'))
    _dw.commit()


# Datan siirto rental_item_fact-tauluun
def rental_item_fact_etl():

    with get_db() as _db:
     items = _get_rental_items_for_fact(_db)


    with get_db(cnx_type='olap') as _dw:

     try:

         rental_item_dims = _get_rental_item_dims(_dw)
         date_dims = _get_date_dims(_dw)

         # Loopataan kaikki keyt
         for item in items:
             _date_key = _get_date_key(item, date_dims)
             _item_key = _get_rental_item_key(item, rental_item_dims)

             if _date_key is None or _item_key is None:
                 continue

             # INSERTTI
             _query = text('INSERT INTO rental_item_fact(rental_item_key, rental_item_creation_date_key) '
                           'VALUES (:rental_item, :created_at)')

             _dw.execute(_query, {'rental_item': _item_key, 'created_at': _date_key})

         _dw.commit()

     except Exception as e:
         _dw.rollback()
         print(e)
