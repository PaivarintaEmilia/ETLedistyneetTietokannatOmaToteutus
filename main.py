# Täällä ei mitää muuta, kuin ohjelman toiminta konsolissa
import date_dim_data_moving
import rental_item_data_moving
import all_fact_data_moving
from db import get_db


def main():
    # dim_taulujen täyttö
    rental_item_data_moving.rental_item_etl()
    date_dim_data_moving.date_dim_etl()

    # faktataulujen tyhjennykset
    with get_db(cnx_type='olap') as _dw:
        all_fact_data_moving._clear_rental_transaction_fact(_dw)
        all_fact_data_moving._clear_rental_item_fact(_dw)



    # faktataulujen täytöt
    all_fact_data_moving.rental_item_fact_etl()
    all_fact_data_moving.rental_transaction_fact_etl()



# Tätä en ihan ymmärtänyt
if __name__ == '__main__':
    main()
