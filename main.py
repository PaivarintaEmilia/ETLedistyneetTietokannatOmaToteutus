# Täällä ei mitää muuta, kuin ohjelman toiminta konsolissa
import date_dim_data_moving
import rental_item_data_moving
import all_fact_data_moving


def main():
    # dim_taulujen täyttö
    #rental_item_data_moving.rental_item_etl()
    #date_dim_data_moving.date_dim_etl()

    # faktataulujen tyhjennykset


    # faktataulujen täytöt
    #all_fact_data_moving.rental_item_fact_etl()
    all_fact_data_moving.rental_transaction_fact_etl()
    #all_fact_data_moving.rental_transaction_fact_etl()


# Tätä en ihan ymmärtänyt
if __name__ == '__main__':
    main()
