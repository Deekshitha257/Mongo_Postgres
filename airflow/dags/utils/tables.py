import logging
from .db import get_pg


def create_tables():

    conn = get_pg()
    cur = conn.cursor()

    logging.info("Creating tables")

    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders_dashboard(
        order_id TEXT PRIMARY KEY,
        order_type TEXT,
        created_timestamp TIMESTAMP,
        
        brand_name TEXT,
        brand_clean TEXT,

        payment_status TEXT,
        payment_status_clean TEXT,

        order_status TEXT,
        order_status_clean TEXT,

        journey_classification TEXT,

        promo_type TEXT,
        member_id TEXT,
        voucher_error_message TEXT,

        hotel_name TEXT,
        hotel_status TEXT,
        room_status TEXT,

        membership_purchase_type TEXT,
        epicure_type TEXT,


        channel TEXT,
        is_user_logged BOOLEAN,
        user_type TEXT,
        payment_method TEXT,

        payable_amount DOUBLE PRECISION,
        base_price DOUBLE PRECISION,
        tax_amount DOUBLE PRECISION,
        grad_total DOUBLE PRECISION
    );
    """)

    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions_dashboard(
    txn_id SERIAL PRIMARY KEY,
    source_txn_id TEXT UNIQUE,
    order_id TEXT,
    txn_number INT,
    txn_status TEXT,
    error_message TEXT,
    payment_type TEXT,
    payment_method TEXT,
    txn_net_amount DOUBLE PRECISION
    );
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_errors_dashboard(
    id SERIAL PRIMARY KEY,
    order_id TEXT,
    error_message TEXT,
    error_type TEXT,
    status_code INTEGER,
    status_description TEXT,
    request_endpoint TEXT,
    error_timestamp TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS rooms_dashboard(
    id SERIAL PRIMARY KEY,
    order_id TEXT REFERENCES orders_dashboard(order_id),
    room_number INT,
    room_name TEXT,
    room_type TEXT,
    status TEXT,
    price DOUBLE PRECISION,
    tax DOUBLE PRECISION,
    grand_total DOUBLE PRECISION,
    check_in DATE,
    check_out DATE,
    UNIQUE(order_id, room_number)
    );
    """)

   
    cur.execute("""
    CREATE TABLE IF NOT EXISTS enquiry_dashboard(
        enquiry_id TEXT PRIMARY KEY,
        created_timestamp TIMESTAMP,
        brand TEXT,
        brand_clean TEXT,
        status TEXT,
        preferred_hotel TEXT,
        error_message TEXT,
        type TEXT,
        channel TEXT,
        is_user_logged_in TEXT,
        user_type TEXT
    );
    """)

    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_metadata(
        pipeline_name TEXT PRIMARY KEY,
        last_loaded_timestamp TIMESTAMP
    );
    """)

    conn.commit()

    cur.close()
    conn.close()

    logging.info("Tables created or already exist")