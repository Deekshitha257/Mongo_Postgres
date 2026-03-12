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
        payment_status TEXT,
        order_status TEXT,

        promo_type TEXT,
        member_id TEXT,
        voucher_error_message TEXT,

        hotel_name TEXT,
        hotel_status TEXT,
        room_status TEXT,

        membership_purchase_type TEXT,
        epicure_type TEXT,

        
        error_message TEXT,
        error_type TEXT,
        error_status_code TEXT,
        error_status_description TEXT,
        error_timestamp TIMESTAMP,

        channel TEXT,
        is_user_logged BOOLEAN,
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
    CREATE TABLE IF NOT EXISTS enquiry_dashboard(
        enquiry_id TEXT PRIMARY KEY,
        created_timestamp TIMESTAMP,
        brand TEXT,
        status TEXT,
        preferred_hotel TEXT,
        error_message TEXT,
        type TEXT,
        channel TEXT,
        is_user_logged_in TEXT
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