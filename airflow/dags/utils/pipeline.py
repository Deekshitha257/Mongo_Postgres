import logging
import pandas as pd
from psycopg2.extras import execute_values
from .dataframe import safe_get
from .db import get_mongo, get_pg
from .dataframe import flatten_orders
from .tables import create_tables
from .checkpoint import get_checkpoint, update_checkpoint
from .enquiry_dataframe import flatten_enquiries


def run_pipeline():

    logging.info("Pipeline started")

    create_tables()

    mongo = get_mongo()
    conn = get_pg()
    cur = conn.cursor()

    last_ts = get_checkpoint(conn, "orders_pipeline")

    logging.info("Fetching orders from MongoDB")

    query = {}

    if last_ts:
        query = {"createdTimestamp": {"$gt": last_ts}}

    docs = list(mongo.order.find(query).batch_size(5000))
    logging.info(f"{len(docs)} documents fetched from MongoDB")

    if not docs:
        logging.info("No new records found")
        cur.execute("SELECT COUNT(*) FROM orders_dashboard")
        total_records = cur.fetchone()[0]
        logging.info(f"Total records present in orders_dashboard table: {total_records}")
        return

    # ---------------------------
    # Extract Transactions
    # ---------------------------

    txn_rows = []
    error_rows = []

    for doc in docs:

        order_id = doc.get("orderId")
        payment_details = doc.get("paymentDetails") or {}

        txn_keys = ["transaction_1", "transaction_2", "transaction_3", "transaction_4"]

        for txn_key in txn_keys:

            txns = payment_details.get(txn_key)

            if not txns:
                continue

            txn_number = int(txn_key.split("_")[1])

            if isinstance(txns, dict):
                txns = [txns]

            for idx, txn in enumerate(txns):

                source_txn_id = f"{order_id}_{txn_number}_{idx}"

                txn_rows.append(
                    (
                        source_txn_id,
                        order_id,
                        txn_number,
                        txn.get("txnStatus"),
                        txn.get("errorMessage"),
                        txn.get("paymentType"),
                        txn.get("paymentMethod"),
                        txn.get("txnNetAmount"),
                    )
                )

    


        errors = doc.get("errorMessages") or []

        if isinstance(errors, list):

            for err in errors:

                error_rows.append(
                    (
                        order_id,
                        err.get("errorMessage"),
                        err.get("type"),
                        safe_get(err, "statusCode", "value"),
                        safe_get(err, "statusCode", "description"),
                        err.get("requestEndPoint"),
                        err.get("timestamp")
                    )
                )   


    # ---------------------------
    # Flatten orders
    # ---------------------------

    df = flatten_orders(docs)

    if df.empty:
        logging.info("No rows after flattening")
        return

    df["createdTimestamp"] = df["createdTimestamp"].astype(object)
    df["createdTimestamp"] = df["createdTimestamp"].where(
        pd.notnull(df["createdTimestamp"]), None
    )

    df["error_timestamp"] = df["error_timestamp"].astype(object)
    df["error_timestamp"] = df["error_timestamp"].where(
        pd.notnull(df["error_timestamp"]), None
    )

    logging.info(f"{len(df)} rows created in dataframe")
    df = df.astype(object).where(pd.notnull(df), None)

    # ---------------------------
    # Prepare orders data
    # ---------------------------

    data = [
        (
            row.orderId,
            row.orderType,
            row.createdTimestamp,
            

            row.brandName,
            row.brand_clean,

            row.paymentStatus,
            row.payment_status_clean,

            row.orderStatus,
            row.order_status_clean,

            row.journey_classification,

            row.hotel_promoType,
            row.voucher_memberId,
            row.voucher_error_message,

            row.hotel_name,
            row.hotel_status,
            row.room_status,

            row.memberShipPurchaseType,
            row.epicure_type,

            

            row.errorMessage,
            row.error_type,
            row.error_status_code,
            row.error_status_description,
            row.error_timestamp,

            row.channel,
            row.isUserLogged,
            row.user_type,
            row.paymentMethod,

            row.payableAmount,
            row.basePrice,
            row.taxAmount,
            row.gradTotal
        )
        for row in df.itertuples(index=False)
    ]

    # ---------------------------
    # Insert Orders
    # ---------------------------

    try:
        execute_values(
            cur,
            """
            INSERT INTO orders_dashboard(
                order_id,
                order_type,
                created_timestamp,
                
                brand_name,
                brand_clean,
                payment_status,
                payment_status_clean,
                order_status,
                order_status_clean,
                journey_classification,
                promo_type,
                member_id,
                voucher_error_message,
                hotel_name,
                hotel_status,
                room_status,
                membership_purchase_type,
                epicure_type,
                error_message,
                error_type,
                error_status_code,
                error_status_description,
                error_timestamp,
                channel,
                is_user_logged,
                user_type,
                payment_method,
                payable_amount,
                base_price,
                tax_amount,
                grad_total
            )
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
            """,
            data
        )

    except Exception as e:
        print("❌ ORDER INSERT FAILED")
        print("ERROR:", e)

        for row in data[:20]:
            print("ROW:", row)

        raise

    inserted_rows = cur.rowcount
    logging.info(f"{inserted_rows} rows inserted into orders_dashboard")

    # ---------------------------
    # Insert Transactions
    # ---------------------------

    if txn_rows:

        try:
            execute_values(
                cur,
                """
                INSERT INTO transactions_dashboard(
                    source_txn_id,
                    order_id,
                    txn_number,
                    txn_status,
                    error_message,
                    payment_type,
                    payment_method,
                    txn_net_amount
                )
                VALUES %s
                ON CONFLICT (source_txn_id) DO NOTHING
                """,
                txn_rows
            )

            logging.info(f"{len(txn_rows)} transaction rows inserted")

        except Exception as e:
            print("❌ TXN INSERT FAILED")
            print("ERROR:", e)
            raise


    # ---------------------------
    # Insert Order Errors
    # ---------------------------

    if error_rows:

        execute_values(
            cur,
            """
            INSERT INTO order_errors_dashboard(
                order_id,
                error_message,
                error_type,
                status_code,
                status_description,
                request_endpoint,
                error_timestamp
            )
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            error_rows
        )

        logging.info(f"{len(error_rows)} error rows inserted")


    # Commit everything
    conn.commit()

        

    # ---------------------------
    # Log total orders
    # ---------------------------

    cur.execute("SELECT COUNT(*) FROM orders_dashboard")
    total_records = cur.fetchone()[0]

    logging.info(f"Total records present in orders_dashboard table: {total_records}")

    # ---------------------------
    # Update checkpoint
    # ---------------------------

    max_ts = df["createdTimestamp"].dropna().max()

    if pd.notna(max_ts):
        update_checkpoint(conn, "orders_pipeline", max_ts)

    cur.close()
    conn.close()

    logging.info("Pipeline completed successfully")


def run_enquiry_pipeline():

    logging.info("Enquiry pipeline started")

    mongo = get_mongo()
    conn = get_pg()
    cur = conn.cursor()

    last_ts = get_checkpoint(conn, "enquiry_pipeline")

    query = {}

    if last_ts:
        query = {"createdTimestamp": {"$gt": last_ts}}

    docs = list(mongo.enquiryMongodbDto.find(query).batch_size(5000))

    logging.info(f"{len(docs)} enquiry documents fetched")

    if not docs:
        logging.info("No new enquiries found")
        return

    df = flatten_enquiries(docs)

    if df.empty:
        logging.info("No rows after flattening enquiries")
        return

    logging.info(f"{len(df)} enquiry rows created")

    data = list(df.itertuples(index=False, name=None))

    execute_values(
        cur,
        """
        INSERT INTO enquiry_dashboard(
            enquiry_id,
            created_timestamp,
            brand,
            brand_clean,
            status,
            preferred_hotel,
            error_message,
            type,
            channel,
            is_user_logged_in,
            user_type
        )
        VALUES %s
        ON CONFLICT (enquiry_id) DO NOTHING
        """,
        data
    )

    inserted_rows = cur.rowcount
    logging.info(f"{inserted_rows} enquiry rows inserted")

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM enquiry_dashboard")
    total_records = cur.fetchone()[0]

    logging.info(f"Total enquiry records: {total_records}")

    max_ts = df["createdTimestamp"].dropna().max()

    if max_ts is not None:
        update_checkpoint(conn, "enquiry_pipeline", max_ts)

    cur.close()
    conn.close()

    logging.info("Enquiry pipeline completed")