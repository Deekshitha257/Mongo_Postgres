import logging


def get_checkpoint(conn, pipeline_name):
    """
    Get last loaded timestamp for incremental loading
    """

    cur = conn.cursor()

    cur.execute("""
        SELECT last_loaded_timestamp
        FROM pipeline_metadata
        WHERE pipeline_name = %s
    """, (pipeline_name,))

    row = cur.fetchone()

    cur.close()

    if row:
        logging.info(f"Last checkpoint: {row[0]}")
        return row[0]

    logging.info("No checkpoint found. Full load will run.")
    return None


def update_checkpoint(conn, pipeline_name, last_ts):
    """
    Update checkpoint after successful load
    """

    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pipeline_metadata (pipeline_name, last_loaded_timestamp)
        VALUES (%s, %s)
        ON CONFLICT (pipeline_name)
        DO UPDATE SET last_loaded_timestamp = EXCLUDED.last_loaded_timestamp
    """, (pipeline_name, last_ts))

    conn.commit()

    cur.close()

    logging.info(f"Checkpoint updated to {last_ts}")