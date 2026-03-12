import pandas as pd

def flatten_enquiries(docs):

    rows = []

    for doc in docs:

        created_ts = doc.get("createdTimestamp")

        # Fix missing timestamp
        if created_ts is None:
            created_ts = None

        row = {
            "enquiryId": doc.get("enquiryId"),
            "createdTimestamp": created_ts,
            "brand": doc.get("brand"),
            "status": doc.get("status"),
            "preferredHotel": doc.get("preferredHotel"),
            "errorMessage": doc.get("errorMessage"),
            "type": doc.get("type"),
            "channel": doc.get("channel"),
            "isUserLoggedIn": str(doc.get("isUserLoggedIn")).lower() == "true"
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    # ⭐ CRITICAL FIX
    df["createdTimestamp"] = pd.to_datetime(df["createdTimestamp"], errors="coerce")

    # ⭐ convert NaT → None
    df["createdTimestamp"] = df["createdTimestamp"].astype(object).where(
        pd.notnull(df["createdTimestamp"]), None
    )

    return df