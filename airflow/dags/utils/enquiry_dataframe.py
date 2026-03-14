import pandas as pd
from .dataframe import clean_brand

def flatten_enquiries(docs):

    rows = []

    for doc in docs:

        created_ts = doc.get("createdTimestamp")

        # Fix missing timestamp
        if created_ts is None:
            created_ts = None

        brand = doc.get("brand")                 
       
        is_logged = str(doc.get("isUserLoggedIn")).lower() == "true"

        row = {
            "enquiryId": doc.get("enquiryId"),
            "createdTimestamp": created_ts,
            "brand": brand,
            "brand_clean": clean_brand(brand), 
            "status": doc.get("status"),
            "preferredHotel": doc.get("preferredHotel"),
            "errorMessage": doc.get("errorMessage"),
            "type": doc.get("type"),
            "channel": doc.get("channel"),
            "isUserLoggedIn": is_logged,
            "user_type": "User Logged" if is_logged else "Non Logged"   
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