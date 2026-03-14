import pandas as pd


def safe_get(d, *keys):
    """Safely fetch nested dictionary values"""
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            return None
    return d


def get_room_status(hotel):
    """Combine statuses of multiple rooms"""

    if not isinstance(hotel, dict):
        return None

    rooms = hotel.get("rooms", [])

    if not rooms:
        return None

    statuses = [room.get("status") for room in rooms if room.get("status")]

    return ",".join(statuses) if statuses else None


def clean_brand(brand):

    if not brand:
        return "Unknown"

    brand = brand.lower()

    if brand.startswith("taj"):
        return "Taj"

    if brand.startswith("vivanta"):
        return "Vivanta"

    if brand.startswith("ginger"):
        return "Ginger"

    if brand.startswith("gateway"):
        return "Gateway"

    if brand.startswith("ama"):
        return "Ama"

    if "seleqtions" in brand or "selections" in brand:
        return "Seleqtions"

    return "Unknown"


def classify_journey(promo_type, member_id):

    promo = (promo_type or "").upper()

    if promo == "OFFER":
        return "Offer Journey"

    if promo == "PROMOTION" and member_id not in [None, ""]:
        return "Voucher Journey"

    if promo in ["COUPON", "CORPORATE", "IATA", "PROMOTION"]:
        return "Special Code Journey"

    return "Default Journey"


def clean_payment_status(status):

    if not status:
        return "Unknown"

    status = status.upper()

    if status in ["FAILED", "ABORTED", "TIMEOUT", "NOT_FOUND"]:
        return "Failed"

    if status in ["CHARGED", "PARTIALLY CHARGED"]:
        return "Successful"

    if status in ["PENDING", "REFUND PENDING"]:
        return "Pending"

    return "Pending"


def clean_order_status(status):

    if not status:
        return "Unknown"

    status = status.upper()

    if status in [
        "FAILED",
        "PAYMENT FAILED",
        "CANCELLED",
        "PARTIALLY CANCELLED",
        "PARTIAL_CANCELLED"
    ]:
        return "Failed"

    if status in ["SUCCESS", "CONFIRMED"]:
        return "Successful"

    if status == "INITIATED":
        return "Initiated"

    return "Pending"


def flatten_orders(docs):

    rows = []

    for doc in docs:

        order_items = doc.get("orderLineItems") or [{}]

        # Handle errorMessages safely
        error_data = doc.get("errorMessages")

        if isinstance(error_data, list) and error_data:
            error_data = error_data[0]
        else:
            error_data = {}

        for item in order_items:

            hotel = item.get("hotel") or {}
            loyalty = item.get("loyalty") or {}

            voucher = hotel.get("voucherRedemption") or {}
            brand_name = doc.get("brandName")
            payment_status = doc.get("paymentStatus")
            order_status = doc.get("orderStatus")

            promo_type = hotel.get("promoType")
            member_id = voucher.get("memberId")
            is_user_logged = doc.get("isUserLogged")

            row = {

                # Order fields
                "orderId": doc.get("orderId"),
                "orderType": doc.get("orderType"),
                "createdTimestamp": doc.get("createdTimestamp"),
                
                
                
                "channel": doc.get("channel"),
                "brandName": brand_name,
                "brand_clean": clean_brand(brand_name),

                "paymentStatus": payment_status,
                "payment_status_clean": clean_payment_status(payment_status),

                "orderStatus": order_status,
                "order_status_clean": clean_order_status(order_status),
                "isUserLogged": is_user_logged,
                "user_type": "User Logged" if is_user_logged else "Non Logged",

                "journey_classification": classify_journey(promo_type, member_id),

                "paymentMethod": doc.get("paymentMethod"),
                "isUserLogged": doc.get("isUserLogged"),

                # Hotel fields
                "hotel_promoType": hotel.get("promoType"),
                "hotel_name": hotel.get("name"),
                "hotel_status": hotel.get("status"),
                "room_status": get_room_status(hotel),

                # Voucher redemption
                "voucher_memberId": voucher.get("memberId"),
                "voucher_error_message": voucher.get("errorMessage"),

                # Loyalty fields
                "memberShipPurchaseType": loyalty.get("memberShipPurchaseType"),
                "epicure_type": safe_get(
                    loyalty,
                    "memberCardDetails",
                    "extra_data",
                    "epicure_type"
                ),

                # Financial
                "payableAmount": doc.get("payableAmount"),
                "basePrice": doc.get("basePrice"),
                "taxAmount": doc.get("taxAmount"),
                "gradTotal": doc.get("gradTotal"),

               

                # Error fields
                "errorMessage": error_data.get("errorMessage"),
                "error_type": error_data.get("type"),
                "error_timestamp": error_data.get("timestamp"),
                "error_status_code": safe_get(error_data, "statusCode", "value") or None,
                "error_status_description": safe_get(error_data, "statusCode", "description"),
            }

            rows.append(row)

    return pd.DataFrame(rows)