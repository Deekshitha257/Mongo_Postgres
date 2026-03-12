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

            row = {

                # Order fields
                "orderId": doc.get("orderId"),
                "orderType": doc.get("orderType"),
                "createdTimestamp": doc.get("createdTimestamp"),
                "brandName": doc.get("brandName"),
                "paymentStatus": doc.get("paymentStatus"),
                "orderStatus": doc.get("orderStatus"),
                "channel": doc.get("channel"),

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