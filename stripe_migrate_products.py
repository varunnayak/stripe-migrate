import os
import json
from typing import Any, Dict, Optional

import stripe

# Load API keys from environment variables
API_KEY_OLD: Optional[str] = os.getenv("API_KEY_OLD")
API_KEY_NEW: Optional[str] = os.getenv("API_KEY_NEW")

# Ensure API keys are set
if not API_KEY_OLD:
    raise ValueError("API_KEY_OLD environment variable not set.")
if not API_KEY_NEW:
    raise ValueError("API_KEY_NEW environment variable not set.")


def get_stripe_client(api_key: str) -> Any:
    """
    Returns a Stripe client initialized with the given API key.

    Args:
        api_key: The Stripe API key to use.

    Returns:
        An initialized Stripe client object.
    """
    stripe.api_key = api_key
    return stripe


# Function to create products and prices in the new account
def create_product_and_prices(
    product: Dict[str, Any], old_stripe: Any, new_stripe: Any
) -> Optional[Dict[str, str]]:
    """
    Creates a product and its associated active prices from the old account
    in the new Stripe account.

    Args:
        product: The product object from the old Stripe account.
        old_stripe: Initialized Stripe client for the old account.
        new_stripe: Initialized Stripe client for the new account.

    Returns:
        A dictionary mapping old price IDs to new price IDs for this product,
        or None if product creation failed.
    """
    product_id = product.id
    print(f"\nProcessing product: {product.name} ({product_id})")

    try:
        # Create the product in the new account
        new_product = new_stripe.Product.create(
            name=product.name,
            active=product.get("active", True),
            description=product.get("description"),
            id=product_id,  # Use the same ID
            metadata=(product.metadata.to_dict_recursive() if product.metadata else {}),
            tax_code=product.get("tax_code"),
        )
        print(f"  Created new product: {new_product.id}")
    except stripe.error.InvalidRequestError as e:
        # Handle cases where the product might already exist (e.g., idempotency)
        if "resource_already_exists" in str(e):
            print(
                f"  Product {product_id} already exists in the new account. Skipping creation."
            )
        else:
            print(f"  Error creating product {product_id}: {e}")
            return None
    except stripe.error.StripeError as e:
        print(f"  Error creating product {product_id}: {e}")
        return None

    price_map: Dict[str, str] = {}
    # Retrieve active prices for the product from the old account
    try:
        prices = old_stripe.Price.list(product=product_id, active=True, limit=100)
        print(f"  Found {len(prices.data)} active price(s) for product {product_id}")

        # Create prices for the new product in the new account
        for price in prices.auto_paging_iter():
            print(f"    Processing price: {price.id}")
            try:
                new_price = new_stripe.Price.create(
                    currency=price.currency,
                    active=price.active,
                    metadata={
                        **(
                            price.metadata.to_dict_recursive() if price.metadata else {}
                        ),
                        "old_price_id": price.id,  # Add reference to the old price ID
                    },
                    nickname=price.get("nickname"),
                    product=product_id,
                    recurring=price.get("recurring"),
                    tax_behavior=price.get("tax_behavior"),
                    unit_amount=price.get("unit_amount"),
                    billing_scheme=price.billing_scheme,
                    tiers=price.get("tiers"),
                    tiers_mode=price.get("tiers_mode"),
                    transform_quantity=price.get("transform_quantity"),
                )
                print(f"      Created new price: {new_price.id}")
                price_map[price.id] = new_price.id
            except stripe.error.InvalidRequestError as e:
                if "resource_already_exists" in str(e):
                    print(
                        f"      Price {price.id} already exists for product {product_id}. Skipping creation."
                    )
                    # If price already exists, assume it was mapped previously or handle accordingly
                    # For simplicity, let's try retrieving it to get the ID for mapping
                    try:
                        existing_price = new_stripe.Price.retrieve(price.id)
                        price_map[price.id] = existing_price.id
                        print(
                            f"      Using existing price mapping: {price.id} -> {existing_price.id}"
                        )
                    except stripe.error.StripeError as retrieve_error:
                        print(
                            f"      Could not retrieve existing price {price.id}: {retrieve_error}"
                        )
                else:
                    print(
                        f"      Error creating price {price.id} for product {product_id}: {e}"
                    )
            except stripe.error.StripeError as e:
                print(
                    f"      Error creating price {price.id} for product {product_id}: {e}"
                )

    except stripe.error.StripeError as e:
        print(f"  Error fetching prices for product {product_id}: {e}")

    return price_map


def migrate_products() -> None:
    """
    Migrates all active products and their active prices from the old Stripe
    account to the new Stripe account.
    """
    print("Starting product and price migration...")
    old_stripe = get_stripe_client(API_KEY_OLD)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_NEW)  # type: ignore

    all_price_mappings: Dict[str, str] = {}

    try:
        # Retrieve active products from the old account
        products = old_stripe.Product.list(active=True, limit=100)
        # Loop through each product and create it in the new account
        for product in products.auto_paging_iter():
            product_price_map = create_product_and_prices(
                product, old_stripe, new_stripe
            )
            if product_price_map:
                all_price_mappings.update(product_price_map)

        print("\nProduct and price migration completed.")

        # Generate and print the JSON price mapping
        if all_price_mappings:
            price_mapping_json = json.dumps(all_price_mappings, indent=4)
            print("\n--- PRICE MAPPING JSON ---")
            print(
                "Set this as the PRICE_MAPPING_JSON environment variable for the subscription migration script:"
            )
            print(price_mapping_json)
            print("--- END PRICE MAPPING JSON ---")
        else:
            print(
                "\nNo price mappings were generated (no prices migrated or errors occurred)."
            )

    except stripe.error.StripeError as e:
        print(f"Error fetching products from old account: {e}")


def migrate_coupons() -> None:
    """
    Migrates all valid coupons from the old Stripe account to the new Stripe account.
    """
    print("\nStarting coupon migration...")
    old_stripe = get_stripe_client(API_KEY_OLD)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_NEW)  # type: ignore

    try:
        coupons = old_stripe.Coupon.list(limit=100)
        migrated_count = 0
        skipped_count = 0
        for coupon in coupons.auto_paging_iter():
            if coupon.valid:
                print(f"  Processing coupon: {coupon.name or coupon.id}")
                try:
                    new_coupon = new_stripe.Coupon.create(
                        amount_off=coupon.get("amount_off"),
                        currency=coupon.get("currency"),
                        duration=coupon.duration,
                        metadata=(
                            coupon.metadata.to_dict_recursive()
                            if coupon.metadata
                            else {}
                        ),
                        name=coupon.get("name"),
                        percent_off=coupon.get("percent_off"),
                        duration_in_months=coupon.get("duration_in_months"),
                        id=coupon.id,  # Use the same ID
                        max_redemptions=coupon.get("max_redemptions"),
                        redeem_by=coupon.get("redeem_by"),
                    )
                    print(f"    Migrated coupon: {new_coupon.id}")
                    migrated_count += 1
                except stripe.error.InvalidRequestError as e:
                    if "resource_already_exists" in str(e):
                        print(
                            f"    Coupon {coupon.id} already exists. Skipping creation."
                        )
                        skipped_count += 1
                    else:
                        print(f"    Error migrating coupon {coupon.id}: {e}")
                        skipped_count += 1
                except stripe.error.StripeError as e:
                    print(f"    Error migrating coupon {coupon.id}: {e}")
                    skipped_count += 1
            else:
                print(f"  Skipping invalid coupon: {coupon.name or coupon.id}")
                skipped_count += 1
        print(
            f"\nCoupon migration completed. Migrated: {migrated_count}, Skipped/Errors: {skipped_count}"
        )
    except stripe.error.StripeError as e:
        print(f"Error fetching coupons from old account: {e}")


def migrate_promocodes() -> None:
    """
    Migrates all active promotion codes from the old Stripe account to the new
    Stripe account. Assumes coupons have already been migrated.
    """
    print("\nStarting promotion code migration...")
    old_stripe = get_stripe_client(API_KEY_OLD)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_NEW)  # type: ignore

    try:
        promo_codes = old_stripe.PromotionCode.list(limit=100)
        migrated_count = 0
        skipped_count = 0
        for promo_code in promo_codes.auto_paging_iter():
            if promo_code.active:
                print(f"  Processing promotion code: {promo_code.code}")
                try:
                    # Ensure the referenced coupon exists in the new account
                    try:
                        new_stripe.Coupon.retrieve(promo_code.coupon.id)
                    except stripe.error.InvalidRequestError as e:
                        print(
                            f"    Skipping promocode {promo_code.code}: Associated coupon {promo_code.coupon.id} not found in new account. Error: {e}"
                        )
                        skipped_count += 1
                        continue  # Skip this promo code if coupon doesn't exist

                    new_promo_code = new_stripe.PromotionCode.create(
                        coupon=promo_code.coupon.id,
                        code=promo_code.code,
                        metadata={
                            **(
                                promo_code.metadata.to_dict_recursive()
                                if promo_code.metadata
                                else {}
                            ),
                            "old_promotion_code_id": promo_code.id,  # Add reference to the old promo_code ID
                        },
                        active=promo_code.active,
                        customer=promo_code.get("customer"),
                        expires_at=promo_code.get("expires_at"),
                        max_redemptions=promo_code.get("max_redemptions"),
                        restrictions=(
                            promo_code.restrictions.to_dict_recursive()
                            if promo_code.restrictions
                            else {}
                        ),
                        # Add other fields if needed: first_time_transaction, minimum_amount, etc.
                    )
                    print(f"    Migrated promotion code: {new_promo_code.id}")
                    migrated_count += 1
                except stripe.error.InvalidRequestError as e:
                    if "resource_already_exists" in str(e):
                        print(
                            f"    Promotion code {promo_code.code} already exists. Skipping creation."
                        )
                        skipped_count += 1
                    else:
                        print(
                            f"    Error migrating promotion code {promo_code.code}: {e}"
                        )
                        skipped_count += 1
                except stripe.error.StripeError as e:
                    print(f"    Error migrating promotion code {promo_code.code}: {e}")
                    skipped_count += 1
            else:
                print(f"  Skipping inactive promotion code: {promo_code.code}")
                skipped_count += 1
        print(
            f"\nPromotion code migration completed. Migrated: {migrated_count}, Skipped/Errors: {skipped_count}"
        )
    except stripe.error.StripeError as e:
        print(f"Error fetching promotion codes from old account: {e}")


def main() -> None:
    """Main function to run the product, coupon, and promotion code migrations."""
    print("Starting Stripe data migration...")
    migrate_products()
    migrate_coupons()
    migrate_promocodes()
    print("\nStripe data migration finished.")


if __name__ == "__main__":
    main()
