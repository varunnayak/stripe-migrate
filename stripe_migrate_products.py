import os
from typing import Any, Dict, Optional

import stripe
from dotenv import load_dotenv

load_dotenv()

# Load API keys from environment variables
API_KEY_SOURCE: Optional[str] = os.getenv("API_KEY_SOURCE")
API_KEY_TARGET: Optional[str] = os.getenv("API_KEY_TARGET")

# Ensure API keys are set
if not API_KEY_SOURCE:
    raise ValueError("API_KEY_SOURCE environment variable not set.")
if not API_KEY_TARGET:
    raise ValueError("API_KEY_TARGET environment variable not set.")


def get_stripe_client(api_key: str) -> Any:
    """
    Returns a Stripe client initialized with the given API key.

    Args:
        api_key: The Stripe API key to use.

    Returns:
        An initialized Stripe client object.
    """
    return stripe.StripeClient(
        api_key=api_key
    )  # New method - return specific client instance


# Function to create products and prices in the new account
def create_product_and_prices(
    product: Dict[str, Any],
    old_stripe: Any,
    new_stripe: Any,
    dry_run: bool = True,
) -> Optional[Dict[str, str]]:
    """
    Creates a product and its associated active prices from the old account
    in the new Stripe account.

    Args:
        product: The product object from the old Stripe account.
        old_stripe: Initialized Stripe client for the old account.
        new_stripe: Initialized Stripe client for the new account.
        dry_run: If True, simulates the process without creating resources.

    Returns:
        A dictionary mapping old price IDs to new price IDs for this product,
        or None if product creation failed (or skipped in dry run).
    """
    product_id = product.id
    print(f"\nProcessing product: {product.name} ({product_id})")

    new_product_id = product_id  # Assume the same ID for dry run mapping

    if dry_run:
        print(f"  [Dry Run] Would create product: {product.name} ({product_id})")
        # Check if product exists in new account even in dry run for better simulation
        try:
            new_stripe.products.retrieve(product_id)
            print(
                f"  [Dry Run] Product {product_id} already exists in the new account."
            )
        except stripe.error.InvalidRequestError as e:
            if "No such product" in str(e):
                print(
                    f"  [Dry Run] Product {product_id} does not exist yet in the new account."
                )
            else:
                # Log other potential errors during check
                print(
                    f"  [Dry Run] Error checking for existing product {product_id}: {e}"
                )
        except stripe.error.StripeError as e:
            print(f"  [Dry Run] Error checking for existing product {product_id}: {e}")
    else:
        try:
            # Create the product in the new account
            new_product = new_stripe.products.create(
                params={
                    "name": product.name,
                    "active": product.get("active", True),
                    "description": product.get("description"),
                    "id": product_id,  # Use the same ID
                    "metadata": (
                        product.metadata.to_dict_recursive() if product.metadata else {}
                    ),
                    "tax_code": product.get("tax_code"),
                }
            )
            new_product_id = new_product.id  # Get actual ID after creation
            print(f"  Created new product: {new_product_id}")
        except stripe.error.InvalidRequestError as e:
            # Handle cases where the product might already exist (e.g., idempotency)
            if "resource_already_exists" in str(e):
                print(
                    f"  Product {product_id} already exists in the new account. Skipping creation."
                )
                # In case it exists, ensure we use the correct ID
                new_product_id = product_id
            else:
                print(f"  Error creating product {product_id}: {e}")
                return None
        except stripe.error.StripeError as e:
            print(f"  Error creating product {product_id}: {e}")
            return None

    price_map: Dict[str, str] = {}
    # Retrieve active prices for the product from the old account
    try:
        prices = old_stripe.prices.list(
            params={"product": product_id, "active": True, "limit": 100}
        )
        print(f"  Found {len(prices.data)} active price(s) for product {product_id}")

        # Create prices for the new product in the new account
        for price in prices.auto_paging_iter():
            old_price_id = price.id
            print(f"    Processing price: {old_price_id}")
            new_price_id_placeholder = f"new_{old_price_id}"  # Placeholder for dry run

            if dry_run:
                print(
                    f"      [Dry Run] Would create price for product {new_product_id}"
                )
                print(f"        Old Price ID: {old_price_id}")
                print(
                    f"        Currency: {price.currency}, Amount: {price.unit_amount}"
                )
                # Check if price exists in new account even in dry run
                try:
                    # Attempt to construct a potential new ID or check by metadata if possible
                    # For simplicity, we'll just log the check intention.
                    # A more robust check might involve listing prices for the product
                    # and checking metadata, but that adds complexity.
                    print(
                        f"      [Dry Run] Checking if price similar to {old_price_id} exists..."
                    )
                    # Simplified check - assumes ID might be the same or predictable
                    try:
                        existing_price = new_stripe.prices.retrieve(old_price_id)
                        print(
                            f"      [Dry Run] Price {old_price_id} might already exist in the new account."
                        )
                        price_map[old_price_id] = (
                            existing_price.id
                        )  # Map to existing if found by ID
                    except stripe.error.InvalidRequestError as e:
                        if "No such price" in str(e):
                            print(
                                f"      [Dry Run] Price {old_price_id} does not exist by that ID."
                            )
                            price_map[old_price_id] = (
                                new_price_id_placeholder  # Map to placeholder
                            )
                        else:
                            print(
                                f"      [Dry Run] Error checking for existing price {old_price_id}: {e}"
                            )
                            price_map[old_price_id] = (
                                new_price_id_placeholder  # Map to placeholder on error
                            )
                    except stripe.error.StripeError as e:
                        print(
                            f"      [Dry Run] Error checking for existing price {old_price_id}: {e}"
                        )
                        price_map[old_price_id] = (
                            new_price_id_placeholder  # Map to placeholder on error
                        )

                except stripe.error.StripeError as e:
                    print(
                        f"      [Dry Run] Error checking for existing price {old_price_id}: {e}"
                    )
                    price_map[old_price_id] = (
                        new_price_id_placeholder  # Map to placeholder on error
                    )

            else:  # Actual creation logic
                try:
                    new_price = new_stripe.prices.create(
                        params={
                            "currency": price.currency,
                            "active": price.active,
                            "metadata": {
                                **(
                                    price.metadata.to_dict_recursive()
                                    if price.metadata
                                    else {}
                                ),
                                "old_price_id": old_price_id,
                            },
                            "nickname": price.get("nickname"),
                            "product": new_product_id,
                            "recurring": price.get("recurring"),
                            "tax_behavior": price.get("tax_behavior"),
                            "unit_amount": price.get("unit_amount"),
                            "billing_scheme": price.billing_scheme,
                            "tiers": price.get("tiers"),
                            "tiers_mode": price.get("tiers_mode"),
                            "transform_quantity": price.get("transform_quantity"),
                        }
                    )
                    print(
                        f"      Created new price: {new_price.id} (linked to old: {old_price_id})"
                    )
                    price_map[old_price_id] = new_price.id
                except stripe.error.InvalidRequestError as e:
                    # Price creation doesn't usually throw 'resource_already_exists' like products
                    # If a price with identical parameters under the new product exists,
                    # Stripe might create it anyway or error differently.
                    # Handling relies more on the 'old_price_id' metadata for mapping.
                    print(
                        f"      Error creating price (linked to old: {old_price_id}) for product {new_product_id}: {e}"
                    )
                    # Decide if we should skip mapping or try to find a potential match
                    # For simplicity, we'll skip mapping on error during live run.
                except stripe.error.StripeError as e:
                    print(
                        f"      Error creating price (linked to old: {old_price_id}) for product {new_product_id}: {e}"
                    )

    except stripe.error.StripeError as e:
        print(f"  Error fetching prices for product {product_id}: {e}")

    return price_map


def migrate_products(dry_run: bool = True) -> None:
    """
    Migrates all active products and their active prices from the old Stripe
    account to the new Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    print(f"Starting product and price migration (dry_run={dry_run})...")
    old_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    try:
        products = old_stripe.products.list(params={"active": True, "limit": 100})
        print(f"Found {len(products.data)} active product(s) in the old account.")
        # Loop through each product and create it in the new account
        for product in products.auto_paging_iter():
            create_product_and_prices(product, old_stripe, new_stripe, dry_run)

        print(f"\nProduct and price migration completed (dry_run={dry_run}).")

    except stripe.error.StripeError as e:
        print(f"Error fetching products from old account: {e}")


def migrate_coupons(dry_run: bool = True) -> None:
    """
    Migrates all valid coupons from the old Stripe account to the new Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    print(f"\nStarting coupon migration (dry_run={dry_run})...")
    old_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    try:
        coupons = old_stripe.coupons.list(params={"limit": 100})
        migrated_count = 0
        skipped_count = 0
        for coupon in coupons.auto_paging_iter():
            if coupon.valid:
                coupon_id = coupon.id
                print(f"  Processing coupon: {coupon.name or coupon_id}")

                if dry_run:
                    print(
                        f"    [Dry Run] Would create coupon: {coupon.name or coupon_id}"
                    )
                    # Check if coupon exists
                    try:
                        new_stripe.coupons.retrieve(coupon_id)
                        print(
                            f"    [Dry Run] Coupon {coupon_id} already exists. Would skip."
                        )
                        skipped_count += 1
                    except stripe.error.InvalidRequestError as e:
                        if "No such coupon" in str(e):
                            print(
                                f"    [Dry Run] Coupon {coupon_id} does not exist yet."
                            )
                            migrated_count += 1  # Count as would-be migrated
                        else:
                            print(
                                f"    [Dry Run] Error checking for coupon {coupon_id}: {e}"
                            )
                            skipped_count += 1
                    except stripe.error.StripeError as e:
                        print(
                            f"    [Dry Run] Error checking for coupon {coupon_id}: {e}"
                        )
                        skipped_count += 1
                    continue  # Skip actual creation logic in dry run

                # Actual creation logic (only runs if not dry_run)
                try:
                    new_coupon = new_stripe.coupons.create(
                        params={
                            "amount_off": coupon.get("amount_off"),
                            "currency": coupon.get("currency"),
                            "duration": coupon.duration,
                            "metadata": (
                                coupon.metadata.to_dict_recursive()
                                if coupon.metadata
                                else {}
                            ),
                            "name": coupon.get("name"),
                            "percent_off": coupon.get("percent_off"),
                            "duration_in_months": coupon.get("duration_in_months"),
                            "id": coupon.id,  # Use the same ID
                            "max_redemptions": coupon.get("max_redemptions"),
                            "redeem_by": coupon.get("redeem_by"),
                        }
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


def migrate_promocodes(dry_run: bool = True) -> None:
    """
    Migrates all active promotion codes from the old Stripe account to the new
    Stripe account. Assumes coupons have already been migrated.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    print(f"\nStarting promotion code migration (dry_run={dry_run})...")
    old_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    try:
        promo_codes = old_stripe.promotion_codes.list(params={"limit": 100})
        migrated_count = 0
        skipped_count = 0
        for promo_code in promo_codes.auto_paging_iter():
            if promo_code.active:
                promo_code_id = promo_code.id
                promo_code_code = promo_code.code
                coupon_id = promo_code.coupon.id
                print(f"  Processing promotion code: {promo_code_code}")

                # Check for coupon existence (always do this check, even in dry run)
                try:
                    new_stripe.coupons.retrieve(coupon_id)
                    print(f"    Associated coupon {coupon_id} found in new account.")
                except stripe.error.InvalidRequestError as e:
                    print(
                        f"    Skipping promocode {promo_code_code}: Associated coupon {coupon_id} not found in new account. Error: {e}"
                    )
                    skipped_count += 1
                    continue  # Skip this promo code if coupon doesn't exist

                # Dry run simulation or actual creation
                if dry_run:
                    print(
                        f"    [Dry Run] Would create promotion code: {promo_code_code} for coupon {coupon_id}"
                    )
                    # Check if promo code exists
                    try:
                        # Promo codes are often retrieved by code, but API might need ID.
                        # We'll check existence conceptually. A list operation might be needed
                        # for a perfect check, but let's keep it simple.
                        # Attempt retrieve by ID (less likely to work for promo codes vs coupons/products)
                        # Or maybe list and filter by code? Let's assume check means "would attempt create".
                        print(
                            f"    [Dry Run] Checking if promo code {promo_code_code} exists..."
                        )
                        # Simulate check - assume it doesn't exist unless an obvious error occurs
                        # A real check might involve listing codes: codes = new_stripe.PromotionCode.list(code=promo_code_code)
                        print(
                            f"    [Dry Run] Promo code {promo_code_code} assumed not to exist yet."
                        )
                        migrated_count += 1  # Count as would-be migrated
                    except stripe.error.StripeError as e:
                        # This block might not be reached with the simplified check above
                        print(
                            f"    [Dry Run] Error checking for promo code {promo_code_code}: {e}"
                        )
                        skipped_count += 1
                    continue  # Skip actual creation

                # Actual creation logic (only runs if not dry_run)
                try:
                    new_promo_code = new_stripe.promotion_codes.create(
                        params={
                            "coupon": coupon_id,
                            "code": promo_code_code,
                            "metadata": {
                                **(
                                    promo_code.metadata.to_dict_recursive()
                                    if promo_code.metadata
                                    else {}
                                ),
                                "old_promotion_code_id": promo_code_id,
                            },
                            "active": promo_code.active,
                            "customer": promo_code.get("customer"),
                            "expires_at": promo_code.get("expires_at"),
                            "max_redemptions": promo_code.get("max_redemptions"),
                            "restrictions": (
                                promo_code.restrictions.to_dict_recursive()
                                if promo_code.restrictions
                                else {}
                            ),
                        }
                    )
                    print(f"    Migrated promotion code: {new_promo_code.id}")
                    migrated_count += 1
                except stripe.error.InvalidRequestError as e:
                    if "resource_already_exists" in str(e):
                        print(
                            f"    Promotion code {promo_code_code} already exists. Skipping creation."
                        )
                        skipped_count += 1
                    else:
                        print(
                            f"    Error migrating promotion code {promo_code_code}: {e}"
                        )
                        skipped_count += 1
                except stripe.error.StripeError as e:
                    print(f"    Error migrating promotion code {promo_code_code}: {e}")
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
    import argparse  # Import argparse here

    parser = argparse.ArgumentParser(
        description="Migrate Stripe Products, Coupons, and Promo Codes."
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Perform the migration live. Default is dry run.",
    )
    args = parser.parse_args()

    is_dry_run = not args.live  # dry_run is True if --live is NOT specified

    print(f"Starting Stripe data migration... (Dry Run: {is_dry_run})")
    migrate_products(dry_run=is_dry_run)
    migrate_coupons(dry_run=is_dry_run)
    migrate_promocodes(dry_run=is_dry_run)
    print(f"\nStripe data migration finished. (Dry Run: {is_dry_run})")


if __name__ == "__main__":
    main()
