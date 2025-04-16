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
    return stripe.StripeClient(api_key=api_key)


# Function to create products and prices in the target account
def create_product_and_prices(
    product: Dict[str, Any],
    source_stripe: Any,
    target_stripe: Any,
    dry_run: bool = True,
) -> Optional[Dict[str, str]]:
    """
    Creates a product and its associated active prices from the source account
    in the target Stripe account.

    Args:
        product: The product object from the source Stripe account.
        source_stripe: Initialized Stripe client for the source account.
        target_stripe: Initialized Stripe client for the target account.
        dry_run: If True, simulates the process without creating resources.

    Returns:
        A dictionary mapping source price IDs to target price IDs for this product,
        or None if product creation failed (or skipped in dry run).
    """
    product_id = product.id
    print(f"\nProcessing product: {product.name} ({product_id})")

    target_product_id = product_id  # Assume the same ID for dry run mapping

    if dry_run:
        print(f"  [Dry Run] Would create product: {product.name} ({product_id})")
        # Check if product exists in target account even in dry run for better simulation
        try:
            target_stripe.products.retrieve(product_id)
            print(
                f"  [Dry Run] Product {product_id} already exists in the target account."
            )
        except stripe.error.InvalidRequestError as e:
            if "No such product" in str(e):
                print(
                    f"  [Dry Run] Product {product_id} does not exist yet in the target account."
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
            # Create the product in the target account
            target_product = target_stripe.products.create(
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
            target_product_id = target_product.id  # Get actual ID after creation
            print(f"  Created target product: {target_product_id}")
        except stripe.error.InvalidRequestError as e:
            # Handle cases where the product might already exist (e.g., idempotency)
            if "resource_already_exists" in str(e):
                print(
                    f"  Product {product_id} already exists in the target account. Skipping creation."
                )
                # In case it exists, ensure we use the correct ID
                target_product_id = product_id
            else:
                print(f"  Error creating product {product_id}: {e}")
                return None
        except stripe.error.StripeError as e:
            print(f"  Error creating product {product_id}: {e}")
            return None

    price_map: Dict[str, str] = {}
    # Retrieve active prices for the product from the source account
    try:
        prices = source_stripe.prices.list(
            params={"product": product_id, "active": True, "limit": 100}
        )
        print(f"  Found {len(prices.data)} active price(s) for product {product_id}")

        # Create prices for the target product in the target account
        for price in prices.auto_paging_iter():
            source_price_id = price.id
            print(f"    Processing price: {source_price_id}")
            target_price_id_placeholder = (
                f"target_{source_price_id}"  # Placeholder for dry run
            )

            if dry_run:
                print(
                    f"      [Dry Run] Would create price for product {target_product_id}"
                )
                print(f"        Source Price ID: {source_price_id}")
                print(
                    f"        Currency: {price.currency}, Amount: {price.unit_amount}"
                )
                # Check if price exists in target account even in dry run
                try:
                    # Attempt to construct a potential target ID or check by metadata if possible
                    # For simplicity, we'll just log the check intention.
                    # A more robust check might involve listing prices for the product
                    # and checking metadata, but that adds complexity.
                    print(
                        f"      [Dry Run] Checking if price similar to {source_price_id} exists..."
                    )
                    # Simplified check - assumes ID might be the same or predictable
                    try:
                        existing_price = target_stripe.prices.retrieve(source_price_id)
                        print(
                            f"      [Dry Run] Price {source_price_id} might already exist in the target account."
                        )
                        price_map[source_price_id] = (
                            existing_price.id
                        )  # Map to existing if found by ID
                    except stripe.error.InvalidRequestError as e:
                        if "No such price" in str(e):
                            print(
                                f"      [Dry Run] Price {source_price_id} does not exist by that ID."
                            )
                            price_map[source_price_id] = (
                                target_price_id_placeholder  # Map to placeholder
                            )
                        else:
                            print(
                                f"      [Dry Run] Error checking for existing price {source_price_id}: {e}"
                            )
                            price_map[source_price_id] = (
                                target_price_id_placeholder  # Map to placeholder on error
                            )
                    except stripe.error.StripeError as e:
                        print(
                            f"      [Dry Run] Error checking for existing price {source_price_id}: {e}"
                        )
                        price_map[source_price_id] = (
                            target_price_id_placeholder  # Map to placeholder on error
                        )

                except stripe.error.StripeError as e:
                    print(
                        f"      [Dry Run] Error checking for existing price {source_price_id}: {e}"
                    )
                    price_map[source_price_id] = (
                        target_price_id_placeholder  # Map to placeholder on error
                    )

            else:  # Actual creation logic
                try:
                    # Check if a price with the same source_price_id metadata already exists
                    existing_target_price = None
                    try:
                        target_prices = target_stripe.prices.list(
                            params={
                                "product": target_product_id,
                                "active": True,
                                "limit": 100,
                            }
                        )
                        for p in target_prices.auto_paging_iter():
                            if (
                                p.metadata
                                and p.metadata.get("source_price_id") == source_price_id
                            ):
                                existing_target_price = p
                                break
                    except stripe.error.StripeError as list_err:
                        print(
                            f"      Warning: Could not list target prices for product {target_product_id} to check existence: {list_err}"
                        )
                        # Continue to attempt creation, relying on creation errors

                    if existing_target_price:
                        print(
                            f"      Target price linked to source {source_price_id} already exists: {existing_target_price.id}"
                        )
                        price_map[source_price_id] = existing_target_price.id
                    else:
                        # Create the price if it doesn't exist
                        target_price = target_stripe.prices.create(
                            params={
                                "currency": price.currency,
                                "active": price.active,
                                "metadata": {
                                    **(
                                        price.metadata.to_dict_recursive()
                                        if price.metadata
                                        else {}
                                    ),
                                    "source_price_id": source_price_id,
                                },
                                "nickname": price.get("nickname"),
                                "product": target_product_id,
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
                            f"      Created target price: {target_price.id} (linked to source: {source_price_id})"
                        )
                        price_map[source_price_id] = target_price.id
                except stripe.error.InvalidRequestError as e:
                    # Price creation doesn't usually throw 'resource_already_exists' like products
                    # If a price with identical parameters under the target product exists,
                    # Stripe might create it anyway or error differently.
                    # The check above using metadata is more reliable.
                    print(
                        f"      Error creating price (linked to source: {source_price_id}) for product {target_product_id}: {e}"
                    )
                    # Decide if we should skip mapping or try to find a potential match
                    # For simplicity, we'll skip mapping on error during live run.
                except stripe.error.StripeError as e:
                    print(
                        f"      Error creating price (linked to source: {source_price_id}) for product {target_product_id}: {e}"
                    )

    except stripe.error.StripeError as e:
        print(f"  Error fetching prices for product {product_id}: {e}")

    return price_map


def migrate_products(dry_run: bool = True) -> None:
    """
    Migrates all active products and their active prices from the source Stripe
    account to the target Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    print(f"Starting product and price migration (dry_run={dry_run})...")
    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    try:
        products = source_stripe.products.list(params={"active": True, "limit": 100})
        print(f"Found {len(products.data)} active product(s) in the source account.")
        # Loop through each product and create it in the target account
        for product in products.auto_paging_iter():
            create_product_and_prices(product, source_stripe, target_stripe, dry_run)

        print(f"\nProduct and price migration completed (dry_run={dry_run}).")

    except stripe.error.StripeError as e:
        print(f"Error fetching products from source account: {e}")


def migrate_coupons(dry_run: bool = True) -> None:
    """
    Migrates all valid coupons from the source Stripe account to the target Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    print(f"\nStarting coupon migration (dry_run={dry_run})...")
    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    try:
        coupons = source_stripe.coupons.list(params={"limit": 100})
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
                        target_stripe.coupons.retrieve(coupon_id)
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
                    target_coupon = target_stripe.coupons.create(
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
                    print(f"    Migrated coupon: {target_coupon.id}")
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
        print(f"Error fetching coupons from source account: {e}")


def migrate_promocodes(dry_run: bool = True) -> None:
    """
    Migrates all active promotion codes from the source Stripe account to the target
    Stripe account. Assumes coupons have already been migrated.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    print(f"\nStarting promotion code migration (dry_run={dry_run})...")
    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    try:
        promo_codes = source_stripe.promotion_codes.list(params={"limit": 100})
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
                    target_stripe.coupons.retrieve(coupon_id)
                    print(f"    Associated coupon {coupon_id} found in target account.")
                except stripe.error.InvalidRequestError as e:
                    print(
                        f"    Skipping promocode {promo_code_code}: Associated coupon {coupon_id} not found in target account. Error: {e}"
                    )
                    skipped_count += 1
                    continue  # Skip this promo code if coupon doesn't exist

                # Check if promo code with the same code already exists in target
                existing_promo_code = None
                try:
                    existing_codes = target_stripe.promotion_codes.list(
                        params={"code": promo_code_code, "active": True, "limit": 1}
                    )
                    if existing_codes and existing_codes.data:
                        existing_promo_code = existing_codes.data[0]
                except stripe.error.StripeError as list_err:
                    print(
                        f"    Warning: Could not list target promo codes to check existence for {promo_code_code}: {list_err}"
                    )
                    # Decide how to proceed: skip or attempt creation?
                    # For safety, let's skip if the check fails, unless it's a dry run
                    if not dry_run:
                        skipped_count += 1
                        continue
                    # In dry run, we can note the check failed but continue simulation
                    print(
                        f"    [Dry Run] Check failed, assuming {promo_code_code} doesn't exist for simulation."
                    )

                if existing_promo_code:
                    print(
                        f"    Skipping promocode {promo_code_code}: Active code already exists in target account (ID: {existing_promo_code.id})."
                    )
                    skipped_count += 1
                    continue

                # Dry run simulation or actual creation
                if dry_run:
                    # We already established it doesn't exist (or check failed) in the block above
                    print(
                        f"    [Dry Run] Would create promotion code: {promo_code_code} for coupon {coupon_id}"
                    )
                    migrated_count += 1
                    continue  # Skip actual creation

                # Actual creation logic (only runs if not dry_run and code doesn't exist)
                try:
                    target_promo_code = target_stripe.promotion_codes.create(
                        params={
                            "coupon": coupon_id,
                            "code": promo_code_code,
                            "metadata": {
                                **(
                                    promo_code.metadata.to_dict_recursive()
                                    if promo_code.metadata
                                    else {}
                                ),
                                "source_promotion_code_id": promo_code_id,
                            },
                            "active": promo_code.active,  # Should always be true based on outer check
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
                    print(
                        f"    Migrated promotion code: {target_promo_code.code} (ID: {target_promo_code.id})"
                    )
                    migrated_count += 1
                except stripe.error.InvalidRequestError as e:
                    # This might still catch race conditions or other creation issues
                    # but the primary existence check is now done above.
                    print(f"    Error migrating promotion code {promo_code_code}: {e}")
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
        print(f"Error fetching promotion codes from source account: {e}")


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
