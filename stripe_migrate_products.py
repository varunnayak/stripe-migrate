import os
import logging
from typing import Any, Dict, Optional

import stripe
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

# Load API keys from environment variables
API_KEY_SOURCE: Optional[str] = os.getenv("API_KEY_SOURCE")
API_KEY_TARGET: Optional[str] = os.getenv("API_KEY_TARGET")

# Ensure API keys are set
if not API_KEY_SOURCE:
    logging.error("API_KEY_SOURCE environment variable not set.")
    raise ValueError("API_KEY_SOURCE environment variable not set.")
if not API_KEY_TARGET:
    logging.error("API_KEY_TARGET environment variable not set.")
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
    logging.info("Processing product: %s (%s)", product.name, product_id)

    target_product_id = product_id  # Assume the same ID for dry run mapping

    if dry_run:
        logging.info(
            "  [Dry Run] Would create product: %s (%s)", product.name, product_id
        )
        # Check if product exists in target account even in dry run for better simulation
        try:
            target_stripe.products.retrieve(product_id)
            logging.info(
                "  [Dry Run] Product %s already exists in the target account.",
                product_id,
            )
        except stripe.error.InvalidRequestError as e:
            if "No such product" in str(e):
                logging.info(
                    "  [Dry Run] Product %s does not exist yet in the target account.",
                    product_id,
                )
            else:
                # Log other potential errors during check
                logging.warning(
                    "  [Dry Run] Error checking for existing product %s: %s",
                    product_id,
                    e,
                )
        except stripe.error.StripeError as e:
            logging.error(
                "  [Dry Run] Error checking for existing product %s: %s", product_id, e
            )
    else:
        try:
            # Create the product in the target account
            # Explicitly list parameters for clarity
            product_params = {
                "id": product_id,  # Use the same ID
                "name": product.name,
                "active": product.get("active", True),
                "description": product.get("description"),
                "metadata": (
                    product.metadata.to_dict_recursive() if product.metadata else {}
                ),
                "tax_code": product.get("tax_code"),
            }
            logging.debug("  Creating product with params: %s", product_params)
            target_product = target_stripe.products.create(params=product_params)
            target_product_id = target_product.id  # Get actual ID after creation
            logging.info("  Created target product: %s", target_product_id)
        except stripe.error.InvalidRequestError as e:
            # Handle cases where the product might already exist (e.g., idempotency)
            if "resource_already_exists" in str(e):
                logging.info(
                    "  Product %s already exists in the target account. Re-fetching and using existing.",
                    product_id,
                )
                # In case it exists, ensure we use the correct ID and fetch the object
                try:
                    target_product = target_stripe.products.retrieve(product_id)
                    target_product_id = target_product.id
                except stripe.error.StripeError as retrieve_err:
                    logging.error(
                        "  Failed to retrieve existing product %s: %s",
                        product_id,
                        retrieve_err,
                    )
                    return None  # Cannot proceed without the product
            else:
                logging.error("  Error creating product %s: %s", product_id, e)
                return None
        except stripe.error.StripeError as e:
            logging.error("  Error creating product %s: %s", product_id, e)
            return None

    price_map: Dict[str, str] = {}
    # Retrieve active prices for the product from the source account
    try:
        prices = source_stripe.prices.list(
            params={"product": product_id, "active": True, "limit": 100}
        )
        logging.info(
            "  Found %d active price(s) for product %s", len(prices.data), product_id
        )

        # Create prices for the target product in the target account
        for price in prices.auto_paging_iter():
            source_price_id = price.id
            logging.info("    Processing price: %s", source_price_id)
            target_price_id_placeholder = f"target_{source_price_id}"  # Placeholder for dry run - f-string ok here as it's not logging directly

            if dry_run:
                logging.info(
                    "      [Dry Run] Would create price for product %s",
                    target_product_id,
                )
                logging.info("        Source Price ID: %s", source_price_id)
                logging.info(
                    "        Currency: %s, Amount: %s",
                    price.currency,
                    price.unit_amount,
                )
                # Check if price exists in target account even in dry run
                try:
                    logging.info(
                        "      [Dry Run] Checking if price similar to %s exists...",
                        source_price_id,
                    )
                    # Check by metadata first for a more reliable check
                    target_prices_dry = target_stripe.prices.list(
                        params={
                            "product": target_product_id,
                            "active": True,
                            "limit": 100,
                        }
                    )
                    found_by_meta = False
                    for p_dry in target_prices_dry.auto_paging_iter():
                        if (
                            p_dry.metadata
                            and p_dry.metadata.get("source_price_id") == source_price_id
                        ):
                            logging.info(
                                "      [Dry Run] Price linked via metadata %s already exists: %s",
                                source_price_id,
                                p_dry.id,
                            )
                            price_map[source_price_id] = p_dry.id
                            found_by_meta = True
                            break
                    if not found_by_meta:
                        # Fallback check by ID (less reliable, might match unrelated price)
                        try:
                            existing_price = target_stripe.prices.retrieve(
                                source_price_id
                            )
                            logging.info(
                                "      [Dry Run] Price %s might exist by ID (less reliable check).",
                                source_price_id,
                            )
                            # Avoid mapping based on ID alone in dry run unless confirmed by metadata
                            price_map[source_price_id] = target_price_id_placeholder
                        except stripe.error.InvalidRequestError as e_inner:
                            if "No such price" in str(e_inner):
                                logging.info(
                                    "      [Dry Run] Price %s does not exist by that ID.",
                                    source_price_id,
                                )
                                price_map[source_price_id] = target_price_id_placeholder
                            else:
                                logging.warning(
                                    "      [Dry Run] Error checking for existing price %s by ID: %s",
                                    source_price_id,
                                    e_inner,
                                )
                                price_map[source_price_id] = target_price_id_placeholder
                        except stripe.error.StripeError as e_inner:
                            logging.error(
                                "      [Dry Run] Stripe error checking for existing price %s by ID: %s",
                                source_price_id,
                                e_inner,
                            )
                            price_map[source_price_id] = target_price_id_placeholder

                except stripe.error.StripeError as e:
                    logging.error(
                        "      [Dry Run] Error listing target prices for check %s: %s",
                        source_price_id,
                        e,
                    )
                    price_map[source_price_id] = target_price_id_placeholder

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
                        logging.warning(
                            "      Warning: Could not list target prices for product %s to check existence: %s",
                            target_product_id,
                            list_err,
                        )
                        # Continue to attempt creation, relying on creation errors

                    if existing_target_price:
                        logging.info(
                            "      Target price linked to source %s already exists: %s. Using existing.",
                            source_price_id,
                            existing_target_price.id,
                        )
                        price_map[source_price_id] = existing_target_price.id
                    else:
                        # Create the price if it doesn't exist
                        # Consolidate all potential price attributes
                        price_params = {
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
                            # Ensure custom_unit_amount is included if present
                            "custom_unit_amount": price.get("custom_unit_amount"),
                        }
                        # Remove None values to avoid sending empty optional params
                        price_params = {
                            k: v for k, v in price_params.items() if v is not None
                        }

                        logging.debug(
                            "      Creating price with params: %s", price_params
                        )
                        target_price = target_stripe.prices.create(params=price_params)
                        logging.info(
                            "      Created target price: %s (linked to source: %s)",
                            target_price.id,
                            source_price_id,
                        )
                        price_map[source_price_id] = target_price.id
                except stripe.error.StripeError as e:
                    logging.error(
                        "      Error processing price (linked to source: %s) for product %s: %s",
                        source_price_id,
                        target_product_id,
                        e,
                    )
                    # Skip mapping on error during live run.

    except stripe.error.StripeError as e:
        logging.error("  Error fetching prices for product %s: %s", product_id, e)

    return price_map


def migrate_products(dry_run: bool = True) -> None:
    """
    Migrates all active products and their active prices from the source Stripe
    account to the target Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    logging.info("Starting product and price migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    processed_count = 0
    skipped_count = 0
    failed_count = 0
    price_maps_aggregated: Dict[str, str] = {}

    try:
        products = source_stripe.products.list(params={"active": True, "limit": 100})
        product_list = list(
            products.auto_paging_iter()
        )  # Convert iterator to list to get count easily
        logging.info(
            "Found %d active product(s) in the source account.", len(product_list)
        )

        # Loop through each product and create it in the target account
        for product in product_list:
            price_map = create_product_and_prices(
                product, source_stripe, target_stripe, dry_run
            )
            processed_count += 1
            if price_map is not None:
                price_maps_aggregated.update(price_map)
            elif not dry_run:  # If it's a live run and price_map is None, it failed
                failed_count += 1
            else:  # If it's a dry run and price_map is None, it was simulated or skipped
                skipped_count += (
                    1  # Refine this if create_product_and_prices gives more status
                )

        logging.info("Product and price migration completed (dry_run=%s).", dry_run)
        logging.info(
            "  Processed: %d, Failed: %d, Skipped (dry run): %d",
            processed_count,
            failed_count,
            skipped_count,
        )
        logging.info("  Total Price Mappings generated: %d", len(price_maps_aggregated))

    except stripe.error.StripeError as e:
        logging.error("Error fetching products from source account: %s", e)


def migrate_coupons(dry_run: bool = True) -> None:
    """
    Migrates all valid coupons from the source Stripe account to the target Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    logging.info("Starting coupon migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    migrated_count = 0
    skipped_count = 0
    failed_count = 0

    try:
        coupons = source_stripe.coupons.list(params={"limit": 100})
        coupon_list = list(coupons.auto_paging_iter())
        logging.info("Found %d coupon(s) in the source account.", len(coupon_list))

        for coupon in coupon_list:
            coupon_id = coupon.id
            coupon_name = coupon.name or coupon_id

            if not coupon.valid:
                logging.info("  Skipping invalid coupon: %s", coupon_name)
                skipped_count += 1
                continue

            logging.info("  Processing coupon: %s", coupon_name)

            if dry_run:
                logging.info("    [Dry Run] Would create coupon: %s", coupon_name)
                # Check if coupon exists
                try:
                    target_stripe.coupons.retrieve(coupon_id)
                    logging.info(
                        "    [Dry Run] Coupon %s already exists. Would skip.", coupon_id
                    )
                    skipped_count += 1
                except stripe.error.InvalidRequestError as e:
                    if "No such coupon" in str(e):
                        logging.info(
                            "    [Dry Run] Coupon %s does not exist yet.", coupon_id
                        )
                        migrated_count += 1  # Count as would-be migrated
                    else:
                        logging.warning(
                            "    [Dry Run] Error checking for coupon %s: %s",
                            coupon_id,
                            e,
                        )
                        skipped_count += 1  # Count as skipped due to check error
                except stripe.error.StripeError as e:
                    logging.error(
                        "    [Dry Run] Error checking for coupon %s: %s", coupon_id, e
                    )
                    skipped_count += 1  # Count as skipped due to check error
                continue  # Skip actual creation logic in dry run

            # Actual creation logic (only runs if not dry_run)
            try:
                # Prepare parameters, removing None values
                coupon_params = {
                    "id": coupon.id,  # Use the same ID
                    "amount_off": coupon.get("amount_off"),
                    "currency": coupon.get("currency"),
                    "duration": coupon.duration,
                    "metadata": (
                        coupon.metadata.to_dict_recursive() if coupon.metadata else {}
                    ),
                    "name": coupon.get("name"),
                    "percent_off": coupon.get("percent_off"),
                    "duration_in_months": coupon.get("duration_in_months"),
                    "max_redemptions": coupon.get("max_redemptions"),
                    "redeem_by": coupon.get("redeem_by"),
                    "applies_to": coupon.get(
                        "applies_to"
                    ),  # Include applies_to if exists
                }
                coupon_params = {
                    k: v for k, v in coupon_params.items() if v is not None
                }

                logging.debug(
                    "    Creating coupon %s with params: %s", coupon_id, coupon_params
                )
                target_coupon = target_stripe.coupons.create(params=coupon_params)
                logging.info("    Migrated coupon: %s", target_coupon.id)
                migrated_count += 1
            except stripe.error.InvalidRequestError as e:
                if "resource_already_exists" in str(e):
                    logging.info(
                        "    Coupon %s already exists. Skipping creation.", coupon.id
                    )
                    skipped_count += 1
                else:
                    logging.error("    Error migrating coupon %s: %s", coupon.id, e)
                    failed_count += 1
            except stripe.error.StripeError as e:
                logging.error("    Error migrating coupon %s: %s", coupon.id, e)
                failed_count += 1

        logging.info(
            "Coupon migration completed. Migrated: %d, Skipped: %d, Failed: %d",
            migrated_count,
            skipped_count,
            failed_count,
        )
    except stripe.error.StripeError as e:
        logging.error("Error fetching coupons from source account: %s", e)


def migrate_promocodes(dry_run: bool = True) -> None:
    """
    Migrates all active promotion codes from the source Stripe account to the target
    Stripe account. Assumes coupons have already been migrated.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    logging.info("Starting promotion code migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    migrated_count = 0
    skipped_count = 0
    failed_count = 0

    try:
        promo_codes = source_stripe.promotion_codes.list(params={"limit": 100})
        promo_code_list = list(promo_codes.auto_paging_iter())
        logging.info(
            "Found %d promotion code(s) in the source account.", len(promo_code_list)
        )

        for promo_code in promo_code_list:
            promo_code_id = promo_code.id
            promo_code_code = promo_code.code

            if not promo_code.active:
                logging.info("  Skipping inactive promotion code: %s", promo_code_code)
                skipped_count += 1
                continue

            coupon_id = promo_code.coupon.id
            logging.info(
                "  Processing promotion code: %s (ID: %s)",
                promo_code_code,
                promo_code_id,
            )

            # Check for coupon existence (always do this check, even in dry run)
            try:
                target_stripe.coupons.retrieve(coupon_id)
                logging.debug(
                    "    Associated coupon %s found in target account.", coupon_id
                )
            except stripe.error.InvalidRequestError as e:
                logging.warning(
                    "    Skipping promocode %s: Associated coupon %s not found in target account. Error: %s",
                    promo_code_code,
                    coupon_id,
                    e,
                )
                skipped_count += 1
                continue  # Skip this promo code if coupon doesn't exist
            except stripe.error.StripeError as e:
                logging.error(
                    "    Error checking for associated coupon %s for promocode %s: %s",
                    coupon_id,
                    promo_code_code,
                    e,
                )
                failed_count += (
                    1  # Treat check failure as a failure for this promo code
                )
                continue

            # Check if promo code with the same code already exists in target
            existing_promo_code = None
            try:
                # Use list with 'code' filter for uniqueness check
                existing_codes = target_stripe.promotion_codes.list(
                    params={"code": promo_code_code, "active": True, "limit": 1}
                )
                if existing_codes and existing_codes.data:
                    existing_promo_code = existing_codes.data[0]
            except stripe.error.StripeError as list_err:
                logging.warning(
                    "    Warning: Could not list target promo codes to check existence for %s: %s",
                    promo_code_code,
                    list_err,
                )
                # Decide how to proceed: skip or attempt creation?
                # For safety, let's skip if the check fails, unless it's a dry run
                if not dry_run:
                    skipped_count += 1
                    continue
                # In dry run, we can note the check failed but continue simulation
                logging.info(
                    "    [Dry Run] Check failed, assuming %s doesn't exist for simulation.",
                    promo_code_code,
                )

            if existing_promo_code:
                logging.info(
                    "    Skipping promocode %s: Active code already exists in target account (ID: %s).",
                    promo_code_code,
                    existing_promo_code.id,
                )
                skipped_count += 1
                continue

            # Dry run simulation or actual creation
            if dry_run:
                # We already established it doesn't exist (or check failed) in the block above
                logging.info(
                    "    [Dry Run] Would create promotion code: %s for coupon %s",
                    promo_code_code,
                    coupon_id,
                )
                migrated_count += 1
                continue  # Skip actual creation

            # Actual creation logic (only runs if not dry_run and code doesn't exist)
            try:
                # Prepare parameters, removing None values and handling restrictions
                promo_params = {
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
                        else None
                    ),
                }
                promo_params = {k: v for k, v in promo_params.items() if v is not None}

                logging.debug(
                    "    Creating promo code %s with params: %s",
                    promo_code_code,
                    promo_params,
                )
                target_promo_code = target_stripe.promotion_codes.create(
                    params=promo_params
                )
                logging.info(
                    "    Migrated promotion code: %s (ID: %s)",
                    target_promo_code.code,
                    target_promo_code.id,
                )
                migrated_count += 1
            except stripe.error.InvalidRequestError as e:
                # This might still catch race conditions or other creation issues
                # but the primary existence check is now done above.
                logging.error(
                    "    Error migrating promotion code %s: %s", promo_code_code, e
                )
                failed_count += 1
            except stripe.error.StripeError as e:
                logging.error(
                    "    Error migrating promotion code %s: %s", promo_code_code, e
                )
                failed_count += 1

        logging.info(
            "Promotion code migration completed. Migrated: %d, Skipped: %d, Failed: %d",
            migrated_count,
            skipped_count,
            failed_count,
        )
    except stripe.error.StripeError as e:
        logging.error("Error fetching promotion codes from source account: %s", e)


def main() -> None:
    """Main function to run the product, coupon, and promotion code migrations."""
    import argparse  # Keep argparse import local to main

    parser = argparse.ArgumentParser(
        description="Migrate Stripe Products, Coupons, and Promo Codes."
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Perform the migration live. Default is dry run.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    # Update logging level if debug flag is set
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Debug logging enabled.")

    is_dry_run = not args.live  # dry_run is True if --live is NOT specified

    logging.info("Starting Stripe data migration... (Dry Run: %s)", is_dry_run)
    migrate_products(dry_run=is_dry_run)
    migrate_coupons(dry_run=is_dry_run)
    migrate_promocodes(dry_run=is_dry_run)
    logging.info("Stripe data migration finished. (Dry Run: %s)", is_dry_run)


if __name__ == "__main__":
    main()
