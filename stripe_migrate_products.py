import os
import logging
from typing import Any, Dict, Optional, Set

import stripe
from dotenv import load_dotenv
from stripe import StripeClient

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


def get_stripe_client(api_key: str) -> StripeClient:
    """
    Returns a Stripe client initialized with the given API key.

    Args:
        api_key: The Stripe API key to use.

    Returns:
        An initialized Stripe client object.
    """
    return stripe.StripeClient(api_key=api_key)


# Helper function to find/create target price
def _find_or_create_target_price(
    source_price: Dict[str, Any],
    target_product_id: str,
    target_stripe: StripeClient,
    dry_run: bool,
) -> Optional[str]:
    """
    Checks if a target price corresponding to the source price exists,
    otherwise creates it (or simulates creation in dry run).

    Checks first by 'source_price_id' metadata, then attempts creation.
    Returns the target price ID if found or created, None if creation failed or skipped.
    """
    source_price_id = source_price.id
    target_price_id = None  # Default to None

    # 1. Check if price linked by metadata exists
    existing_target_price_meta = None
    try:
        target_prices = target_stripe.prices.list(
            params={
                "product": target_product_id,
                "active": True,  # Only check active prices
                "limit": 100,
            }
        )
        for p in target_prices.auto_paging_iter():
            if p.metadata and p.metadata.get("source_price_id") == source_price_id:
                existing_target_price_meta = p
                break
    except stripe.error.StripeError as list_err:
        logging.warning(
            "      Warning: Could not list target prices for product %s to check existence by metadata: %s",
            target_product_id,
            list_err,
        )
        # Proceed, creation attempt might still work/fail appropriately

    if existing_target_price_meta:
        target_price_id = existing_target_price_meta.id
        log_prefix = "[Dry Run] " if dry_run else ""
        logging.info(
            "      %sPrice linked via metadata %s already exists: %s. Using existing.",
            log_prefix,
            source_price_id,
            target_price_id,
        )
        return target_price_id  # Found by metadata, return ID

    # 2. If not found by metadata, simulate or attempt creation
    if dry_run:
        logging.info(
            "      [Dry Run] Price linked via metadata %s not found.", source_price_id
        )
        # Optional: Add check by ID for dry run logging (less reliable)
        try:
            target_stripe.prices.retrieve(source_price_id)
            logging.info(
                "      [Dry Run] Price %s might exist by ID (less reliable check), but not linked by metadata.",
                source_price_id,
            )
        except stripe.error.InvalidRequestError as e_inner:
            if "No such price" in str(e_inner):
                logging.info(
                    "      [Dry Run] Price %s does not exist by ID either.",
                    source_price_id,
                )
            else:
                logging.warning(
                    "      [Dry Run] Error checking for existing price %s by ID: %s",
                    source_price_id,
                    e_inner,
                )
        except stripe.error.StripeError as e_inner:
            logging.error(
                "      [Dry Run] Stripe error checking for existing price %s by ID: %s",
                source_price_id,
                e_inner,
            )

        logging.info(
            "      [Dry Run] Would attempt to create price for product %s (linked to source %s)",
            target_product_id,
            source_price_id,
        )
        # Return None in dry run if not found, as no ID is generated/mapped
        return None

    else:  # Actual creation logic
        logging.info(
            "      Target price linked to source %s not found by metadata. Creating.",
            source_price_id,
        )
        try:
            # Prepare parameters
            price_params = {
                "currency": source_price.currency,
                "active": source_price.active,
                "metadata": {
                    **(
                        source_price.metadata.to_dict_recursive()
                        if source_price.metadata
                        else {}
                    ),
                    "source_price_id": source_price_id,
                },
                "nickname": source_price.get("nickname"),
                "product": target_product_id,
                "recurring": source_price.get("recurring"),
                "tax_behavior": source_price.get("tax_behavior"),
                "unit_amount": source_price.get("unit_amount"),
                "billing_scheme": source_price.billing_scheme,
                "tiers": source_price.get("tiers"),
                "tiers_mode": source_price.get("tiers_mode"),
                "transform_quantity": source_price.get("transform_quantity"),
                "custom_unit_amount": source_price.get("custom_unit_amount"),
            }
            price_params = {k: v for k, v in price_params.items() if v is not None}

            logging.debug("      Creating price with params: %s", price_params)
            target_price = target_stripe.prices.create(params=price_params)
            target_price_id = target_price.id
            logging.info(
                "      Created target price: %s (linked to source: %s)",
                target_price_id,
                source_price_id,
            )
            return target_price_id
        except stripe.error.StripeError as e:
            logging.error(
                "      Error creating price (linked to source: %s) for product %s: %s",
                source_price_id,
                target_product_id,
                e,
            )
            return None  # Creation failed


# Function to create products and prices in the target account
def create_product_and_prices(
    product: Dict[str, Any],
    source_stripe: StripeClient,
    target_stripe: StripeClient,
    existing_target_product_ids: Set[str],
    dry_run: bool = True,
) -> bool:
    """
    Creates a product and its associated active prices from the source account
    in the target Stripe account, utilizing a pre-fetched set of existing target IDs.

    Args:
        product: The product object from the source Stripe account.
        source_stripe: Initialized Stripe client for the source account.
        target_stripe: Initialized Stripe client for the target account.
        existing_target_product_ids: Set of existing active product IDs in the target account.
        dry_run: If True, simulates the process without creating resources.

    Returns:
        True if the product and its prices were processed successfully (or simulated),
        False if product creation failed or fetching source prices failed.
    """
    product_id = product.id
    logging.info("Processing product: %s (%s)", product.name, product_id)

    target_product_id = product_id
    target_product = None  # Initialize target_product

    # --- Product Handling (Dry Run / Live) ---
    if dry_run:
        logging.info(
            "  [Dry Run] Would process product: %s (%s)", product.name, product_id
        )
        if product_id in existing_target_product_ids:
            logging.info(
                "  [Dry Run] Product %s already exists in the target account (based on pre-fetched list).",
                product_id,
            )
        else:
            logging.info(
                "  [Dry Run] Product %s does not exist yet in the target account (based on pre-fetched list). Would create.",
                product_id,
            )
        # In dry run, proceed to price simulation regardless of product "existence"
    else:
        # Actual creation logic: Use the pre-fetched set
        product_exists = product_id in existing_target_product_ids

        if product_exists:
            logging.info(
                "  Product %s exists in target account (based on pre-fetched list). Skipping product creation.",
                product_id,
            )
            # We still need the target_product_id which is the same as product_id
            # No API call needed here to retrieve the product object itself.
        else:
            logging.info(
                "  Product %s does not exist in target account (based on pre-fetched list). Creating.",
                product_id,
            )
            try:
                product_params = {
                    "id": product_id,
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
                # Use the ID from the created product, though it should match product_id
                target_product_id = target_product.id
                logging.info("  Created target product: %s", target_product_id)
            except stripe.error.InvalidRequestError as create_err:
                if "resource_already_exists" in str(create_err):
                    logging.warning(
                        "  Product %s was created between list and attempt, or list failed. Assuming it exists.",
                        product_id,
                    )
                    # target_product_id remains product_id, continue to price creation
                else:
                    logging.error(
                        "  Error creating product %s: %s", product_id, create_err
                    )
                    return False  # Cannot proceed if product creation fails
            except stripe.error.StripeError as create_err:
                logging.error("  Error creating product %s: %s", product_id, create_err)
                return False  # Cannot proceed if product creation fails

    # --- Price Handling (Dry Run / Live) ---
    try:
        # Fetch active prices from the source account
        prices = source_stripe.prices.list(
            params={"product": product_id, "active": True, "limit": 100}
        )
        logging.info(
            "  Found %d active price(s) for source product %s",
            len(prices.data),
            product_id,
        )

        # Process each source price using the helper function
        for price in prices.auto_paging_iter():
            source_price_id = price.id
            logging.info("    Processing source price: %s", source_price_id)

            target_price_id = _find_or_create_target_price(
                price, target_product_id, target_stripe, dry_run
            )

            # The helper function logs details about finding/creating/skipping the price.
            # We don't store the result here anymore.
            if not target_price_id and not dry_run:
                logging.warning(
                    "      Failed to find or create target price for source %s",
                    source_price_id,
                )
                # Optionally, decide if a single price failure should cause the whole product processing to return False
                # Current logic: continue processing other prices, return True at the end unless source price fetch failed.

    except stripe.error.StripeError as e:
        logging.error(
            "  Error fetching source prices for product %s: %s", product_id, e
        )
        # Depending on requirements, you might want to return None or an empty map
        # If fetching source prices fails, no prices can be mapped.
        return False  # Fetching source prices failed

    # Return True if product creation (if attempted) and source price fetch were successful
    return True


def migrate_products(dry_run: bool = True) -> None:
    """
    Migrates all active products and their active prices from the source Stripe
    account to the target Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    logging.info("Starting product and price migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)
    target_stripe = get_stripe_client(API_KEY_TARGET)

    processed_count = 0
    failed_count = 0

    try:
        # Fetch existing active product IDs from the target account first
        logging.info("Fetching existing active product IDs from target account...")
        existing_target_product_ids = set()
        try:
            target_products_list = target_stripe.products.list(
                params={"active": True, "limit": 100}
            )
            for prod in target_products_list.auto_paging_iter():
                existing_target_product_ids.add(prod.id)
            logging.info(
                "Found %d existing active products in target account.",
                len(existing_target_product_ids),
            )
        except stripe.error.StripeError as e:
            logging.error(
                "Failed to list products from target account: %s. Proceeding without pre-check set.",
                e,
            )
            # If listing fails, the inner check in create_product_and_prices will still work (less efficient)

        # Fetch active products from the source account
        logging.info("Fetching active products from source account...")
        products = source_stripe.products.list(params={"active": True, "limit": 100})
        product_list = list(
            products.auto_paging_iter()
        )  # Convert iterator to list to get count easily
        logging.info(
            "Found %d active product(s) in the source account.", len(product_list)
        )

        # Loop through each product and create it in the target account
        for product in product_list:
            success = create_product_and_prices(
                product,
                source_stripe,
                target_stripe,
                existing_target_product_ids,  # Pass the set of IDs
                dry_run,
            )
            processed_count += 1
            if not success and not dry_run:  # Count failure only in live run
                failed_count += 1
            # Skips are handled/logged within create_product_and_prices

        logging.info("Product and price migration completed (dry_run=%s).", dry_run)
        logging.info(
            "  Products Processed: %d, Products Failed (live run): %d",
            processed_count,
            failed_count,
        )

    except stripe.error.StripeError as e:
        logging.error("Error fetching products from source account: %s", e)


def migrate_coupons(dry_run: bool = True) -> None:
    """
    Migrates all valid coupons from the source Stripe account to the target Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    logging.info("Starting coupon migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)
    target_stripe = get_stripe_client(API_KEY_TARGET)

    migrated_count = 0
    skipped_count = 0
    failed_count = 0

    try:
        # Fetch existing coupon IDs from the target account first
        logging.info("Fetching existing coupon IDs from target account...")
        existing_target_coupon_ids = set()
        try:
            target_coupons_list = target_stripe.coupons.list(params={"limit": 100})
            for cpn in target_coupons_list.auto_paging_iter():
                existing_target_coupon_ids.add(cpn.id)
            logging.info(
                "Found %d existing coupons in target account.",
                len(existing_target_coupon_ids),
            )
        except stripe.error.StripeError as e:
            logging.error(
                "Failed to list coupons from target account: %s. Proceeding without pre-check set.",
                e,
            )
            # If listing fails, the inner check will still work (less efficient)

        # Fetch coupons from source account
        logging.info("Fetching coupons from source account...")
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
                logging.info(
                    "    [Dry Run] Would process coupon: %s (%s)",
                    coupon_name,
                    coupon_id,
                )
                # Check if coupon exists using the pre-fetched set
                if coupon_id in existing_target_coupon_ids:
                    logging.info(
                        "    [Dry Run] Coupon %s already exists in target (based on pre-fetched list). Would skip.",
                        coupon_id,
                    )
                    skipped_count += 1
                else:
                    logging.info(
                        "    [Dry Run] Coupon %s does not exist yet in target (based on pre-fetched list). Would create.",
                        coupon_id,
                    )
                    migrated_count += 1  # Count as would-be migrated
                continue  # Skip actual creation logic in dry run

            # Actual creation logic (only runs if not dry_run)
            # Use the pre-fetched set to check existence
            if coupon_id in existing_target_coupon_ids:
                logging.info(
                    "    Coupon %s exists in target (based on pre-fetched list). Skipping creation.",
                    coupon_id,
                )
                skipped_count += 1
                continue  # Move to the next coupon

            # Coupon does not exist in target (based on list), proceed with creation
            logging.info(
                "    Coupon %s does not exist in target (based on pre-fetched list). Creating.",
                coupon_id,
            )
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
                    "    Creating coupon %s with params: %s",
                    coupon_id,
                    coupon_params,
                )
                target_coupon = target_stripe.coupons.create(params=coupon_params)
                logging.info("    Migrated coupon: %s", target_coupon.id)
                migrated_count += 1
            except stripe.error.InvalidRequestError as create_err:
                # Handle potential creation errors (e.g., race condition or list failure)
                if "resource_already_exists" in str(create_err):
                    logging.warning(
                        "    Coupon %s was created between list and attempt, or list failed. Skipping.",
                        coupon_id,
                    )
                    skipped_count += 1
                else:
                    logging.error(
                        "    Error migrating coupon %s: %s", coupon.id, create_err
                    )
                    failed_count += 1
            except stripe.error.StripeError as create_err:
                logging.error(
                    "    Error migrating coupon %s: %s", coupon.id, create_err
                )
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
    source_stripe = get_stripe_client(API_KEY_SOURCE)
    target_stripe = get_stripe_client(API_KEY_TARGET)

    migrated_count = 0
    skipped_count = 0
    failed_count = 0

    try:
        # Fetch existing target coupon IDs first
        logging.info("Fetching existing target coupon IDs for promo code check...")
        existing_target_coupon_ids = set()
        try:
            target_coupons_list = target_stripe.coupons.list(params={"limit": 100})
            for cpn in target_coupons_list.auto_paging_iter():
                existing_target_coupon_ids.add(cpn.id)
            logging.info(
                "Found %d existing coupons in target account.",
                len(existing_target_coupon_ids),
            )
        except stripe.error.StripeError as e:
            logging.error(
                "Failed to list coupons from target account: %s. Cannot reliably check promo codes.",
                e,
            )
            return  # Cannot proceed without coupon list

        # Fetch existing active target promo codes (the actual code strings)
        logging.info("Fetching existing active promo codes from target account...")
        existing_target_promo_codes = set()
        try:
            target_promos_list = target_stripe.promotion_codes.list(
                params={"active": True, "limit": 100}
            )
            for pc in target_promos_list.auto_paging_iter():
                existing_target_promo_codes.add(pc.code)
            logging.info(
                "Found %d existing active promo codes in target account.",
                len(existing_target_promo_codes),
            )
        except stripe.error.StripeError as e:
            logging.error(
                "Failed to list promo codes from target account: %s. Proceeding without pre-check set.",
                e,
            )
            # If listing fails, the inner creation logic might still fail on duplicates

        # Fetch active promo codes from source account
        logging.info("Fetching active promo codes from source account...")
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
                "  Processing promotion code: %s (ID: %s, Coupon: %s)",
                promo_code_code,
                promo_code_id,
                coupon_id,
            )

            # Check for coupon existence using the pre-fetched set
            if coupon_id not in existing_target_coupon_ids:
                logging.warning(
                    "    Skipping promocode %s: Associated coupon %s not found in target account (based on pre-fetched list).",
                    promo_code_code,
                    coupon_id,
                )
                skipped_count += 1
                continue  # Skip this promo code

            # Check if promo code with the same code already exists in target using the pre-fetched set
            code_exists = promo_code_code in existing_target_promo_codes

            # Dry run simulation or actual creation
            if dry_run:
                if code_exists:
                    logging.info(
                        "    [Dry Run] Skipping promocode %s: Code already exists in target (based on pre-fetched list).",
                        promo_code_code,
                    )
                    skipped_count += 1
                else:
                    logging.info(
                        "    [Dry Run] Would create promotion code: %s for coupon %s (Code doesn't exist in pre-fetched list)",
                        promo_code_code,
                        coupon_id,
                    )
                    migrated_count += 1
                continue  # Skip actual creation

            # Actual creation logic (only runs if not dry_run)
            if code_exists:
                logging.info(
                    "    Skipping promocode %s: Code already exists in target (based on pre-fetched list).",
                    promo_code_code,
                )
                skipped_count += 1
                continue

            # Code does not exist, proceed with creation
            logging.info(
                "    Promo code %s does not exist in target (based on pre-fetched list). Creating.",
                promo_code_code,
            )
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
