"""Migrates Stripe products, prices, coupons and subscriptions."""

import os
import logging
from typing import Any, Dict, List, Optional, Set
import argparse

import stripe
from dotenv import load_dotenv
from stripe import StripeClient  # Explicitly import StripeClient

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

# Define return status constants for subscription clarity
STATUS_CREATED = "created"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"
STATUS_DRY_RUN = "dry_run"


def get_stripe_client(api_key: str) -> StripeClient:
    """
    Returns a Stripe client initialized with the given API key.

    Args:
        api_key: The Stripe API key to use.

    Returns:
        An initialized Stripe client object.
    """
    # Use stripe.StripeClient directly for type hinting consistency
    return stripe.StripeClient(api_key=api_key)


# --- Product/Price/Coupon/Promo Migration Functions (from stripe_migrate_products.py) ---


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
) -> str:  # Return status string
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
        A status string (STATUS_CREATED, STATUS_SKIPPED, STATUS_FAILED, STATUS_DRY_RUN)
        indicating the result of the operation for this product.
    """
    product_id = product.id
    logging.info("Processing product: %s (%s)", product.name, product_id)

    target_product_id = product_id
    target_product = None  # Initialize target_product
    product_skipped = False  # Initialize flag

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
            # Don't return STATUS_SKIPPED yet, need to process prices
        else:
            logging.info(
                "  [Dry Run] Product %s does not exist yet in the target account (based on pre-fetched list). Would create.",
                product_id,
            )
            # Don't return STATUS_DRY_RUN yet, need to process prices
        # In dry run, proceed to price simulation regardless of product "existence"
        # Final status will be STATUS_DRY_RUN if we get to the end

    else:
        # Actual creation logic: Use the pre-fetched set
        product_exists = product_id in existing_target_product_ids

        if product_exists:
            logging.info(
                "  Product %s exists in target account (based on pre-fetched list). Skipping product creation.",
                product_id,
            )
            product_skipped = True  # Flag that product creation was skipped
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
                    return STATUS_FAILED  # Cannot proceed if product creation fails
            except stripe.error.StripeError as create_err:
                logging.error("  Error creating product %s: %s", product_id, create_err)
                return STATUS_FAILED  # Cannot proceed if product creation fails

    # --- Price Handling (Dry Run / Live) ---
    price_creation_failed = False
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
                price_creation_failed = True

    except stripe.error.StripeError as e:
        logging.error(
            "  Error fetching source prices for product %s: %s", product_id, e
        )
        # If fetching source prices fails, no prices can be mapped.
        return STATUS_FAILED  # Fetching source prices failed

    # Return True if product creation (if attempted) and source price fetch were successful
    # Determine final status based on dry_run, skips, and failures
    if dry_run:
        return STATUS_DRY_RUN
    if price_creation_failed:
        # If any price failed, mark the whole product as failed
        return STATUS_FAILED
    if product_skipped:
        # If product was skipped AND no prices failed, mark as skipped
        return STATUS_SKIPPED
    # If product was created (not skipped) AND no prices failed, mark as created
    return STATUS_CREATED


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
    created_count = 0
    skipped_count = 0
    dry_run_count = 0

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
            logging.error(
                "Cannot reliably migrate products without the list of existing target products. Exiting."
            )
            return

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
            status = create_product_and_prices(
                product,
                source_stripe,
                target_stripe,
                existing_target_product_ids,  # Pass the set of IDs
                dry_run,
            )
            processed_count += 1
            if not status and not dry_run:  # Count failure only in live run
                failed_count += 1
            # Skips are handled/logged within create_product_and_prices

            if status == STATUS_CREATED:
                created_count += 1
            elif status == STATUS_SKIPPED:
                skipped_count += 1
            elif status == STATUS_FAILED:
                failed_count += 1
            elif status == STATUS_DRY_RUN:
                dry_run_count += 1
            else:
                logging.error(
                    "Unknown status '%s' returned for product %s. Treating as failed.",
                    status,
                    product.id,
                )
                failed_count += 1

        logging.info("Product and price migration completed (dry_run=%s).", dry_run)
        logging.info(
            "  Products Processed: %d, Products Failed (live run): %d",
            processed_count,
            failed_count,
        )
        if dry_run:
            logging.info("  Results (dry run) - Would Process: %d", dry_run_count)
            # Note: Dry run doesn't distinguish created/skipped/failed easily in summary
        else:
            logging.info(
                "  Results (live run) - Created: %d, Skipped: %d, Failed: %d",
                created_count,
                skipped_count,
                failed_count,
            )

    except stripe.error.StripeError as e:
        logging.error("Error fetching products from source account: %s", e)


def migrate_coupons(dry_run: bool = True) -> None:
    """
    Migrates all valid coupons and their associated active promotion codes
    from the source Stripe account to the target Stripe account.

    Args:
        dry_run: If True, simulates the process without creating resources.
    """
    logging.info("Starting coupon and promo code migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)
    target_stripe = get_stripe_client(API_KEY_TARGET)

    coupon_migrated_count = 0
    coupon_skipped_count = 0
    coupon_failed_count = 0
    promo_migrated_count = 0
    promo_skipped_count = 0
    promo_failed_count = 0

    try:
        # --- Pre-fetch Target Coupons ---
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
                "Failed to list coupons from target account: %s. Proceeding without coupon pre-check set.",
                e,
            )
            logging.error(
                "Cannot reliably migrate coupons without the list of existing target coupons. Exiting."
            )
            return

        # --- Pre-fetch Target Promo Codes (by Code string) ---
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
                "Failed to list promo codes from target account: %s. Proceeding without promo code pre-check set.",
                e,
            )
            logging.error(
                "Cannot reliably migrate promo codes without the list of existing target promo codes. Exiting."
            )
            return

        # Fetch coupons from source account
        logging.info("Fetching coupons from source account...")
        coupons = source_stripe.coupons.list(params={"limit": 100})
        coupon_list = list(coupons.auto_paging_iter())
        logging.info("Found %d coupon(s) in the source account.", len(coupon_list))

        for coupon in coupon_list:
            coupon_id = coupon.id
            coupon_name = coupon.name or coupon_id
            coupon_processed = False  # Flag to check if coupon itself was processed

            if not coupon.valid:
                logging.info(
                    "  Skipping invalid coupon: %s (and its promo codes)", coupon_name
                )
                coupon_skipped_count += 1
                continue

            logging.info("  Processing coupon: %s (%s)", coupon_name, coupon_id)

            if dry_run:
                logging.info(
                    "    [Dry Run] Would process coupon: %s (%s)",
                    coupon_name,
                    coupon_id,
                )
                if coupon_id in existing_target_coupon_ids:
                    logging.info(
                        "    [Dry Run] Coupon %s already exists in target (based on pre-fetched list). Would skip coupon creation.",
                        coupon_id,
                    )
                    coupon_skipped_count += 1
                    coupon_processed = True  # Coupon exists, can process promo codes
                else:
                    logging.info(
                        "    [Dry Run] Coupon %s does not exist yet in target (based on pre-fetched list). Would create.",
                        coupon_id,
                    )
                    coupon_migrated_count += 1  # Count as would-be migrated
                    coupon_processed = (
                        True  # Coupon would be created, can process promo codes
                    )
                # Continue to promo code dry run regardless of coupon status in dry run

            else:  # Actual coupon creation logic
                if coupon_id in existing_target_coupon_ids:
                    logging.info(
                        "    Coupon %s exists in target (based on pre-fetched list). Skipping coupon creation.",
                        coupon_id,
                    )
                    coupon_skipped_count += 1
                    coupon_processed = True  # Coupon exists, can process promo codes
                else:
                    # Coupon does not exist in target (based on list), proceed with creation
                    logging.info(
                        "    Coupon %s does not exist in target (based on pre-fetched list). Creating.",
                        coupon_id,
                    )
                    try:
                        coupon_params = {
                            "id": coupon.id,
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
                            "max_redemptions": coupon.get("max_redemptions"),
                            "redeem_by": coupon.get("redeem_by"),
                            "applies_to": coupon.get("applies_to"),
                        }
                        coupon_params = {
                            k: v for k, v in coupon_params.items() if v is not None
                        }

                        logging.debug(
                            "    Creating coupon %s with params: %s",
                            coupon_id,
                            coupon_params,
                        )
                        target_coupon = target_stripe.coupons.create(
                            params=coupon_params
                        )
                        logging.info("    Migrated coupon: %s", target_coupon.id)
                        coupon_migrated_count += 1
                        coupon_processed = (
                            True  # Coupon created, can process promo codes
                        )
                    except stripe.error.InvalidRequestError as create_err:
                        if "resource_already_exists" in str(create_err):
                            logging.warning(
                                "    Coupon %s was created between list and attempt, or list failed. Assuming exists.",
                                coupon_id,
                            )
                            coupon_skipped_count += (
                                1  # Treat as skipped if it already exists now
                            )
                            coupon_processed = (
                                True  # Coupon exists now, can process promo codes
                            )
                        else:
                            logging.error(
                                "    Error migrating coupon %s: %s",
                                coupon.id,
                                create_err,
                            )
                            coupon_failed_count += 1
                            # coupon_processed remains False, skip promo codes for this failed coupon
                    except stripe.error.StripeError as create_err:
                        logging.error(
                            "    Error migrating coupon %s: %s", coupon.id, create_err
                        )
                        coupon_failed_count += 1
                        # coupon_processed remains False, skip promo codes for this failed coupon

            # --- Promotion Code Migration (Inside Coupon Loop) ---
            if (
                coupon_processed
            ):  # Only migrate promo codes if the coupon exists (or would exist in dry run)
                try:
                    logging.debug(
                        "    Fetching active promo codes for source coupon %s...",
                        coupon_id,
                    )
                    source_promo_codes = source_stripe.promotion_codes.list(
                        params={"coupon": coupon_id, "active": True, "limit": 100}
                    )
                    promo_code_list = list(source_promo_codes.auto_paging_iter())
                    if promo_code_list:
                        logging.info(
                            "      Found %d active promo code(s) for coupon %s.",
                            len(promo_code_list),
                            coupon_id,
                        )

                    for promo_code in promo_code_list:
                        promo_code_id = promo_code.id
                        promo_code_code = promo_code.code

                        # Check existence using the pre-fetched set of promo codes (by code string)
                        code_exists = promo_code_code in existing_target_promo_codes

                        logging.info(
                            "      Processing promo code: %s (ID: %s)",
                            promo_code_code,
                            promo_code_id,
                        )

                        if dry_run:
                            if code_exists:
                                logging.info(
                                    "        [Dry Run] Skipping promo code %s: Code already exists in target (based on pre-fetched list).",
                                    promo_code_code,
                                )
                                promo_skipped_count += 1
                            else:
                                logging.info(
                                    "        [Dry Run] Would create promotion code: %s for coupon %s (Code doesn't exist in pre-fetched list)",
                                    promo_code_code,
                                    coupon_id,  # Use coupon_id (which is same in source/target)
                                )
                                promo_migrated_count += 1
                            continue  # Next promo code

                        # Actual promo code creation logic (only runs if not dry_run)
                        if code_exists:
                            logging.info(
                                "        Skipping promo code %s: Code already exists in target (based on pre-fetched list).",
                                promo_code_code,
                            )
                            promo_skipped_count += 1
                            continue

                        # Code does not exist, proceed with creation
                        logging.info(
                            "        Promo code %s does not exist in target (based on pre-fetched list). Creating.",
                            promo_code_code,
                        )
                        try:
                            promo_params = {
                                "coupon": coupon_id,  # Use the target coupon ID (same as source)
                                "code": promo_code_code,
                                "metadata": {
                                    **(
                                        promo_code.metadata.to_dict_recursive()
                                        if promo_code.metadata
                                        else {}
                                    ),
                                    "source_promotion_code_id": promo_code_id,
                                },
                                "active": promo_code.active,  # Should be true based on list query
                                "customer": promo_code.get("customer"),
                                "expires_at": promo_code.get("expires_at"),
                                "max_redemptions": promo_code.get("max_redemptions"),
                                "restrictions": (
                                    promo_code.restrictions.to_dict_recursive()
                                    if promo_code.restrictions
                                    else None
                                ),
                            }
                            promo_params = {
                                k: v for k, v in promo_params.items() if v is not None
                            }

                            logging.debug(
                                "        Creating promo code %s with params: %s",
                                promo_code_code,
                                promo_params,
                            )
                            target_promo_code = target_stripe.promotion_codes.create(
                                params=promo_params
                            )
                            logging.info(
                                "        Migrated promotion code: %s (ID: %s)",
                                target_promo_code.code,
                                target_promo_code.id,
                            )
                            promo_migrated_count += 1
                            # Add newly created code to set to prevent duplicate creation attempts within the same run if list failed
                            existing_target_promo_codes.add(target_promo_code.code)
                        except stripe.error.InvalidRequestError as promo_create_err:
                            # Handle race conditions or other creation errors
                            if "already exists" in str(promo_create_err).lower():
                                logging.warning(
                                    "        Promo code %s was created between list and attempt, or list failed. Skipping.",
                                    promo_code_code,
                                )
                                promo_skipped_count += 1
                                # Add code to set if it exists now
                                existing_target_promo_codes.add(promo_code_code)

                            else:
                                logging.error(
                                    "        Error migrating promotion code %s: %s",
                                    promo_code_code,
                                    promo_create_err,
                                )
                                promo_failed_count += 1
                        except stripe.error.StripeError as promo_create_err:
                            logging.error(
                                "        Error migrating promotion code %s: %s",
                                promo_code_code,
                                promo_create_err,
                            )
                            promo_failed_count += 1

                except stripe.error.StripeError as promo_list_err:
                    logging.error(
                        "    Error fetching source promo codes for coupon %s: %s. Skipping promo codes for this coupon.",
                        coupon_id,
                        promo_list_err,
                    )
                    # Cannot process promo codes if we can't list them

        logging.info("Coupon and Promo Code migration completed.")
        logging.info(
            "  Coupons - Migrated: %d, Skipped: %d, Failed: %d",
            coupon_migrated_count,
            coupon_skipped_count,
            coupon_failed_count,
        )
        logging.info(
            "  Promo Codes - Migrated: %d, Skipped: %d, Failed: %d",
            promo_migrated_count,
            promo_skipped_count,
            promo_failed_count,
        )

    except stripe.error.StripeError as e:
        logging.error("Error during coupon/promo code migration: %s", e)


# --- Subscription Migration Functions (from stripe_migrate_subscriptions.py) ---


# Helper function to ensure a default payment method exists for the customer in the target account
def _ensure_payment_method(
    customer_id: str, target_stripe: StripeClient
) -> Optional[str]:
    """
    Checks for and sets up a default payment method for a customer in the target account.

    1. Checks if the target customer already has a default payment method.
    2. If not, checks the source customer for a suitable payment method (card).
    3. If found in source, attempts to attach it to the target customer and set it as default.

    Args:
        customer_id: The Stripe Customer ID.
        target_stripe: Initialized Stripe client for the target account.

    Returns:
        The ID of the default payment method in the target account, or None if setup failed.
    """
    payment_method_id: Optional[str] = None
    try:
        # 1. Check target account first
        logging.debug(
            "  Ensuring PM: Checking target customer %s for default PM...", customer_id
        )
        target_customer = target_stripe.customers.retrieve(
            customer_id, params={"expand": ["invoice_settings.default_payment_method"]}
        )
        if (
            target_customer.invoice_settings
            and target_customer.invoice_settings.default_payment_method
        ):
            # Ensure we are accessing the ID correctly
            payment_method_id = (
                target_customer.invoice_settings.default_payment_method.id
            )
            logging.info(
                "  Ensuring PM: Found existing default payment method in target: %s",
                payment_method_id,
            )
            return payment_method_id
        else:
            # 2. If no default PM in target, check if any PM is attached to TARGET customer
            logging.info(
                "  Ensuring PM: No default PM in target, checking target account for attached cards..."
            )
            try:
                target_pms = target_stripe.payment_methods.list(
                    params={"customer": customer_id, "type": "card", "limit": 1}
                )
                if target_pms.data:
                    payment_method_id = target_pms.data[0].id
                    logging.info(
                        "  Ensuring PM: Found existing attached card in target: %s. Using this.",
                        payment_method_id,
                    )
                    # Optional: Update customer's default PM if needed, but often
                    # providing it in subscription creation is enough.
                    # target_stripe.customers.update(...)
                    return payment_method_id
                else:
                    logging.warning(
                        "  Ensuring PM: No default PM and no attached cards found for customer %s in target account.",
                        customer_id,
                    )
                    return None  # Cannot proceed without a PM in the target account

            except stripe.error.StripeError as list_err:
                logging.error(
                    "  Ensuring PM: Error listing payment methods for target customer %s: %s",
                    customer_id,
                    list_err,
                )
                return None  # Cannot proceed if we cannot check for PMs

    except stripe.error.StripeError as e:
        # Error retrieving the target customer initially
        logging.error(
            "  Ensuring PM: Error retrieving target customer %s: %s",
            customer_id,
            e,
        )
        return None


# Function to recreate a subscription in the target account
def recreate_subscription(
    subscription: Dict[str, Any],
    price_mapping: Dict[str, str],
    existing_target_subs_by_metadata: Dict[str, str],
    target_stripe: StripeClient,
    source_stripe: StripeClient,
    dry_run: bool = True,
) -> str:  # Return only status
    """
    Recreates a given subscription in the target Stripe account, checking metadata
    to prevent duplicates using a pre-fetched map.

    Args:
        subscription: The subscription object from the source Stripe account.
        price_mapping: A dictionary mapping source price IDs to target price IDs.
        existing_target_subs_by_metadata: Dict mapping source_sub_id to target_sub_id for existing target subs.
        target_stripe: Initialized Stripe client for the target account.
        source_stripe: Initialized Stripe client for the source account.
        dry_run: If True, simulates the process without creating the subscription.

    Returns:
        A status string (STATUS_CREATED, STATUS_SKIPPED, STATUS_FAILED, STATUS_DRY_RUN)
        indicating the result of the operation for this specific subscription.
    """
    source_subscription_id = subscription.id
    # Ensure customer_id is retrieved correctly (it can be an object or a string)
    customer_field = subscription["customer"]
    customer_id: str = (
        customer_field if isinstance(customer_field, str) else customer_field.id
    )

    logging.info(
        "Processing source subscription: %s for customer: %s",
        source_subscription_id,
        customer_id,
    )

    # --- Check for existing migrated subscription using the pre-fetched map --- (New Check)
    if source_subscription_id in existing_target_subs_by_metadata:
        target_sub_id = existing_target_subs_by_metadata[source_subscription_id]
        log_prefix = "[Dry Run] " if dry_run else ""
        logging.info(
            "  %sSkipping: Target subscription %s already exists (found via pre-fetched metadata source_subscription_id=%s).",
            log_prefix,
            target_sub_id,
            source_subscription_id,
        )
        return STATUS_SKIPPED
    else:
        # Log check only if not found, to reduce noise
        logging.debug(
            "  No existing target subscription found in pre-fetched metadata map."
        )

    # --- Proceed with mapping and creation only if not skipped ---

    if not price_mapping:
        logging.error(
            "  Error: Price mapping is empty. Skipping source subscription %s.",
            source_subscription_id,
        )
        return STATUS_FAILED

    # Map source price IDs to target price IDs for this subscription
    target_items: List[Dict[str, str]] = []
    target_price_ids_set = set()
    has_mapping_error = False
    for item in subscription["items"]["data"]:
        source_price_id = item["price"]["id"]
        try:
            target_price_id = price_mapping[source_price_id]
            target_items.append({"price": target_price_id})
            target_price_ids_set.add(target_price_id)
        except KeyError:
            logging.error(
                "  Error: Source Price ID %s not found in price_mapping. Subscription %s cannot be migrated.",
                source_price_id,
                source_subscription_id,
            )
            has_mapping_error = True
            break  # Stop processing items for this subscription

    if has_mapping_error:
        return STATUS_FAILED

    logging.debug("  Mapped target items: %s", target_items)

    # --- Dry Run Simulation --- (Now handled earlier by metadata check)
    if dry_run:
        # The actual creation logic is skipped, but we already logged the 'would check'
        # and potentially 'would create' based on the metadata check outcome.
        # We just need to return the correct status.
        # If the metadata check indicated it *would* exist, we'd return STATUS_DRY_RUN (or maybe a new STATUS_DRY_RUN_SKIPPED?)
        # For simplicity, let's stick to STATUS_DRY_RUN if it gets past the metadata check phase in dry_run mode.
        logging.info(
            "  [Dry Run] Subscription creation would proceed if not for dry run."
        )
        return STATUS_DRY_RUN

    # --- Fetch/Attach Payment Method (using helper function) ---
    payment_method_id = _ensure_payment_method(customer_id, target_stripe)

    # Ensure we have a payment method ID before proceeding
    if not payment_method_id:
        logging.error(
            "  Failed to ensure payment method for customer %s. Cannot create subscription %s.",
            customer_id,
            source_subscription_id,
        )
        return STATUS_FAILED

    # --- Create the subscription in the target account ---
    try:
        subscription_params = {
            "customer": customer_id,
            "items": target_items,
            # Use current_period_end from source as trial_end for the new sub
            # Stripe expects an integer timestamp
            "trial_end": subscription.get("current_period_end"),
            "metadata": {"source_subscription_id": source_subscription_id},
            # Crucially, ensure the default payment method is used for future invoices
            "default_payment_method": payment_method_id,
            # Prorate behavior might need adjustment based on migration strategy
            # "proration_behavior": "none", # Example: disable proration initially
            # Off session is important if customer isn't actively involved
            "off_session": True,
        }
        # Remove None values, especially for trial_end if not present
        subscription_params = {
            k: v for k, v in subscription_params.items() if v is not None
        }

        logging.debug("  Creating subscription with params: %s", subscription_params)
        target_subscription = target_stripe.subscriptions.create(
            params=subscription_params
        )
        logging.info(
            "  Successfully created target subscription %s (from source %s)",
            target_subscription.id,
            source_subscription_id,
        )

        # --- Update source subscription to cancel at period end ---
        if not dry_run:  # Ensure this only happens in a live run
            try:
                logging.info(
                    "    Attempting to set cancel_at_period_end=True for source subscription %s",
                    source_subscription_id,
                )
                source_stripe.subscriptions.update(
                    source_subscription_id,
                    params={"cancel_at_period_end": True},
                )
                logging.info(
                    "    Successfully set cancel_at_period_end=True for source subscription %s",
                    source_subscription_id,
                )
            except stripe.error.StripeError as update_err:
                logging.error(
                    "    Error updating source subscription %s to cancel at period end: %s",
                    source_subscription_id,
                    update_err,
                )
                # For now, we log the error but still return STATUS_CREATED for the target creation.
                logging.error(
                    "    FAILED TO CANCEL SOURCE SUBSCRIPTION %s AT PERIOD END. Manual intervention may be required in the source account.",
                    source_subscription_id,
                )

        return STATUS_CREATED
    except stripe.error.StripeError as e:
        logging.error(
            "  Error creating subscription for source %s (customer %s): %s",
            source_subscription_id,
            customer_id,
            e,
        )
        return STATUS_FAILED


def migrate_subscriptions(dry_run: bool = True) -> None:
    """
    Fetches all active subscriptions from the source Stripe account and attempts
    to recreate them in the target Stripe account. Dynamically builds price mapping.
    """
    logging.info("Starting subscription migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)
    target_stripe = get_stripe_client(API_KEY_TARGET)

    # --- Build price mapping dynamically --- (Crucial step)
    logging.info("Building price map from target account metadata...")
    price_mapping: Dict[str, str] = {}
    try:
        # Use active=None to potentially catch inactive prices if needed,
        # but usually mapping active source to active target is desired.
        # Stick with active=True based on original scripts.
        prices = target_stripe.prices.list(params={"limit": 100, "active": True})
        for price in prices.auto_paging_iter():
            if price.metadata and "source_price_id" in price.metadata:
                source_id = price.metadata["source_price_id"]
                price_mapping[source_id] = price.id
                logging.debug(
                    "  Mapped source price %s -> target price %s", source_id, price.id
                )
        logging.info(
            "Price map built successfully. Found %d mappings.", len(price_mapping)
        )
        if not price_mapping:
            logging.warning(
                "Warning: Price map is empty. Ensure products/prices were migrated correctly with 'source_price_id' in metadata. Subscription migration cannot proceed without it."
            )
            return  # Cannot migrate subscriptions without the price map

    except stripe.error.StripeError as e:
        logging.error("Error fetching prices from target account to build map: %s", e)
        logging.error("Cannot proceed without price mapping.")
        return  # Exit if map cannot be built
    # --- End build price mapping ---

    # --- Pre-fetch existing target subscriptions by metadata --- (Optimization)
    logging.info(
        "Pre-fetching existing target subscriptions and checking for 'source_subscription_id' metadata..."
    )
    existing_target_subs_by_metadata: Dict[str, str] = {}
    try:
        # Fetch all non-canceled, then filter locally
        target_subscriptions = target_stripe.subscriptions.list(
            params={
                # Fetch all non-canceled, then filter locally
                "status": "all",
                "limit": 100,
                # Metadata included by default
            }
        )
        for sub in target_subscriptions.auto_paging_iter():
            # Filter locally for desired statuses
            if sub.status not in ["active", "trialing"]:
                continue  # Skip if not active or trialing

            # Metadata should be directly accessible here as sub.metadata
            if (
                sub.metadata  # Check if metadata exists first
                and "source_subscription_id" in sub.metadata
            ):
                source_id = sub.metadata["source_subscription_id"]
                if source_id in existing_target_subs_by_metadata:
                    logging.warning(
                        "  Duplicate source_subscription_id %s found in target metadata. Target Sub IDs: %s, %s",
                        source_id,
                        existing_target_subs_by_metadata[source_id],
                        sub.id,
                    )
                existing_target_subs_by_metadata[source_id] = sub.id
        logging.info(
            "Found %d existing target subscriptions with 'source_subscription_id' metadata.",
            len(existing_target_subs_by_metadata),
        )
    except stripe.error.StripeError as e:
        logging.error(
            "Error pre-fetching target subscriptions for metadata check: %s. Proceeding without pre-check data.",
            e,
        )
        return  # Exit if map cannot be built

    created_count = 0
    skipped_count = 0
    failed_count = 0
    dry_run_count = 0

    try:
        logging.info("Fetching active subscriptions from source account...")
        subscriptions = source_stripe.subscriptions.list(
            params={
                "status": "active",
                "limit": 100,
                # Expand customer and items.price for consistent access
                "expand": ["data.customer", "data.items.data.price"],
            }
        )
        subs_list = list(subscriptions.auto_paging_iter())
        logging.info(
            "Found %d active subscription(s) in the source account.", len(subs_list)
        )

        # Loop through each subscription and recreate it in the target account
        for subscription in subs_list:
            status = recreate_subscription(
                subscription,
                price_mapping,
                existing_target_subs_by_metadata,  # Pass pre-fetched map
                target_stripe,
                source_stripe,
                dry_run,
            )

            if status == STATUS_CREATED:
                created_count += 1
            elif status == STATUS_SKIPPED:
                skipped_count += 1
            elif status == STATUS_FAILED:
                failed_count += 1
            elif status == STATUS_DRY_RUN:
                dry_run_count += 1
            else:
                # Should not happen with defined statuses
                logging.error(
                    "  Unknown status '%s' returned for source subscription %s. Treating as failed.",
                    status,
                    subscription.id,
                )
                failed_count += 1

        logging.info("Subscription migration process completed (Dry Run: %s).", dry_run)
        if dry_run:
            logging.info("  Processed (dry run): %d", dry_run_count)
            logging.info(
                "  (Includes potential skips/failures if checks were performed in dry run mode)"
            )
        else:
            logging.info("  Created: %d", created_count)
            logging.info("  Skipped (e.g., already existed): %d", skipped_count)
            logging.info("  Failed: %d", failed_count)

    except stripe.error.StripeError as e:
        logging.error("Error fetching subscriptions from source account: %s", e)


# --- Main Execution Logic ---


def main() -> None:
    """Main function to run the Stripe data migrations."""
    parser = argparse.ArgumentParser(
        description="Migrate Stripe Products, Prices, Coupons, Promo Codes, and/or Subscriptions."
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
    parser.add_argument(
        "--step",
        type=str,
        choices=["products", "coupons", "subscriptions", "all"],
        required=True,
        help="Specify which migration step to run: products, coupons, subscriptions, or all (default).",
    )

    args = parser.parse_args()

    # Update logging level if debug flag is set
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Debug logging enabled.")

    is_dry_run = not args.live  # dry_run is True if --live is NOT specified

    logging.info(
        "Starting Stripe data migration... (Step: %s, Dry Run: %s)",
        args.step,
        is_dry_run,
    )

    # --- Run Migrations Sequentially based on the step argument ---
    if args.step in ["products", "all"]:
        migrate_products(dry_run=is_dry_run)

    if args.step in ["coupons", "all"]:
        migrate_coupons(dry_run=is_dry_run)  # Now includes promo codes

    if args.step in ["subscriptions", "all"]:
        # Ensure products/prices/coupons are migrated first for subscriptions
        if args.step == "subscriptions":
            logging.warning(
                "Running subscription migration without ensuring products/coupons exist in target. Ensure they are already migrated or run with '--step all'."
            )
        migrate_subscriptions(dry_run=is_dry_run)  # Run subscription migration last

    logging.info(
        "Stripe data migration finished. (Step: %s, Dry Run: %s)", args.step, is_dry_run
    )


if __name__ == "__main__":
    main()
