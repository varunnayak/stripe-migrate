"""Migrates Stripe products, prices, coupons and subscriptions."""

import os
import logging
from typing import Any, Dict, Optional, Set
import argparse

import stripe
from stripe import StripeClient
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
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
    return StripeClient(api_key=api_key)


# --- Product/Price/Coupon/Promo Migration Functions (from stripe_migrate_products.py) ---


# Helper function to find/create target price
def _find_or_create_target_price(
    source_price: Dict[str, Any],
    target_product_id: str,
    target_stripe: StripeClient,
    unarchive_prices: bool = True,
    dry_run: bool = False,
) -> Optional[str]:
    """
    Checks if a target price corresponding to the source price exists,
    otherwise creates it (or simulates creation in dry run).

    Args:
        source_price: The price object from the source account
        target_product_id: The product ID in the target account
        target_stripe: Initialized Stripe client for the target account
        unarchive_prices: If True, sets inactive prices to active when migrating
        dry_run: If True, simulates the process without creating resources

    Returns:
        The target price ID if found or created, None if creation failed or skipped
    """
    source_price_id = source_price.id
    log_prefix = "[Dry Run] " if dry_run else ""

    # 1. Check if price linked by metadata exists
    try:
        params = {
            "product": target_product_id,
            "active": None if unarchive_prices else True,
            "limit": 100,
        }

        target_prices = target_stripe.prices.list(params=params)

        for p in target_prices.auto_paging_iter():
            if p.metadata and p.metadata.get("source_price_id") == source_price_id:
                target_price_id = p.id
                logging.info(
                    "      %sPrice linked via metadata %s already exists: %s. Using existing.",
                    log_prefix,
                    source_price_id,
                    target_price_id,
                )
                return target_price_id
    except stripe.error.StripeError as list_err:
        logging.warning(
            "      Warning: Could not list target prices for product %s: %s",
            target_product_id,
            list_err,
        )

    # 2. If not found by metadata, simulate or attempt creation
    if dry_run:
        logging.info(
            "      [Dry Run] Price linked via metadata %s not found.", source_price_id
        )
        try:
            target_stripe.prices.retrieve(source_price_id)
            logging.info(
                "      [Dry Run] Price %s might exist by ID, but not linked by metadata.",
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
                    "      [Dry Run] Error checking price %s by ID: %s",
                    source_price_id,
                    e_inner,
                )
        except stripe.error.StripeError as e_inner:
            logging.error(
                "      [Dry Run] Stripe error checking price %s by ID: %s",
                source_price_id,
                e_inner,
            )

        logging.info(
            "      [Dry Run] Would create price for product %s (linked to source %s)",
            target_product_id,
            source_price_id,
        )
        return None

    # Actual creation logic
    logging.info(
        "      Target price linked to source %s not found by metadata. Creating.",
        source_price_id,
    )
    try:
        # Prepare parameters with only non-None values
        price_params = {
            "currency": source_price.currency,
            "active": True if unarchive_prices else source_price.active,
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
            "      Error creating price for source %s, product %s: %s",
            source_price_id,
            target_product_id,
            e,
        )
        return None


# Function to create products and prices in the target account
def create_product_and_prices(
    product: Dict[str, Any],
    source_stripe: StripeClient,
    target_stripe: StripeClient,
    existing_target_product_ids: Set[str],
    unarchive_prices: bool = True,
    dry_run: bool = False,
) -> str:
    """
    Creates a product and its associated prices in the target Stripe account.

    Args:
        product: The product object from the source Stripe account
        source_stripe: Initialized Stripe client for the source account
        target_stripe: Initialized Stripe client for the target account
        existing_target_product_ids: Set of existing product IDs in the target account
        unarchive_prices: If True, sets inactive prices to active when migrating
        dry_run: If True, simulates the process without creating resources

    Returns:
        Status string indicating the result of the operation
    """
    product_id = product.id
    logging.info("Processing product: %s (%s)", product.name, product_id)

    target_product_id = product_id
    product_skipped = False

    # --- Product Handling ---
    if dry_run:
        logging.info(
            "  [Dry Run] Would process product: %s (%s)", product.name, product_id
        )
        if product_id in existing_target_product_ids:
            logging.info(
                "  [Dry Run] Product %s already exists in target account.",
                product_id,
            )
        else:
            logging.info(
                "  [Dry Run] Product %s would be created in target account.",
                product_id,
            )
    else:
        if product_id in existing_target_product_ids:
            logging.info(
                "  Product %s exists in target account. Skipping product creation.",
                product_id,
            )
            product_skipped = True
        else:
            logging.info(
                "  Product %s does not exist in target account. Creating.",
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
                product_params = {
                    k: v for k, v in product_params.items() if v is not None
                }

                logging.debug("  Creating product with params: %s", product_params)
                target_product = target_stripe.products.create(params=product_params)
                target_product_id = target_product.id
                logging.info("  Created target product: %s", target_product_id)
            except stripe.error.InvalidRequestError as create_err:
                if "resource_already_exists" in str(create_err):
                    logging.warning(
                        "  Product %s exists but wasn't in pre-fetched list. Using existing.",
                        product_id,
                    )
                else:
                    logging.error(
                        "  Error creating product %s: %s", product_id, create_err
                    )
                    return STATUS_FAILED
            except stripe.error.StripeError as create_err:
                logging.error("  Error creating product %s: %s", product_id, create_err)
                return STATUS_FAILED

    # --- Price Handling ---
    price_creation_failed = False
    try:
        # Fetch prices from the source account
        params = {
            "product": product_id,
            "active": None if unarchive_prices else True,
            "limit": 100,
        }
        prices = source_stripe.prices.list(params=params)
        logging.info(
            "  Found %d price(s) for source product %s",
            len(prices.data),
            product_id,
        )

        # Process each source price
        for price in prices.auto_paging_iter():
            source_price_id = price.id
            logging.info("    Processing source price: %s", source_price_id)

            target_price_id = _find_or_create_target_price(
                price, target_product_id, target_stripe, unarchive_prices, dry_run
            )

            if not target_price_id and not dry_run:
                logging.warning(
                    "      Failed to find or create target price for source %s",
                    source_price_id,
                )
                price_creation_failed = True

    except stripe.error.StripeError as e:
        logging.error(
            "  Error fetching source prices for product %s: %s", product_id, e
        )
        return STATUS_FAILED

    # Determine final status
    if dry_run:
        return STATUS_DRY_RUN
    if price_creation_failed:
        return STATUS_FAILED
    if product_skipped:
        return STATUS_SKIPPED
    return STATUS_CREATED


def migrate_products(unarchive_prices: bool = True, dry_run: bool = False) -> None:
    """
    Migrates all active products and their prices from the source Stripe
    account to the target Stripe account.

    Args:
        unarchive_prices: If True, sets inactive prices to active when migrating
        dry_run: If True, simulates the process without creating resources
    """
    logging.info("Starting product and price migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)
    target_stripe = get_stripe_client(API_KEY_TARGET)

    # Initialize counters
    processed_count = created_count = skipped_count = failed_count = dry_run_count = 0

    try:
        # Fetch existing active product IDs from the target account
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
                "Failed to list products from target account: %s. Cannot proceed.",
                e,
            )
            return

        # Fetch active products from the source account
        logging.info("Fetching active products from source account...")
        products = source_stripe.products.list(params={"active": True, "limit": 100})
        product_list = list(products.auto_paging_iter())
        logging.info(
            "Found %d active product(s) in the source account.", len(product_list)
        )

        # Process each product
        for product in product_list:
            status = create_product_and_prices(
                product,
                source_stripe,
                target_stripe,
                existing_target_product_ids,
                unarchive_prices,
                dry_run,
            )
            processed_count += 1

            # Update counters based on status
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
                    "Unknown status '%s' for product %s. Treating as failed.",
                    status,
                    product.id,
                )
                failed_count += 1

        # Log migration results
        logging.info("Product and price migration completed (dry_run=%s).", dry_run)
        if dry_run:
            logging.info("  Results (dry run) - Would Process: %d", dry_run_count)
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
        dry_run: If True, simulates the process without creating resources
    """
    logging.info("Starting coupon and promo code migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)
    target_stripe = get_stripe_client(API_KEY_TARGET)

    # Initialize counters
    coupon_migrated_count = coupon_skipped_count = coupon_failed_count = 0
    promo_migrated_count = promo_skipped_count = promo_failed_count = 0

    try:
        # Pre-fetch target coupons
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
                "Failed to list coupons from target account: %s. Cannot proceed.",
                e,
            )
            return

        # Pre-fetch target promo codes
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
                "Failed to list promo codes from target account: %s. Cannot proceed.",
                e,
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
            coupon_processed = False  # Flag to track if coupon was processed

            # Skip invalid coupons
            if not coupon.valid:
                logging.info(
                    "  Skipping invalid coupon: %s (and its promo codes)", coupon_name
                )
                coupon_skipped_count += 1
                continue

            logging.info("  Processing coupon: %s (%s)", coupon_name, coupon_id)

            # Handle dry run case
            if dry_run:
                logging.info(
                    "    [Dry Run] Would process coupon: %s (%s)",
                    coupon_name,
                    coupon_id,
                )
                if coupon_id in existing_target_coupon_ids:
                    logging.info(
                        "    [Dry Run] Coupon %s already exists in target. Would skip creation.",
                        coupon_id,
                    )
                    coupon_skipped_count += 1
                else:
                    logging.info(
                        "    [Dry Run] Coupon %s would be created in target.",
                        coupon_id,
                    )
                    coupon_migrated_count += 1  # Count as would-be migrated

                # In dry run, we'll process promo codes regardless
                coupon_processed = True

            else:  # Actual coupon creation logic
                if coupon_id in existing_target_coupon_ids:
                    logging.info(
                        "    Coupon %s exists in target. Skipping creation.",
                        coupon_id,
                    )
                    coupon_skipped_count += 1
                    coupon_processed = True  # Coupon exists, can process promo codes
                else:
                    # Create the coupon
                    logging.info(
                        "    Coupon %s does not exist in target. Creating.",
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
                        # Remove None values
                        coupon_params = {
                            k: v for k, v in coupon_params.items() if v is not None
                        }

                        logging.debug(
                            "    Creating coupon with params: %s",
                            coupon_params,
                        )
                        target_coupon = target_stripe.coupons.create(
                            params=coupon_params
                        )
                        logging.info("    Created coupon: %s", target_coupon.id)
                        coupon_migrated_count += 1
                        coupon_processed = True
                    except stripe.error.InvalidRequestError as create_err:
                        if "resource_already_exists" in str(create_err):
                            logging.warning(
                                "    Coupon %s exists but wasn't in pre-fetched list. Using existing.",
                                coupon_id,
                            )
                            coupon_skipped_count += 1
                            coupon_processed = True
                        else:
                            logging.error(
                                "    Error creating coupon %s: %s",
                                coupon.id,
                                create_err,
                            )
                            coupon_failed_count += 1
                    except stripe.error.StripeError as create_err:
                        logging.error(
                            "    Error creating coupon %s: %s", coupon.id, create_err
                        )
                        coupon_failed_count += 1

            # --- Process Promotion Codes ---
            if coupon_processed:  # Only if coupon exists or would exist in dry run
                try:
                    logging.debug(
                        "    Fetching active promo codes for coupon %s...",
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
                        code_exists = promo_code_code in existing_target_promo_codes

                        logging.info(
                            "      Processing promo code: %s (ID: %s)",
                            promo_code_code,
                            promo_code_id,
                        )

                        if dry_run:
                            if code_exists:
                                logging.info(
                                    "        [Dry Run] Promo code %s already exists. Would skip.",
                                    promo_code_code,
                                )
                                promo_skipped_count += 1
                            else:
                                logging.info(
                                    "        [Dry Run] Would create promo code: %s for coupon %s",
                                    promo_code_code,
                                    coupon_id,
                                )
                                promo_migrated_count += 1
                            continue

                        # Actual promo code creation
                        if code_exists:
                            logging.info(
                                "        Promo code %s already exists. Skipping.",
                                promo_code_code,
                            )
                            promo_skipped_count += 1
                            continue

                        try:
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
                                "active": promo_code.active,
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
                                "        Creating promo code with params: %s",
                                promo_params,
                            )
                            target_promo_code = target_stripe.promotion_codes.create(
                                params=promo_params
                            )
                            logging.info(
                                "        Created promo code: %s (ID: %s)",
                                target_promo_code.code,
                                target_promo_code.id,
                            )
                            promo_migrated_count += 1
                            # Add to set to prevent duplicates
                            existing_target_promo_codes.add(target_promo_code.code)
                        except stripe.error.InvalidRequestError as promo_err:
                            if "already exists" in str(promo_err).lower():
                                logging.warning(
                                    "        Promo code %s already exists. Skipping.",
                                    promo_code_code,
                                )
                                promo_skipped_count += 1
                                existing_target_promo_codes.add(promo_code_code)
                            else:
                                logging.error(
                                    "        Error creating promo code %s: %s",
                                    promo_code_code,
                                    promo_err,
                                )
                                promo_failed_count += 1
                        except stripe.error.StripeError as promo_err:
                            logging.error(
                                "        Error creating promo code %s: %s",
                                promo_code_code,
                                promo_err,
                            )
                            promo_failed_count += 1

                except stripe.error.StripeError as promo_list_err:
                    logging.error(
                        "    Error fetching promo codes for coupon %s: %s",
                        coupon_id,
                        promo_list_err,
                    )

        # Log migration results
        logging.info("Coupon and Promo Code migration completed.")
        logging.info(
            "  Coupons - Created: %d, Skipped: %d, Failed: %d",
            coupon_migrated_count,
            coupon_skipped_count,
            coupon_failed_count,
        )
        logging.info(
            "  Promo Codes - Created: %d, Skipped: %d, Failed: %d",
            promo_migrated_count,
            promo_skipped_count,
            promo_failed_count,
        )

    except stripe.error.StripeError as e:
        logging.error("Error during coupon/promo code migration: %s", e)


# --- Subscription Migration Functions (from stripe_migrate_subscriptions.py) ---


def _ensure_payment_method(
    customer_id: str, target_stripe: StripeClient
) -> Optional[str]:
    """
    Checks for and sets up a default payment method for a customer.

    Args:
        customer_id: The Stripe Customer ID
        target_stripe: Initialized Stripe client for the target account

    Returns:
        The ID of the default payment method, or None if setup failed
    """
    try:
        # Check if customer already has a default payment method
        logging.debug(
            "  Checking customer %s for default payment method...", customer_id
        )
        target_customer = target_stripe.customers.retrieve(
            customer_id, params={"expand": ["invoice_settings.default_payment_method"]}
        )

        # If customer has a default payment method, return it
        if (
            target_customer.invoice_settings
            and target_customer.invoice_settings.default_payment_method
        ):
            payment_method_id = (
                target_customer.invoice_settings.default_payment_method.id
            )
            logging.info(
                "  Found existing default payment method: %s", payment_method_id
            )
            return payment_method_id

        # Check if any payment method is attached to customer
        logging.info("  No default payment method, checking for attached cards...")
        target_pms = target_stripe.payment_methods.list(
            params={"customer": customer_id, "type": "card", "limit": 1}
        )

        if target_pms.data:
            payment_method_id = target_pms.data[0].id
            logging.info("  Found attached card: %s", payment_method_id)
            return payment_method_id

        logging.warning("  No payment methods found for customer %s.", customer_id)
        return None

    except stripe.error.StripeError as e:
        logging.error(
            "  Error accessing payment information for customer %s: %s",
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
) -> str:
    """
    Recreates a given subscription in the target Stripe account.

    Args:
        subscription: The subscription object from the source account
        price_mapping: Dict mapping source price IDs to target price IDs
        existing_target_subs_by_metadata: Dict mapping source_sub_id to target_sub_id
        target_stripe: Initialized Stripe client for the target account
        source_stripe: Initialized Stripe client for the source account
        dry_run: If True, simulates the process without creating resources

    Returns:
        Status string indicating the result of the operation
    """
    source_subscription_id = subscription.id
    # Get customer ID (can be an object or a string)
    customer_field = subscription["customer"]
    customer_id = (
        customer_field if isinstance(customer_field, str) else customer_field.id
    )

    logging.info(
        "Processing subscription: %s for customer: %s",
        source_subscription_id,
        customer_id,
    )

    # Check if subscription already exists in target
    if source_subscription_id in existing_target_subs_by_metadata:
        target_sub_id = existing_target_subs_by_metadata[source_subscription_id]
        log_prefix = "[Dry Run] " if dry_run else ""
        logging.info(
            "  %sSubscription already exists in target: %s. Skipping.",
            log_prefix,
            target_sub_id,
        )
        return STATUS_SKIPPED

    # Validate price mapping
    if not price_mapping:
        logging.error("  Error: Price mapping is empty. Cannot migrate subscription.")
        return STATUS_FAILED

    # Map source price IDs to target price IDs
    target_items = []
    has_mapping_error = False

    for item in subscription["items"]["data"]:
        source_price_id = item["price"]["id"]
        if source_price_id not in price_mapping:
            logging.error(
                "  Error: Source Price ID %s not found in price mapping. Cannot migrate.",
                source_price_id,
            )
            has_mapping_error = True
            break

        target_price_id = price_mapping[source_price_id]
        target_items.append({"price": target_price_id, "quantity": item["quantity"]})

    if has_mapping_error:
        return STATUS_FAILED

    logging.debug("  Mapped target items: %s", target_items)

    # Dry run simulation
    if dry_run:
        logging.info("  [Dry Run] Would create subscription in target account.")
        return STATUS_DRY_RUN

    # Fetch/Attach Payment Method
    payment_method_id = _ensure_payment_method(customer_id, target_stripe)
    if not payment_method_id:
        logging.error(
            "  Failed to ensure payment method for customer %s. Cannot create subscription.",
            customer_id,
        )
        return STATUS_FAILED

    # Create the subscription in the target account
    try:
        # Get subscription parameters
        source_cancels_at_period_end = subscription.get("cancel_at_period_end", False)
        source_collection_method = subscription.get("collection_method")
        source_metadata = (
            subscription.metadata.to_dict_recursive() if subscription.metadata else {}
        )

        # Prepare subscription parameters
        subscription_params = {
            "customer": customer_id,
            "items": target_items,
            "trial_end": subscription.get("current_period_end"),
            "metadata": {
                **source_metadata,
                "source_subscription_id": source_subscription_id,
            },
            "default_payment_method": payment_method_id,
            "off_session": True,
            "cancel_at_period_end": source_cancels_at_period_end,
            "collection_method": source_collection_method,
        }

        # Add days_until_due for invoice collection method
        if source_collection_method == "send_invoice":
            days_until_due = subscription.get("days_until_due", 30)
            subscription_params["days_until_due"] = days_until_due
            logging.info("    Setting days_until_due=%d", days_until_due)

        # Handle discount
        source_discount = subscription.get("discount")
        if source_discount:
            if source_discount.coupon:
                source_coupon_id = source_discount.coupon.id
                subscription_params["coupon"] = source_coupon_id
                logging.info("    Applying coupon %s", source_coupon_id)
            elif source_discount.promotion_code:
                source_promo_code_obj = source_discount.promotion_code
                if isinstance(source_promo_code_obj, stripe.PromotionCode):
                    subscription_params["promotion_code"] = source_promo_code_obj.code
                    logging.info(
                        "    Applying promotion code %s", source_promo_code_obj.code
                    )
                else:
                    logging.warning(
                        "    Could not determine promotion code: %s",
                        source_discount.promotion_code,
                    )

        # Remove None values
        subscription_params = {
            k: v for k, v in subscription_params.items() if v is not None
        }

        # Create subscription
        logging.debug("  Creating subscription with params: %s", subscription_params)
        target_subscription = target_stripe.subscriptions.create(
            params=subscription_params
        )
        logging.info(
            "  Created target subscription: %s (from source: %s)",
            target_subscription.id,
            source_subscription_id,
        )

        # Update source subscription to cancel at period end
        if not source_cancels_at_period_end:
            try:
                source_stripe.subscriptions.update(
                    source_subscription_id,
                    params={"cancel_at_period_end": True},
                )
                logging.info(
                    "    Set cancel_at_period_end=True for source subscription %s",
                    source_subscription_id,
                )
            except stripe.error.StripeError as update_err:
                logging.error(
                    "    Error updating source subscription to cancel at period end: %s",
                    update_err,
                )
                logging.error(
                    "    FAILED TO CANCEL SOURCE SUBSCRIPTION. Manual intervention required.",
                )

        return STATUS_CREATED
    except stripe.error.StripeError as e:
        logging.error(
            "  Error creating subscription: %s",
            e,
        )
        return STATUS_FAILED


def migrate_subscriptions(dry_run: bool = True) -> None:
    """
    Migrates all active subscriptions from the source Stripe account to the target account.

    Args:
        dry_run: If True, simulates the process without creating resources
    """
    logging.info("Starting subscription migration (dry_run=%s)...", dry_run)
    source_stripe = get_stripe_client(API_KEY_SOURCE)
    target_stripe = get_stripe_client(API_KEY_TARGET)

    # Build price mapping from target account
    logging.info("Building price map from target account metadata...")
    price_mapping = {}
    try:
        prices = target_stripe.prices.list(params={"limit": 100, "active": True})
        for price in prices.auto_paging_iter():
            if price.metadata and "source_price_id" in price.metadata:
                source_id = price.metadata["source_price_id"]
                price_mapping[source_id] = price.id
                logging.debug(
                    "  Mapped source price %s -> target price %s", source_id, price.id
                )

        logging.info("Price map built with %d mappings.", len(price_mapping))
        if not price_mapping:
            logging.warning(
                "Price map is empty. Ensure products/prices were migrated with 'source_price_id' metadata."
            )
            return
    except stripe.error.StripeError as e:
        logging.error("Error fetching prices from target account: %s", e)
        return

    # Pre-fetch existing target subscriptions by metadata
    logging.info("Pre-fetching existing target subscriptions...")
    existing_target_subs_by_metadata = {}
    try:
        # Fetch all non-canceled subscriptions
        target_subscriptions = target_stripe.subscriptions.list(
            params={"status": "all", "limit": 100}
        )
        for sub in target_subscriptions.auto_paging_iter():
            # Skip if not active or trialing
            if sub.status not in ["active", "trialing"]:
                continue

            # Check for source_subscription_id in metadata
            if sub.metadata and "source_subscription_id" in sub.metadata:
                source_id = sub.metadata["source_subscription_id"]
                if source_id in existing_target_subs_by_metadata:
                    logging.warning(
                        "  Duplicate source_subscription_id %s found. Target IDs: %s, %s",
                        source_id,
                        existing_target_subs_by_metadata[source_id],
                        sub.id,
                    )
                existing_target_subs_by_metadata[source_id] = sub.id

        logging.info(
            "Found %d existing target subscriptions with source metadata.",
            len(existing_target_subs_by_metadata),
        )
    except stripe.error.StripeError as e:
        logging.error("Error pre-fetching target subscriptions: %s", e)
        return

    # Initialize counters
    created_count = skipped_count = failed_count = dry_run_count = 0

    try:
        # Fetch active subscriptions from source account
        logging.info("Fetching active subscriptions from source account...")
        params = {
            "status": "active",
            "limit": 100,
            "expand": ["data.customer", "data.items.data.price", "data.discount"],
        }
        subscriptions = source_stripe.subscriptions.list(params=params)
        subs_list = list(subscriptions.auto_paging_iter())
        logging.info(
            "Found %d active subscription(s) in source account.", len(subs_list)
        )

        # Process each subscription
        for subscription in subs_list:
            status = recreate_subscription(
                subscription,
                price_mapping,
                existing_target_subs_by_metadata,
                target_stripe,
                source_stripe,
                dry_run,
            )

            # Update counters based on status
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
                    "Unknown status '%s' for subscription %s. Treating as failed.",
                    status,
                    subscription.id,
                )
                failed_count += 1

        # Log migration results
        logging.info("Subscription migration completed (dry_run=%s).", dry_run)
        if dry_run:
            logging.info("  Processed (dry run): %d", dry_run_count)
        else:
            logging.info(
                "  Created: %d, Skipped: %d, Failed: %d",
                created_count,
                skipped_count,
                failed_count,
            )

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
        help="Specify which migration step to run: products, coupons, subscriptions, or all.",
    )
    parser.add_argument(
        "--unarchive-prices",
        action="store_true",
        help="Unarchive inactive prices when migrating. Default is True.",
    )
    parser.add_argument(
        "--keep-price-status",
        action="store_true",
        help="Keep the active/inactive status of prices when migrating. Overrides --unarchive-prices.",
    )

    args = parser.parse_args()

    # Configure logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Debug logging enabled.")

    # Process arguments
    is_dry_run = not args.live
    # keep_price_status takes precedence over unarchive_prices
    unarchive_prices = not args.keep_price_status

    logging.info(
        "Starting Stripe migration... (Step: %s, Dry Run: %s, Unarchive Prices: %s)",
        args.step,
        is_dry_run,
        unarchive_prices,
    )

    # Run migrations based on the selected step
    if args.step in ["products", "all"]:
        migrate_products(unarchive_prices=unarchive_prices, dry_run=is_dry_run)

    if args.step in ["coupons", "all"]:
        migrate_coupons(dry_run=is_dry_run)

    if args.step in ["subscriptions", "all"]:
        if args.step == "subscriptions":
            logging.warning(
                "Running subscription migration directly. Ensure products/coupons exist in the target account."
            )
        migrate_subscriptions(dry_run=is_dry_run)

    logging.info(
        "Stripe migration finished. (Step: %s, Dry Run: %s)", args.step, is_dry_run
    )


if __name__ == "__main__":
    main()
