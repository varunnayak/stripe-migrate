import os
import logging
from typing import Any, Dict, List, Optional, Tuple
import argparse

import stripe
from dotenv import load_dotenv

# Configure logging (same as products script for consistency)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

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


# Define return status constants for clarity
STATUS_CREATED = "created"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"
STATUS_DRY_RUN = "dry_run"


# Helper function to ensure a default payment method exists for the customer in the target account
def _ensure_payment_method(
    customer_id: str, target_stripe: Any, source_stripe: Any
) -> Optional[str]:
    """
    Checks for and sets up a default payment method for a customer in the target account.

    1. Checks if the target customer already has a default payment method.
    2. If not, checks the source customer for a suitable payment method (card).
    3. If found in source, attempts to attach it to the target customer and set it as default.

    Args:
        customer_id: The Stripe Customer ID.
        target_stripe: Initialized Stripe client for the target account.
        source_stripe: Initialized Stripe client for the source account.

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
            customer_id, expand=["invoice_settings.default_payment_method"]
        )
        if (
            target_customer.invoice_settings
            and target_customer.invoice_settings.default_payment_method
        ):
            payment_method_id = (
                target_customer.invoice_settings.default_payment_method.id
            )
            logging.info(
                "  Ensuring PM: Found existing default payment method in target: %s",
                payment_method_id,
            )
            return payment_method_id
        else:
            # 2. If no default PM in target, check source
            logging.info(
                "  Ensuring PM: No default PM in target, checking source account..."
            )
            source_pms = source_stripe.payment_methods.list(
                params={"customer": customer_id, "type": "card", "limit": 1}
            )
            if source_pms.data:
                source_pm_id = source_pms.data[0].id
                logging.info(
                    "  Ensuring PM: Found source PM: %s. Attaching to target...",
                    source_pm_id,
                )
                try:
                    # 3. Attach source PM to target customer
                    attached_pm = target_stripe.payment_methods.attach(
                        source_pm_id, params={"customer": customer_id}
                    )
                    payment_method_id = attached_pm.id
                    # Set as default for customer
                    logging.debug(
                        "  Ensuring PM: Setting %s as default PM for target customer %s...",
                        payment_method_id,
                        customer_id,
                    )
                    target_stripe.customers.update(
                        customer_id,
                        params={
                            "invoice_settings": {
                                "default_payment_method": payment_method_id
                            }
                        },
                    )
                    logging.info(
                        "  Ensuring PM: Successfully attached and set source PM as default in target: %s",
                        payment_method_id,
                    )
                    return payment_method_id
                except stripe.error.StripeError as attach_err:
                    logging.error(
                        "  Ensuring PM: Error attaching source PM %s to target customer %s: %s",
                        source_pm_id,
                        customer_id,
                        attach_err,
                    )
                    # Log warning here, but failure is handled by returning None
                    logging.warning(
                        "  Subscription creation might fail due to payment method attachment issue."
                    )
                    return None
            else:
                logging.warning(
                    "  Ensuring PM: No suitable card payment methods found for customer %s in source account.",
                    customer_id,
                )
                return None  # Cannot proceed without a PM
    except stripe.error.StripeError as e:
        logging.error(
            "  Ensuring PM: Error checking/attaching payment methods for customer %s: %s",
            customer_id,
            e,
        )
        return None


# Function to recreate a subscription in the target account
def recreate_subscription(
    subscription: Dict[str, Any],
    price_mapping: Dict[str, str],
    existing_target_subs_by_customer: Dict[str, set[frozenset[str]]],
    target_stripe: Any,
    source_stripe: Any,
    dry_run: bool = True,
) -> str:  # Return only status
    """
    Recreates a given subscription in the target Stripe account, using pre-fetched data
    to check for existing subscriptions.

    Args:
        subscription: The subscription object from the source Stripe account.
        price_mapping: A dictionary mapping source price IDs to target price IDs.
        existing_target_subs_by_customer: A dictionary mapping customer IDs to sets of frozensets of price IDs
                                           for their active subscriptions in the target account.
        target_stripe: Initialized Stripe client for the target account.
        source_stripe: Initialized Stripe client for the source account.
        dry_run: If True, simulates the process without creating the subscription.

    Returns:
        A status string indicating the result of the operation.
    """
    source_subscription_id = subscription.id
    customer_id: str = subscription["customer"]
    logging.info(
        "Processing source subscription: %s for customer: %s",
        source_subscription_id,
        customer_id,
    )

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

    # --- Check for existing active subscription in target account using pre-fetched data --- (Optimization)
    target_price_ids_frozenset = frozenset(target_price_ids_set)
    if customer_id in existing_target_subs_by_customer:
        customer_existing_subs_price_sets = existing_target_subs_by_customer[
            customer_id
        ]
        if target_price_ids_frozenset in customer_existing_subs_price_sets:
            logging.info(
                "  Skipping: Customer %s already has an active subscription with the same price IDs (based on pre-fetched list).",
                customer_id,
            )
            # Cannot easily get the existing sub ID from this structure
            return STATUS_SKIPPED

    # --- Dry Run Simulation ---
    if dry_run:
        logging.info(
            "  [Dry Run] Would create subscription for customer %s", customer_id
        )
        logging.info("    Items: %s", target_items)
        # Use current_period_end as the trial_end for the new subscription
        trial_end_ts = subscription.get("current_period_end")
        logging.info("    Trial End (from source current_period_end): %s", trial_end_ts)
        logging.info("    Default Payment Method: To be fetched and attached if needed")
        logging.info(
            "    Metadata: {'source_subscription_id': '%s'}", source_subscription_id
        )
        logging.info("  [Dry Run] Subscription creation skipped.")
        return STATUS_DRY_RUN

    # --- Fetch/Attach Payment Method (using helper function) ---
    payment_method_id = _ensure_payment_method(
        customer_id, target_stripe, source_stripe
    )

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
    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    # --- Build price mapping dynamically --- (Crucial step)
    logging.info("Building price map from target account metadata...")
    price_mapping: Dict[str, str] = {}
    try:
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

    # --- Pre-fetch existing active target subscriptions --- (Optimization)
    logging.info("Pre-fetching existing active subscriptions from target account...")
    existing_target_subs_by_customer: Dict[str, set[frozenset[str]]] = {}
    try:
        target_subscriptions = target_stripe.subscriptions.list(
            params={"status": "active", "limit": 100}
        )
        for sub in target_subscriptions.auto_paging_iter():
            customer_id = sub.customer
            price_ids = frozenset(item.price.id for item in sub.items.data)
            if customer_id not in existing_target_subs_by_customer:
                existing_target_subs_by_customer[customer_id] = set()
            existing_target_subs_by_customer[customer_id].add(price_ids)
        logging.info(
            "Found active subscriptions for %d customers in target account.",
            len(existing_target_subs_by_customer),
        )
    except stripe.error.StripeError as e:
        logging.error(
            "Error pre-fetching target subscriptions: %s. Proceeding without pre-check data.",
            e,
        )
        # Let the script continue, checks inside recreate_subscription will fallback or fail
    # --- End pre-fetch target subscriptions ---

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
            }  # Consider adding 'expand': ['items.data.price'] if needed often
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
                existing_target_subs_by_customer,  # Pass pre-fetched data
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


def main() -> None:
    """Main function to run the subscription migration."""
    parser = argparse.ArgumentParser(description="Migrate Stripe Subscriptions.")
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

    # Set dry_run=False to perform actual migration
    migrate_subscriptions(dry_run=is_dry_run)


if __name__ == "__main__":
    main()
