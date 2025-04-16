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


# Function to recreate a subscription in the target account
def recreate_subscription(
    subscription: Dict[str, Any],
    price_mapping: Dict[str, str],
    target_stripe: Any,
    source_stripe: Any,
    dry_run: bool = True,
) -> Tuple[str, Optional[str]]:  # Return status and subscription ID (or None)
    """
    Recreates a given subscription in the target Stripe account.

    Args:
        subscription: The subscription object from the source Stripe account.
        price_mapping: A dictionary mapping source price IDs to target price IDs.
        target_stripe: Initialized Stripe client for the target account.
        source_stripe: Initialized Stripe client for the source account.
        dry_run: If True, simulates the process without creating the subscription.

    Returns:
        A tuple containing the status (created, skipped, failed, dry_run)
        and the target subscription ID if created, otherwise None.
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
        return STATUS_FAILED, None

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
        return STATUS_FAILED, None

    logging.debug("  Mapped target items: %s", target_items)

    # --- Check for existing active subscription in target account ---
    try:
        logging.debug(
            "  Checking for existing active subscriptions for customer %s...",
            customer_id,
        )
        existing_target_subscriptions = target_stripe.subscriptions.list(
            params={"customer": customer_id, "status": "active", "limit": 100}
        )
        for existing_sub in existing_target_subscriptions.auto_paging_iter():
            existing_sub_price_ids = {item.price.id for item in existing_sub.items.data}
            if existing_sub_price_ids == target_price_ids_set:
                logging.info(
                    "  Skipping: Found existing active subscription %s for customer %s with the same price IDs.",
                    existing_sub.id,
                    customer_id,
                )
                return (
                    STATUS_SKIPPED,
                    existing_sub.id,
                )  # Return skipped status and existing ID
    except stripe.error.StripeError as list_err:
        logging.warning(
            "  Warning: Could not list target subscriptions for customer %s to check for duplicates: %s",
            customer_id,
            list_err,
        )
        # Decide whether to proceed or stop if check fails.
        # For now, let's proceed but log the warning. A stricter approach might return STATUS_FAILED.

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
        return STATUS_DRY_RUN, None

    # --- Fetch/Attach Payment Method (only needed for live run) ---
    payment_method_id: Optional[str] = None
    try:
        # Check target account first for existing default PM
        logging.debug(
            "  Fetching target customer %s to check default PM...", customer_id
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
                "  Found default payment method in target account: %s",
                payment_method_id,
            )
        else:
            # If no default PM in target, check source PMs and attach if found
            logging.info(
                "  No default payment method found in target, checking source..."
            )
            source_pms = source_stripe.payment_methods.list(
                params={
                    "customer": customer_id,
                    "type": "card",
                    "limit": 1,
                }  # Assuming card PM is desired
            )
            if source_pms.data:
                source_pm_id = source_pms.data[0].id
                logging.info(
                    "  Found payment method in source: %s. Attaching to target customer...",
                    source_pm_id,
                )
                try:
                    # Attach the source PM to the target customer
                    # Note: This might fail depending on Stripe setup (e.g., cross-account PM usage requires specific permissions)
                    # Using PaymentIntents/SetupIntents is generally more robust for capturing payment details directly in the target account.
                    attached_pm = target_stripe.payment_methods.attach(
                        source_pm_id, params={"customer": customer_id}
                    )
                    payment_method_id = attached_pm.id
                    # Set as default for customer (important for subscriptions)
                    logging.debug(
                        "  Setting %s as default PM for customer %s...",
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
                        "  Successfully attached and set as default PM in target: %s",
                        payment_method_id,
                    )
                except stripe.error.StripeError as attach_err:
                    logging.error(
                        "  Error attaching source PM %s to target customer %s: %s",
                        source_pm_id,
                        customer_id,
                        attach_err,
                    )
                    logging.warning(
                        "  Skipping subscription %s due to payment method attachment issue.",
                        source_subscription_id,
                    )
                    return STATUS_FAILED, None
            else:
                logging.warning(
                    "  No suitable card payment methods found for customer %s in source account. Subscription requires a payment method. Skipping %s.",
                    customer_id,
                    source_subscription_id,
                )
                return STATUS_FAILED, None  # Cannot create subscription without PM
    except stripe.error.StripeError as e:
        logging.error(
            "  Error fetching/attaching payment methods for customer %s: %s",
            customer_id,
            e,
        )
        return STATUS_FAILED, None

    # Ensure we have a payment method ID before proceeding
    if not payment_method_id:
        logging.error(
            "  Cannot create subscription %s: No payment method ID was determined.",
            source_subscription_id,
        )
        return STATUS_FAILED, None

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
        return STATUS_CREATED, target_subscription.id
    except stripe.error.StripeError as e:
        logging.error(
            "  Error creating subscription for source %s (customer %s): %s",
            source_subscription_id,
            customer_id,
            e,
        )
        return STATUS_FAILED, None


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
            status, target_sub_id = recreate_subscription(
                subscription,
                price_mapping,
                target_stripe,
                source_stripe,
                dry_run,
            )

            if status == STATUS_CREATED:
                created_count += 1
                # TODO: Implement source subscription cancellation logic HERE if desired
                # Be cautious about doing this automatically. Might be better as a separate step/script.
                # source_sub_id = subscription.id
                # logging.info(f"  -> Successfully created target sub {target_sub_id}. Now cancelling source sub {source_sub_id}...")
                # try:
                #     source_stripe.subscriptions.cancel(source_sub_id)
                #     logging.info(f"      Cancelled source subscription {source_sub_id}.")
                # except stripe.error.StripeError as cancel_err:
                #     logging.error(f"      Error cancelling source subscription {source_sub_id}: {cancel_err}")
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
