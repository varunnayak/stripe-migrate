import os
from typing import Any, Dict, List, Optional
import argparse

import stripe
from dotenv import load_dotenv

load_dotenv()

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


# Function to recreate a subscription in the target account
def recreate_subscription(
    subscription: Dict[str, Any],
    price_mapping: Dict[str, str],  # Added price_mapping argument
    target_stripe: Any,  # Pass target client
    source_stripe: Any,  # Pass source client
    dry_run: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Recreates a given subscription in the target Stripe account.

    Args:
        subscription: The subscription object from the source Stripe account.
        price_mapping: A dictionary mapping source price IDs to target price IDs.
        target_stripe: Initialized Stripe client for the target account.
        source_stripe: Initialized Stripe client for the source account.
        dry_run: If True, simulates the process without creating the subscription.

    Returns:
        The newly created subscription object from the target Stripe account, or None if
        skipped or in dry_run mode.
    """
    source_subscription_id = subscription.id  # Store source ID
    if not price_mapping:
        print(
            f"Error: Price mapping is empty. Skipping source subscription {source_subscription_id}."
        )
        return None

    customer_id: str = subscription["customer"]
    print(
        f"Processing source subscription: {source_subscription_id} for customer: {customer_id}"
    )

    # Map source price IDs to target price IDs for this subscription
    target_items: List[Dict[str, str]] = []
    target_price_ids_set = set()
    try:
        for item in subscription["items"]["data"]:
            source_price_id = item["price"]["id"]
            target_price_id = price_mapping[source_price_id]
            target_items.append({"price": target_price_id})
            target_price_ids_set.add(target_price_id)
        print(f"  Mapped target items: {target_items}")
    except KeyError as e:
        print(
            f"Error: Source Price ID {e} not found in price_mapping. Skipping subscription {source_subscription_id}."
        )
        return None

    # --- Check for existing active subscription in target account ---
    try:
        existing_target_subscriptions = target_stripe.subscriptions.list(
            params={"customer": customer_id, "status": "active", "limit": 100}
        )
        for existing_sub in existing_target_subscriptions.auto_paging_iter():
            existing_sub_price_ids = {item.price.id for item in existing_sub.items.data}
            if existing_sub_price_ids == target_price_ids_set:
                print(
                    f"  Skipping: Found existing active subscription {existing_sub.id} for customer {customer_id} with the same price IDs."
                )
                return None  # Skip creation
    except stripe.error.StripeError as list_err:
        print(
            f"Warning: Could not list target subscriptions for customer {customer_id} to check for duplicates: {list_err}"
        )
        # Decide whether to proceed or stop if check fails.
        # For now, let's proceed but log the warning. A stricter approach might return None.

    if dry_run:
        print(f"  [Dry Run] Would create subscription for customer {customer_id}")
        print(f"    Items: {target_items}")
        print(
            f"    Trial End (from source current_period_end): {subscription['current_period_end']}"
        )
        print(
            f"    Default Payment Method: To be fetched"
        )  # Indicate PM needs fetching
        print(f"    Metadata: {{'source_subscription_id': '{source_subscription_id}'}}")
        print("  [Dry Run] Subscription creation skipped.")
        return None

    # --- Fetch Payment Method (moved after dry run check) ---
    payment_method_id: Optional[str] = None
    try:
        # Check target account first for existing PM
        target_pms = target_stripe.payment_methods.list(
            params={"customer": customer_id, "type": "card", "limit": 1}
        )
        if target_pms.data:
            payment_method_id = target_pms.data[0].id
            print(
                f"  Found existing payment method in target account: {payment_method_id}"
            )
        else:
            # If not found in target, fetch from source and try to attach to target customer
            print(f"  No payment method found in target, checking source...")
            source_pms = source_stripe.payment_methods.list(
                params={"customer": customer_id, "type": "card", "limit": 1}
            )
            if source_pms.data:
                source_pm_id = source_pms.data[0].id
                print(
                    f"  Found payment method in source: {source_pm_id}. Attaching to target customer..."
                )
                try:
                    # Attach the source PM to the target customer
                    # Note: This might fail depending on Stripe setup (e.g., cross-account PM usage)
                    # A more robust solution involves PaymentIntents or SetupIntents flow.
                    attached_pm = target_stripe.payment_methods.attach(
                        source_pm_id, params={"customer": customer_id}
                    )
                    payment_method_id = attached_pm.id
                    # Set as default for customer (important for subscriptions)
                    target_stripe.customers.update(
                        customer_id,
                        params={
                            "invoice_settings": {
                                "default_payment_method": payment_method_id
                            }
                        },
                    )
                    print(
                        f"  Successfully attached and set as default PM in target: {payment_method_id}"
                    )
                except stripe.error.StripeError as attach_err:
                    print(
                        f"  Error attaching source PM {source_pm_id} to target customer {customer_id}: {attach_err}"
                    )
                    print(
                        f"  Skipping subscription {source_subscription_id} due to payment method issue."
                    )
                    return None
            else:
                print(
                    f"  No suitable card payment methods found for customer {customer_id} in source or target. Skipping subscription {source_subscription_id}."
                )
                return None
    except stripe.error.StripeError as e:
        print(
            f"  Error fetching/attaching payment methods for customer {customer_id}: {e}"
        )
        return None
    # --- End Fetch Payment Method ---

    # --- Create the subscription in the target account ---
    try:
        target_subscription = target_stripe.subscriptions.create(
            params={
                "customer": customer_id,
                "items": target_items,
                "trial_end": subscription["current_period_end"],
                "metadata": {"source_subscription_id": source_subscription_id},
            }
        )
        print(
            f"  Successfully created target subscription {target_subscription.id} (from source {source_subscription_id})"
        )
        return target_subscription
    except stripe.error.StripeError as e:
        print(
            f"  Error creating subscription {source_subscription_id} for customer {customer_id}: {e}"
        )
        return None


def migrate_subscriptions(dry_run: bool = True) -> None:
    """
    Fetches all active subscriptions from the source Stripe account and attempts
    to recreate them in the target Stripe account. Dynamically builds price mapping.
    """
    print(f"Starting subscription migration (dry_run={dry_run})...")
    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore # Initialize target client here

    # --- Build price mapping dynamically ---
    print("Building price map from target account metadata...")
    price_mapping: Dict[str, str] = {}
    try:
        prices = target_stripe.prices.list(params={"limit": 100, "active": True})
        for price in prices.auto_paging_iter():
            if price.metadata and "source_price_id" in price.metadata:
                source_id = price.metadata["source_price_id"]
                price_mapping[source_id] = price.id
                # Optional: print mapping for verification
                # print(f"  Mapped source price {source_id} -> target price {price.id}")
        print(f"Price map built successfully. Found {len(price_mapping)} mappings.")
        if not price_mapping:
            print(
                "Warning: Price map is empty. Ensure products/prices were migrated correctly with 'source_price_id' in metadata."
            )

    except stripe.error.StripeError as e:
        print(f"Error fetching prices from target account to build map: {e}")
        print("Cannot proceed without price mapping.")
        return  # Exit if map cannot be built
    # --- End build price mapping ---

    try:
        subscriptions = source_stripe.subscriptions.list(
            params={"status": "active", "limit": 100}
        )
        print(
            f"Found {len(subscriptions.data)} active subscription(s) in the source account."
        )
        created_count = 0
        skipped_count = 0
        failed_count = 0
        # Loop through each subscription and recreate it in the target account
        for subscription in subscriptions.auto_paging_iter():
            target_subscription = recreate_subscription(
                subscription,
                price_mapping,
                target_stripe,
                source_stripe,
                dry_run,  # Pass clients
            )
            if dry_run:
                # In dry run, recreate_subscription returns None, handle counting based on logs
                # This is slightly imprecise, relies on log messages from recreate_subscription
                # A better way might be for recreate_subscription to return a status tuple
                # For now, assume skipped if it returns None in dry run
                skipped_count += 1
            elif target_subscription:
                print(
                    f"Successfully handled subscription for customer {target_subscription['customer']}"
                )
                created_count += 1
                # TODO: Add robust testing before enabling cancellation.
                # Consider potential race conditions or failures during migration.
                # if not dry_run:
                #     try:
                #         source_stripe.Subscription.delete(subscription['id'])
                #         print(f"Cancelled source subscription {subscription['id']}")
                #     except stripe.error.StripeError as e:
                #         print(f"Error cancelling source subscription {subscription['id']}: {e}")
            else:
                # Handles skips (duplicate found) or failures (errors during process)
                print(
                    f"Skipped or failed recreating source subscription {subscription.id} (Dry Run: {dry_run})"
                )
                # We need to differentiate skips from failures if possible
                # Based on current logs, assume failure if not dry_run and target_subscription is None
                # This assumes the duplicate check logs clearly and returns None
                # Let's increment a general 'skipped/failed' counter
                skipped_count += 1  # Or should this be failed_count? Ambiguous without better return value.

        print(f"\nSubscription migration process completed (Dry Run: {dry_run}).")
        if not dry_run:
            print(f"  Created: {created_count}")
            print(f"  Skipped/Failed: {skipped_count}")  # Needs refinement for accuracy
        else:
            print(
                f"  Subscriptions processed (dry run): {skipped_count}"
            )  # In dry run, all are effectively 'skipped'
    except stripe.error.StripeError as e:
        print(f"Error fetching subscriptions from source account: {e}")


def main() -> None:
    """Main function to run the subscription migration."""
    parser = argparse.ArgumentParser(description="Migrate Stripe Subscriptions.")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Perform the migration live. Default is dry run.",
    )
    args = parser.parse_args()

    is_dry_run = not args.live  # dry_run is True if --live is NOT specified

    # Set dry_run=False to perform actual migration
    migrate_subscriptions(dry_run=is_dry_run)


if __name__ == "__main__":
    main()
