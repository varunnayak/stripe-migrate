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
    dry_run: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Recreates a given subscription in the target Stripe account.

    Args:
        subscription: The subscription object from the source Stripe account.
        price_mapping: A dictionary mapping source price IDs to target price IDs.
        dry_run: If True, simulates the process without creating the subscription.

    Returns:
        The newly created subscription object from the target Stripe account, or None if
        skipped or in dry_run mode.
    """
    if not price_mapping:
        print(
            "Error: Price mapping is empty or could not be generated. Cannot proceed."
        )
        return None

    customer_id: str = subscription["customer"]
    print(f"Processing subscription for customer: {customer_id}")

    try:
        items: List[Dict[str, str]] = [
            {"price": price_mapping[item["price"]["id"]]}
            for item in subscription["items"]["data"]
        ]
        print(f"Mapped items: {items}")
    except KeyError as e:
        print(
            f"Error: Price ID {e} not found in price_mapping. Skipping subscription for customer {customer_id}."
        )
        return None

    if dry_run:
        print(f"  [Dry Run] Would create subscription for customer {customer_id}")
        print(f"    Items: {items}")
        print(
            f"    Trial End (from source current_period_end): {subscription['current_period_end']}"
        )
        print(f"    Default Payment Method: {None}")
        print("  [Dry Run] Subscription creation skipped.")
        return None

    source_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    target_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    # Fetch payment methods for the customer from the source account
    try:
        payment_methods = source_stripe.payment_methods.list(
            params={"customer": customer_id, "type": "card"}
        )
        payment_method_id: Optional[str] = None

        if payment_methods.data:
            payment_method = payment_methods.data[0]
            payment_method_id = payment_method.id
            print(f"Using payment method: {payment_method_id}")
        else:
            print(
                f"No suitable payment methods found for customer {customer_id}. Skipping subscription creation."
            )
            return None
    except stripe.error.StripeError as e:
        print(f"Error fetching payment methods for customer {customer_id}: {e}")
        return None

    # Create the subscription in the target account
    try:
        target_subscription = target_stripe.subscriptions.create(
            params={
                "customer": customer_id,
                "items": items,
                "trial_end": subscription["current_period_end"],
                "default_payment_method": payment_method_id,
            }
        )
        print(f"Successfully created target subscription {target_subscription.id}")
        return target_subscription
    except stripe.error.StripeError as e:
        print(f"Error creating subscription for customer {customer_id}: {e}")
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
        # Loop through each subscription and recreate it in the target account
        for subscription in subscriptions.auto_paging_iter():
            # print(subscription) # Uncomment for detailed subscription info
            target_subscription = recreate_subscription(
                subscription, price_mapping, dry_run  # Pass the map
            )
            if target_subscription:
                print(
                    f"Recreated subscription {target_subscription['id']} for customer {target_subscription['customer']}"
                )
                # TODO: Add robust testing before enabling cancellation.
                # Consider potential race conditions or failures during migration.
                # if not dry_run:
                #     try:
                #         source_stripe.Subscription.delete(subscription['id'])
                #         print(f"Cancelled source subscription {subscription['id']}")
                #     except stripe.error.StripeError as e:
                #         print(f"Error cancelling source subscription {subscription['id']}: {e}")
            else:
                print(
                    f"Skipped or failed recreating subscription {subscription.id} (Dry Run: {dry_run})"
                )

        print(f"\nSubscription migration process completed (Dry Run: {dry_run}).")
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
