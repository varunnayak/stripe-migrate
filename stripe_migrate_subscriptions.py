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


# Function to recreate a subscription in the new account
def recreate_subscription(
    subscription: Dict[str, Any],
    price_mapping: Dict[str, str],  # Added price_mapping argument
    dry_run: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Recreates a given subscription in the new Stripe account.

    Args:
        subscription: The subscription object from the old Stripe account.
        price_mapping: A dictionary mapping old price IDs to new price IDs.
        dry_run: If True, simulates the process without creating the subscription.

    Returns:
        The newly created subscription object from the new Stripe account, or None if
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
            f"    Trial End (from old current_period_end): {subscription['current_period_end']}"
        )
        print(f"    Default Payment Method: {None}")
        print("  [Dry Run] Subscription creation skipped.")
        return None

    old_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore

    # Fetch payment methods for the customer from the old account
    try:
        payment_methods = old_stripe.payment_methods.list(
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

    # Create the subscription in the new account
    try:
        new_subscription = new_stripe.subscriptions.create(
            params={
                "customer": customer_id,
                "items": items,
                "trial_end": subscription["current_period_end"],
                "default_payment_method": payment_method_id,
            }
        )
        print(f"Successfully created new subscription {new_subscription.id}")
        return new_subscription
    except stripe.error.StripeError as e:
        print(f"Error creating subscription for customer {customer_id}: {e}")
        return None


def migrate_subscriptions(dry_run: bool = True) -> None:
    """
    Fetches all active subscriptions from the old Stripe account and attempts
    to recreate them in the new Stripe account. Dynamically builds price mapping.
    """
    print(f"Starting subscription migration (dry_run={dry_run})...")
    old_stripe = get_stripe_client(API_KEY_SOURCE)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_TARGET)  # type: ignore # Initialize new client here

    # --- Build price mapping dynamically ---
    print("Building price map from new account metadata...")
    price_mapping: Dict[str, str] = {}
    try:
        prices = new_stripe.prices.list(params={"limit": 100, "active": True})
        for price in prices.auto_paging_iter():
            if price.metadata and "old_price_id" in price.metadata:
                old_id = price.metadata["old_price_id"]
                price_mapping[old_id] = price.id
                # Optional: print mapping for verification
                # print(f"  Mapped old price {old_id} -> new price {price.id}")
        print(f"Price map built successfully. Found {len(price_mapping)} mappings.")
        if not price_mapping:
            print(
                "Warning: Price map is empty. Ensure products/prices were migrated correctly with 'old_price_id' in metadata."
            )

    except stripe.error.StripeError as e:
        print(f"Error fetching prices from new account to build map: {e}")
        print("Cannot proceed without price mapping.")
        return  # Exit if map cannot be built
    # --- End build price mapping ---

    try:
        subscriptions = old_stripe.subscriptions.list(
            params={"status": "active", "limit": 100}
        )
        print(
            f"Found {len(subscriptions.data)} active subscription(s) in the old account."
        )
        # Loop through each subscription and recreate it in the new account
        for subscription in subscriptions.auto_paging_iter():
            # print(subscription) # Uncomment for detailed subscription info
            new_subscription = recreate_subscription(
                subscription, price_mapping, dry_run  # Pass the map
            )
            if new_subscription:
                print(
                    f"Recreated subscription {new_subscription['id']} for customer {new_subscription['customer']}"
                )
                # TODO: Add robust testing before enabling cancellation.
                # Consider potential race conditions or failures during migration.
                # if not dry_run:
                #     try:
                #         old_stripe.Subscription.delete(subscription['id'])
                #         print(f"Cancelled old subscription {subscription['id']}")
                #     except stripe.error.StripeError as e:
                #         print(f"Error cancelling old subscription {subscription['id']}: {e}")
            else:
                print(
                    f"Skipped or failed recreating subscription {subscription.id} (Dry Run: {dry_run})"
                )

        print(f"\nSubscription migration process completed (Dry Run: {dry_run}).")
    except stripe.error.StripeError as e:
        print(f"Error fetching subscriptions from old account: {e}")


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
