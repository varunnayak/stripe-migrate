import json
import os
from typing import Any, Dict, List, Optional

import stripe

API_KEY_OLD: Optional[str] = os.getenv("API_KEY_OLD")
API_KEY_NEW: Optional[str] = os.getenv("API_KEY_NEW")
PRICE_MAPPING_JSON: str = os.getenv("PRICE_MAPPING_JSON", "{}")

# Ensure API keys are set
if not API_KEY_OLD:
    raise ValueError("API_KEY_OLD environment variable not set.")
if not API_KEY_NEW:
    raise ValueError("API_KEY_NEW environment variable not set.")


try:
    price_mapping: Dict[str, str] = json.loads(PRICE_MAPPING_JSON)
except json.JSONDecodeError:
    print("Error: Invalid JSON format for PRICE_MAPPING_JSON.")
    price_mapping = {}


def get_stripe_client(api_key: str) -> Any:
    """
    Returns a Stripe client initialized with the given API key.

    Args:
        api_key: The Stripe API key to use.

    Returns:
        An initialized Stripe client object.
    """
    stripe.api_key = api_key
    return stripe


# Function to recreate a subscription in the new account
def recreate_subscription(
    subscription: Dict[str, Any], dry_run: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Recreates a given subscription in the new Stripe account.

    Args:
        subscription: The subscription object from the old Stripe account.
        dry_run: If True, simulates the process without creating the subscription.

    Returns:
        The newly created subscription object from the new Stripe account, or None if
        skipped or in dry_run mode.
    """
    if not price_mapping:
        print("Error: Price mapping is empty. Cannot proceed.")
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
        print("Dry run: Subscription creation skipped.")
        return None

    old_stripe = get_stripe_client(API_KEY_OLD)  # type: ignore
    new_stripe = get_stripe_client(API_KEY_NEW)  # type: ignore

    # Fetch payment methods for the customer from the old account
    try:
        payment_methods = old_stripe.PaymentMethod.list(
            customer=customer_id, type="card"
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
        new_subscription = new_stripe.Subscription.create(
            customer=customer_id,
            items=items,
            # Set trial end to the end of the current period to avoid immediate billing
            trial_end=subscription["current_period_end"],
            default_payment_method=payment_method_id,
            # Consider adding other relevant parameters like coupon, metadata, etc.
        )
        print(f"Successfully created new subscription {new_subscription.id}")
        return new_subscription
    except stripe.error.StripeError as e:
        print(f"Error creating subscription for customer {customer_id}: {e}")
        return None


def migrate_subscriptions(dry_run: bool = True) -> None:
    """
    Fetches all active subscriptions from the old Stripe account and attempts
    to recreate them in the new Stripe account.
    """
    print(f"Starting subscription migration (dry_run={dry_run})...")
    old_stripe = get_stripe_client(API_KEY_OLD)  # type: ignore

    try:
        subscriptions = old_stripe.Subscription.list(status="active", limit=100)
        # Loop through each subscription and recreate it in the new account
        for subscription in subscriptions.auto_paging_iter():
            # print(subscription) # Uncomment for detailed subscription info
            new_subscription = recreate_subscription(subscription, dry_run)
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
                print(f"Skipped or failed recreating subscription {subscription.id}")

        print("\nSubscription migration process completed.")
    except stripe.error.StripeError as e:
        print(f"Error fetching subscriptions from old account: {e}")


def main() -> None:
    """Main function to run the subscription migration."""
    # Set dry_run=False to perform actual migration
    migrate_subscriptions(dry_run=True)


if __name__ == "__main__":
    main()
