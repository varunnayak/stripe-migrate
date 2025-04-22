# Stripe Data Migration Script

This script migrates data (Products, Prices, Coupons, Promotion Codes, and Subscriptions) between two Stripe accounts.

## Description

The script connects to a source Stripe account and a target Stripe account using API keys provided via environment variables. It allows for migrating:

1.  **Products and Prices:** Copies active products and their associated active prices from the source to the target account. It checks for existing products/prices in the target account (first by ID, then by metadata linking `source_price_id`) to avoid duplicates.
2.  **Coupons and Promotion Codes:** Copies valid coupons and their associated active promotion codes from the source to the target account. It checks for existing coupons (by ID) and promotion codes (by code string) in the target account.
3.  **Subscriptions:** Recreates active subscriptions from the source account in the target account. It requires products and prices to have been migrated previously, as it relies on a mapping between source and target price IDs (stored in target price metadata). It also attempts to find or set up a default payment method for the customer in the target account before creating the subscription.

The script includes a `--dry-run` mode (default) to simulate the migration without making any actual changes to the target account.

## Installation / Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```
2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    _(Note: You might need to create a `requirements.txt` file containing `stripe python-dotenv`)_
3.  **Set up environment variables:**
    Create a `.env` file in the project root directory with your Stripe API keys:
    ```dotenv
    API_KEY_SOURCE=sk_test_xxxxxxxxxxxxxxxxxxxxxx_source
    API_KEY_TARGET=sk_test_yyyyyyyyyyyyyyyyyyyyyy_target
    ```
    Replace the placeholder keys with your actual source and target account **secret keys**. **Never commit your API keys to version control.**

## Usage

The script is run from the command line.

```bash
python stripe_migrate.py --step <step_name> [options]
```

**Arguments:**

- `--step {products|coupons|subscriptions|all}`: (Required) Specifies which migration step to run.
  - `products`: Migrates products and their prices.
  - `coupons`: Migrates coupons and their promotion codes.
  - `subscriptions`: Migrates active subscriptions. **Requires products/prices to be migrated first.**
  - `all`: Runs all steps sequentially (products -> coupons -> subscriptions).
- `--live`: (Optional) Performs the migration live. If omitted, the script runs in **dry run mode** by default, only logging what actions _would_ be taken.
- `--debug`: (Optional) Enables detailed debug logging output.
- `--unarchive-prices`: (Optional) Explicitly indicates that inactive prices should be unarchived during migration (this is the default behavior).
- `--keep-price-status`: (Optional) Preserves the original active/inactive status of prices when migrating. Overrides `--unarchive-prices`.

**Examples:**

- **Dry run migrating only products:**
  ```bash
  python stripe_migrate.py --step products
  ```
- **Live migration of all data (products, coupons, subscriptions):**
  ```bash
  python stripe_migrate.py --step all --live
  ```
- **Dry run migrating subscriptions with debug output:**
  ```bash
  python stripe_migrate.py --step subscriptions --debug
  ```
- **Live migration of products while preserving price active/inactive status:**
  ```bash
  python stripe_migrate.py --step products --live --keep-price-status
  ```

## Important Considerations

- **Idempotency:** The script attempts to be idempotent by checking for existing resources (products by ID, prices by metadata, coupons by ID, promo codes by code string, subscriptions by customer and price set) before creating new ones. However, race conditions are possible if resources are created manually between the script's checks and creation attempts.
- **Metadata:** The script relies heavily on metadata:
  - It adds `source_price_id` to target prices.
  - It adds `source_promotion_code_id` to target promotion codes.
  - It adds `source_subscription_id` to target subscriptions.
    This metadata is crucial, especially for mapping prices during subscription migration.
- **Customers:** Before migrating subscriptions, customers must be copied from the source to target account. This can be done using the copy button in the Stripe dashboard's customers page. The script does not create customers; it expects them to already exist in the target account with the same IDs as in the source account.
- **Payment Methods:** For subscription migration, the script checks if the target customer has a default payment method or an attached card. If not, it **cannot** create the subscription. It **does not** currently migrate payment methods themselves due to complexity and security implications (often requiring customer interaction). You need to ensure customers have valid payment methods in the target account _before_ migrating subscriptions.
- **Archived Prices:** Subscription creation will fail if the corresponding price in the target account is archived. By default, the script will unarchive inactive prices during migration. You can preserve the original active/inactive status using the `--keep-price-status` flag.
- **Invalid Coupons:** Subscription creation will fail if an applied coupon is invalid due to maximum redemptions being reached or the redemption period having expired. The script does not modify coupon validity settings during migration.
- **Trial Periods:** Migrated subscriptions have their `trial_end` set to the `current_period_end` of the source subscription.
- **API Keys:** Ensure you are using the correct **secret keys** for both accounts. Using restricted keys might lead to permission errors.
- **Error Handling:** The script includes basic error handling for Stripe API calls, but complex scenarios might require manual intervention. Review the logs carefully.
- **Rate Limits:** For large numbers of resources, be mindful of Stripe API rate limits. The script processes items sequentially.

## Dependencies

- [Stripe Python Library](https://github.com/stripe/stripe-python)
- [python-dotenv](https://github.com/theskumar/python-dotenv)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details (or add license text directly here).

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request
