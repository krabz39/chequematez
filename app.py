import csv
import io
import os
import secrets
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from supabase import Client, create_client
from supabase.lib.client_options import ClientOptions


# ============================================================
# CHEQUEMATEZ
# ============================================================
#
# Production architecture
#
# Vercel
#    │
#    ▼
# Flask
#    │
#    ▼
# Supabase
#    ├── Auth
#    ├── PostgreSQL
#    ├── RLS
#    ├── Profiles
#    ├── Exchange Rates
#    ├── M-Pesa Fee Bands
#    ├── Pricing Config
#    ├── Transactions
#    ├── Transaction Events
#    └── Audit Events
#
# Financial configuration is NEVER hardcoded here.
#
# ============================================================


# ============================================================
# ENVIRONMENT
# ============================================================

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()

SUPABASE_SERVICE_ROLE_KEY = os.getenv(
    "SUPABASE_SERVICE_ROLE_KEY",
    ""
).strip()

FLASK_SECRET_KEY = os.getenv(
    "FLASK_SECRET_KEY",
    ""
).strip()


if not SUPABASE_URL:
    raise RuntimeError(
        "Missing SUPABASE_URL environment variable."
    )

if not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError(
        "Missing SUPABASE_SERVICE_ROLE_KEY environment variable."
    )

if not FLASK_SECRET_KEY:
    raise RuntimeError(
        "Missing FLASK_SECRET_KEY environment variable."
    )


# ============================================================
# FLASK
# ============================================================

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)

app.secret_key = FLASK_SECRET_KEY

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=604800,
)


# ============================================================
# SUPABASE SERVER CLIENT
# ============================================================
#
# The service-role/secret key is SERVER ONLY.
# Never place this key inside HTML or JavaScript.
#
# Supabase recommends using the secret key only in trusted
# server-side environments for admin operations.
#
# ============================================================

supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    options=ClientOptions(
        auto_refresh_token=False,
        persist_session=False,
    ),
)


# ============================================================
# CONSTANTS
# ============================================================

VALID_DIRECTIONS = {
    "kwd_to_kes",
    "kes_to_kwd",
}

VALID_ROLES = {
    "customer",
    "merchant",
    "admin",
}

VALID_TRANSACTION_STATUSES = {
    "pending",
    "processing",
    "completed",
    "failed",
    "cancelled",
    "reversed",
    "manual_review",
}


# ============================================================
# GENERAL HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def utc_iso():
    return utc_now().isoformat()


def to_decimal(value, default=None):
    if value is None:
        return default

    try:
        return Decimal(str(value))
    except (
        InvalidOperation,
        TypeError,
        ValueError,
    ):
        return default


def round_money(value, places=2):
    value = to_decimal(
        value,
        Decimal("0"),
    )

    quantizer = Decimal("1").scaleb(-places)

    return value.quantize(
        quantizer,
        rounding=ROUND_HALF_UP,
    )


def error_response(
    message,
    status=400,
):
    return jsonify(
        {
            "ok": False,
            "error": message,
        }
    ), status


def success_response(
    data=None,
    status=200,
):
    payload = {
        "ok": True,
    }

    if isinstance(data, dict):
        payload.update(data)

    return jsonify(payload), status


def generate_reference():
    """
    Generates a transaction reference.

    The prefix is configurable through Vercel.
    """

    prefix = os.getenv(
        "TRANSACTION_REFERENCE_PREFIX",
        "CM",
    ).strip()

    if not prefix:
        prefix = "CM"

    timestamp = utc_now().strftime(
        "%Y%m%d%H%M%S"
    )

    random_part = secrets.token_hex(4).upper()

    return (
        f"{prefix}-"
        f"{timestamp}-"
        f"{random_part}"
    )


def log_error(label, error):
    print(
        f"[ChequeMatez] {label}: {error}"
    )


# ============================================================
# PROFILE
# ============================================================

def get_profile(user_id):
    result = (
        supabase
        .table("profiles")
        .select(
            """
            id,
            email,
            phone,
            full_name,
            username,
            role,
            status,
            kyc_status,
            created_at,
            updated_at
            """
        )
        .eq(
            "id",
            user_id,
        )
        .limit(1)
        .execute()
    )

    if not result.data:
        return None

    return result.data[0]


def current_profile():
    user_id = session.get("user_id")

    if not user_id:
        return None

    try:
        return get_profile(user_id)
    except Exception as error:
        log_error(
            "current_profile",
            error,
        )
        return None


def is_logged_in():
    return bool(
        session.get("user_id")
    )


def current_user_role():
    return session.get(
        "role",
        "customer",
    )


def is_admin():
    return (
        is_logged_in()
        and current_user_role() == "admin"
    )


def is_merchant():
    return (
        is_logged_in()
        and current_user_role() == "merchant"
    )


def establish_session(profile):
    session.clear()

    session.permanent = True

    session["user_id"] = profile["id"]
    session["username"] = profile.get(
        "username"
    )
    session["role"] = profile.get(
        "role",
        "customer",
    )


def clear_session():
    session.clear()


# ============================================================
# AUTH GUARDS
# ============================================================

def require_login():

    if not is_logged_in():

        return error_response(
            "Authentication required.",
            401,
        )

    return None


def require_admin():

    if not is_logged_in():

        return error_response(
            "Authentication required.",
            401,
        )

    if not is_admin():

        return error_response(
            "Administrator access required.",
            403,
        )

    return None


def require_merchant():

    if not is_logged_in():

        return error_response(
            "Authentication required.",
            401,
        )

    if not (
        is_admin()
        or is_merchant()
    ):

        return error_response(
            "Merchant access required.",
            403,
        )

    return None


# ============================================================
# SUPABASE: EXCHANGE RATE
# ============================================================

def get_active_exchange_rate():

    result = (
        supabase
        .table("exchange_rates")
        .select(
            """
            id,
            currency_from,
            currency_to,
            rate,
            active,
            effective_from,
            effective_until,
            created_by,
            created_at
            """
        )
        .eq(
            "currency_from",
            "KWD",
        )
        .eq(
            "currency_to",
            "KES",
        )
        .eq(
            "active",
            True,
        )
        .order(
            "effective_from",
            desc=True,
        )
        .limit(1)
        .execute()
    )

    if not result.data:

        raise RuntimeError(
            "No active KWD/KES exchange rate exists in Supabase."
        )

    record = result.data[0]

    rate = to_decimal(
        record.get("rate")
    )

    if rate is None or rate <= 0:

        raise RuntimeError(
            "The active KWD/KES exchange rate is invalid."
        )

    return rate, record


# ============================================================
# SUPABASE: M-PESA FEES
# ============================================================

def get_active_mpesa_fee_bands():

    result = (
        supabase
        .table("mpesa_fee_bands")
        .select(
            """
            id,
            min_amount_kes,
            max_amount_kes,
            fee_kes,
            active,
            created_at
            """
        )
        .eq(
            "active",
            True,
        )
        .order(
            "min_amount_kes",
            desc=False,
        )
        .execute()
    )

    return result.data or []


def get_mpesa_fee(
    amount_kes,
):

    amount_kes = to_decimal(
        amount_kes
    )

    if amount_kes is None:

        raise ValueError(
            "Invalid KES amount."
        )

    bands = get_active_mpesa_fee_bands()

    for band in bands:

        minimum = to_decimal(
            band.get(
                "min_amount_kes"
            )
        )

        maximum = to_decimal(
            band.get(
                "max_amount_kes"
            )
        )

        fee = to_decimal(
            band.get(
                "fee_kes"
            )
        )

        if (
            minimum is None
            or maximum is None
            or fee is None
        ):
            continue

        if (
            minimum
            <= amount_kes
            <= maximum
        ):
            return fee, band

    raise RuntimeError(
        "No M-Pesa fee band covers this amount."
    )


# ============================================================
# SUPABASE: PRICING CONFIG
# ============================================================

def get_active_pricing_config():

    result = (
        supabase
        .table("pricing_config")
        .select(
            """
            id,
            competitor_fee_kwd,
            distributor_fee_kwd,
            base_profit_kwd,
            max_margin_kwd,
            active,
            created_by,
            created_at
            """
        )
        .eq(
            "active",
            True,
        )
        .order(
            "created_at",
            desc=True,
        )
        .limit(1)
        .execute()
    )

    if not result.data:

        raise RuntimeError(
            "No active pricing configuration exists in Supabase."
        )

    config = result.data[0]

    fields = [
        "competitor_fee_kwd",
        "distributor_fee_kwd",
        "base_profit_kwd",
        "max_margin_kwd",
    ]

    for field in fields:

        value = to_decimal(
            config.get(field)
        )

        if value is None:

            raise RuntimeError(
                f"Invalid pricing configuration: {field}"
            )

    return config


# ============================================================
# PRICING ENGINE
# ============================================================

def calculate_pricing(
    amount_kwd,
    amount_kes,
    exchange_rate,
):

    config = get_active_pricing_config()

    competitor_fee = to_decimal(
        config[
            "competitor_fee_kwd"
        ]
    )

    distributor_fee = to_decimal(
        config[
            "distributor_fee_kwd"
        ]
    )

    base_profit = to_decimal(
        config[
            "base_profit_kwd"
        ]
    )

    max_margin = to_decimal(
        config[
            "max_margin_kwd"
        ]
    )

    safaricom_fee_kes, fee_band = (
        get_mpesa_fee(
            amount_kes
        )
    )

    safaricom_fee_kwd = (
        safaricom_fee_kes
        / exchange_rate
    )

    available_margin = (
        competitor_fee
        - distributor_fee
        - base_profit
    )

    if available_margin < 0:
        available_margin = Decimal("0")

    margin_kwd = (
        available_margin
        - safaricom_fee_kwd
    )

    if margin_kwd < 0:
        margin_kwd = Decimal("0")

    if margin_kwd > max_margin:
        margin_kwd = max_margin

    total_fee_kwd = (
        safaricom_fee_kwd
        + margin_kwd
    )

    profit_kwd = (
        margin_kwd
        + base_profit
        - safaricom_fee_kwd
    )

    return {
        "safaricom_fee_kes":
            safaricom_fee_kes,

        "safaricom_fee_kwd":
            safaricom_fee_kwd,

        "margin_kwd":
            margin_kwd,

        "total_fee_kwd":
            total_fee_kwd,

        "profit_kwd":
            profit_kwd,

        "competitor_fee_kwd":
            competitor_fee,

        "distributor_fee_kwd":
            distributor_fee,

        "base_profit_kwd":
            base_profit,

        "max_margin_kwd":
            max_margin,

        "fee_band":
            fee_band,

        "pricing_config_id":
            config["id"],
    }


# ============================================================
# TRANSFER CALCULATOR
# ============================================================

def calculate_transfer(
    amount,
    currency,
    fee_pass_through=False,
):

    amount = to_decimal(
        amount
    )

    if amount is None or amount <= 0:

        raise ValueError(
            "Amount must be greater than zero."
        )

    currency = str(
        currency or "KWD"
    ).strip().upper()

    if currency not in {
        "KWD",
        "KES",
    }:

        raise ValueError(
            "Currency must be KWD or KES."
        )

    exchange_rate, rate_record = (
        get_active_exchange_rate()
    )

    if currency == "KWD":

        amount_kwd = amount

        amount_kes = (
            amount_kwd
            * exchange_rate
        )

    else:

        amount_kes = amount

        amount_kwd = (
            amount_kes
            / exchange_rate
        )

    pricing = calculate_pricing(
        amount_kwd=amount_kwd,
        amount_kes=amount_kes,
        exchange_rate=exchange_rate,
    )

    total_fee_kwd = pricing[
        "total_fee_kwd"
    ]

    total_fee_kes = (
        total_fee_kwd
        * exchange_rate
    )

    if currency == "KWD":

        if fee_pass_through:

            total_cost_kwd = (
                amount_kwd
                + total_fee_kwd
            )

            total_cost_kes = (
                total_cost_kwd
                * exchange_rate
            )

        else:

            total_cost_kwd = amount_kwd
            total_cost_kes = amount_kes

    else:

        if fee_pass_through:

            total_cost_kes = (
                amount_kes
                + total_fee_kes
            )

            total_cost_kwd = (
                total_cost_kes
                / exchange_rate
            )

        else:

            total_cost_kes = amount_kes
            total_cost_kwd = amount_kwd

    return {
        "exchange_rate": float(
            round_money(
                exchange_rate,
                6,
            )
        ),

        "rate_id":
            rate_record["id"],

        "amount_kwd": float(
            round_money(
                amount_kwd
            )
        ),

        "amount_kes": float(
            round_money(
                amount_kes
            )
        ),

        "total_fee_kwd": float(
            round_money(
                total_fee_kwd
            )
        ),

        "total_fee_kes": float(
            round_money(
                total_fee_kes
            )
        ),

        "total_cost_kwd": float(
            round_money(
                total_cost_kwd
            )
        ),

        "total_cost_kes": float(
            round_money(
                total_cost_kes
            )
        ),

        "safaricom_fee_kes": float(
            round_money(
                pricing[
                    "safaricom_fee_kes"
                ]
            )
        ),

        "safaricom_fee_kwd": float(
            round_money(
                pricing[
                    "safaricom_fee_kwd"
                ],
                6,
            )
        ),

        "margin_kwd": float(
            round_money(
                pricing[
                    "margin_kwd"
                ],
                6,
            )
        ),

        "profit_kwd": float(
            round_money(
                pricing[
                    "profit_kwd"
                ],
                6,
            )
        ),

        "fee_band_id":
            pricing[
                "fee_band"
            ]["id"],

        "pricing_config_id":
            pricing[
                "pricing_config_id"
            ],

        "currency":
            currency,
    }


# ============================================================
# AUDIT EVENTS
# ============================================================

def create_audit_event(
    actor_id,
    entity_type,
    entity_id,
    action,
    old_values=None,
    new_values=None,
):

    try:

        (
            supabase
            .table("audit_events")
            .insert(
                {
                    "actor_id":
                        actor_id,

                    "entity_type":
                        entity_type,

                    "entity_id":
                        entity_id,

                    "action":
                        action,

                    "old_values":
                        old_values,

                    "new_values":
                        new_values,
                }
            )
            .execute()
        )

    except Exception as error:

        log_error(
            "audit_event",
            error,
        )


# ============================================================
# TRANSACTION EVENTS
# ============================================================

def create_transaction_event(
    transaction_id,
    event_type,
    status=None,
    provider=None,
    provider_reference=None,
    payload=None,
):

    (
        supabase
        .table("transaction_events")
        .insert(
            {
                "transaction_id":
                    transaction_id,

                "event_type":
                    event_type,

                "status":
                    status,

                "provider":
                    provider,

                "provider_reference":
                    provider_reference,

                "payload":
                    payload,
            }
        )
        .execute()
    )


# ============================================================
# PUBLIC PAGES
# ============================================================

@app.route("/")
def home():

    return render_template(
        "chapaa.html"
    )


@app.route("/chapaa")
@app.route("/chapaa.html")
def chapaa():

    return render_template(
        "chapaa.html"
    )


@app.route("/signup")
@app.route("/signup.html")
def signup():

    return render_template(
        "signup.html"
    )


@app.route("/login")
@app.route("/login.html")
def login():

    return render_template(
        "login.html"
    )


# ============================================================
# AUTH STATUS
# ============================================================

@app.route(
    "/auth/status",
    methods=["GET"],
)
def auth_status():

    profile = current_profile()

    if not profile:

        return jsonify(
            {
                "authenticated": False,
                "user": None,
            }
        )

    return jsonify(
        {
            "authenticated": True,
            "user": profile,
        }
    )


@app.route("/logout")
def logout():

    clear_session()

    return redirect(
        url_for("chapaa")
    )


# ============================================================
# EXCHANGE RATE API
# ============================================================

@app.route(
    "/api/rate",
    methods=["GET"],
)
def api_rate():

    try:

        rate, record = (
            get_active_exchange_rate()
        )

        return jsonify(
            {
                "ok": True,
                "rate": float(
                    round_money(
                        rate,
                        6,
                    )
                ),
                "currency_from":
                    record[
                        "currency_from"
                    ],
                "currency_to":
                    record[
                        "currency_to"
                    ],
                "effective_from":
                    record[
                        "effective_from"
                    ],
                "rate_id":
                    record["id"],
            }
        )

    except Exception as error:

        log_error(
            "api_rate",
            error,
        )

        return error_response(
            "No active exchange rate is configured.",
            503,
        )


# ============================================================
# CALCULATOR
# ============================================================

@app.route(
    "/calculate",
    methods=["POST"],
)
def calculate():

    try:

        amount = request.form.get(
            "amount"
        )

        currency = request.form.get(
            "currency",
            "KWD",
        )

        fee_pass_through = (
            request.form.get(
                "fee_pass_through",
                "",
            ).lower()
            in {
                "1",
                "true",
                "yes",
                "on",
            }
        )

        result = calculate_transfer(
            amount=amount,
            currency=currency,
            fee_pass_through=fee_pass_through,
        )

        return jsonify(
            result
        )

    except ValueError as error:

        return error_response(
            str(error),
            400,
        )

    except Exception as error:

        log_error(
            "calculate",
            error,
        )

        return error_response(
            "Calculator temporarily unavailable.",
            503,
        )


# ============================================================
# CREATE TRANSACTION
# ============================================================

@app.route(
    "/api/add-transaction",
    methods=["POST"],
)
def add_transaction():

    guard = require_login()

    if guard:
        return guard

    payload = (
        request.get_json(
            silent=True
        )
        or request.form.to_dict()
    )

    try:

        amount = to_decimal(
            payload.get("amount")
        )

        if amount is None or amount <= 0:

            return error_response(
                "Invalid transaction amount."
            )

        direction = str(
            payload.get(
                "direction",
                "kwd_to_kes",
            )
        ).strip().lower()

        if direction not in VALID_DIRECTIONS:

            return error_response(
                "Invalid transaction direction."
            )

        if direction == "kwd_to_kes":

            source_currency = "KWD"
            destination_currency = "KES"

        else:

            source_currency = "KES"
            destination_currency = "KWD"

        calculation = calculate_transfer(
            amount=amount,
            currency=source_currency,
            fee_pass_through=False,
        )

        reference = generate_reference()

        transaction = {
            "reference":
                reference,

            "user_id":
                session["user_id"],

            "merchant_id":
                payload.get(
                    "merchant_id"
                ) or None,

            "direction":
                direction,

            "status":
                "pending",

            "source_currency":
                source_currency,

            "destination_currency":
                destination_currency,

            "input_amount":
                float(amount),

            "exchange_rate":
                calculation[
                    "exchange_rate"
                ],

            "gross_amount":
                (
                    calculation[
                        "amount_kes"
                    ]
                    if direction
                    == "kwd_to_kes"
                    else
                    calculation[
                        "amount_kwd"
                    ]
                ),

            "fee_amount":
                calculation[
                    "total_fee_kwd"
                ],

            "total_amount":
                (
                    calculation[
                        "amount_kes"
                    ]
                    if direction
                    == "kwd_to_kes"
                    else
                    calculation[
                        "amount_kwd"
                    ]
                ),

            "safaricom_fee_kes":
                calculation[
                    "safaricom_fee_kes"
                ],

            "safaricom_fee_kwd":
                calculation[
                    "safaricom_fee_kwd"
                ],

            "distributor_fee_kwd":
                None,

            "profit_kwd":
                calculation[
                    "profit_kwd"
                ],

            "margin_kwd":
                calculation[
                    "margin_kwd"
                ],

            "payment_method":
                payload.get(
                    "payment_method"
                ),

            "payment_reference":
                payload.get(
                    "payment_reference"
                ),
        }

        result = (
            supabase
            .table("transactions")
            .insert(transaction)
            .execute()
        )

        if not result.data:

            return error_response(
                "Transaction could not be created.",
                500,
            )

        created = result.data[0]

        create_transaction_event(
            transaction_id=created["id"],
            event_type="transaction_created",
            status="pending",
            provider=payload.get(
                "payment_method"
            ),
            provider_reference=payload.get(
                "payment_reference"
            ),
            payload={
                "direction":
                    direction,

                "source_currency":
                    source_currency,

                "destination_currency":
                    destination_currency,
            },
        )

        create_audit_event(
            actor_id=session["user_id"],
            entity_type="transaction",
            entity_id=created["id"],
            action="transaction_created",
            new_values={
                "reference":
                    created["reference"],

                "status":
                    created["status"],
            },
        )

        return jsonify(
            {
                "ok": True,
                "transaction": created,
            }
        ), 201

    except Exception as error:

        log_error(
            "add_transaction",
            error,
        )

        return error_response(
            "Unable to create transaction.",
            500,
        )


# ============================================================
# MY TRANSACTIONS
# ============================================================

@app.route(
    "/api/my-transactions",
    methods=["GET"],
)
def my_transactions():

    guard = require_login()

    if guard:
        return guard

    try:

        result = (
            supabase
            .table("transactions")
            .select("*")
            .eq(
                "user_id",
                session["user_id"],
            )
            .order(
                "created_at",
                desc=True,
            )
            .limit(100)
            .execute()
        )

        return jsonify(
            {
                "ok": True,
                "transactions":
                    result.data or [],
            }
        )

    except Exception as error:

        log_error(
            "my_transactions",
            error,
        )

        return error_response(
            "Unable to load transactions.",
            500,
        )


# ============================================================
# USER LOOKUP
# ============================================================

@app.route(
    "/api/user/<username>",
    methods=["GET"],
)
def api_user(username):

    username = (
        username
        .strip()
        .lower()
    )

    try:

        result = (
            supabase
            .table("profiles")
            .select(
                """
                id,
                full_name,
                username,
                role,
                status,
                kyc_status,
                created_at
                """
            )
            .eq(
                "username",
                username,
            )
            .limit(1)
            .execute()
        )

        if not result.data:

            return error_response(
                "User not found.",
                404,
            )

        return jsonify(
            {
                "ok": True,
                "user":
                    result.data[0],
            }
        )

    except Exception as error:

        log_error(
            "api_user",
            error,
        )

        return error_response(
            "Unable to resolve user.",
            500,
        )


@app.route(
    "/api/resolve-userid",
    methods=["GET"],
)
def resolve_userid():

    username = (
        request.args.get(
            "username",
            "",
        )
        .strip()
        .lower()
    )

    if not username:

        return error_response(
            "Username is required."
        )

    try:

        result = (
            supabase
            .table("profiles")
            .select(
                """
                id,
                username,
                role,
                status
                """
            )
            .eq(
                "username",
                username,
            )
            .limit(1)
            .execute()
        )

        if not result.data:

            return error_response(
                "User not found.",
                404,
            )

        return jsonify(
            {
                "ok": True,
                "user":
                    result.data[0],
            }
        )

    except Exception as error:

        log_error(
            "resolve_userid",
            error,
        )

        return error_response(
            "Unable to resolve user.",
            500,
        )


# ============================================================
# WAMD CONFIGURATION
# ============================================================
#
# WAMD details belong in Vercel environment variables.
#
# ============================================================

@app.route(
    "/api/wamd/settings",
    methods=["GET"],
)
def wamd_settings():

    phone = os.getenv(
        "WAMD_PHONE",
        "",
    ).strip()

    beneficiary = os.getenv(
        "WAMD_BENEFICIARY",
        "",
    ).strip()

    currency = os.getenv(
        "WAMD_CURRENCY",
        "",
    ).strip()

    prefix = os.getenv(
        "WAMD_REFERENCE_PREFIX",
        "",
    ).strip()

    logo = os.getenv(
        "WAMD_LOGO_URL",
        "",
    ).strip()

    return jsonify(
        {
            "ok": True,

            "phone":
                phone,

            "wamd_phone":
                phone,

            "recipient_phone":
                phone,

            "beneficiary":
                beneficiary,

            "bene":
                beneficiary,

            "currency":
                currency,

            "ccy":
                currency,

            "reference_prefix":
                prefix,

            "prefix":
                prefix,

            "logo_url":
                logo,

            "logo":
                logo,
        }
    )


# ============================================================
# RECEIPT
# ============================================================

@app.route(
    "/receipt/<reference>",
    methods=["GET"],
)
def receipt(reference):

    if not is_logged_in():

        return (
            "Authentication required.",
            401,
        )

    try:

        result = (
            supabase
            .table("transactions")
            .select("*")
            .eq(
                "reference",
                reference,
            )
            .limit(1)
            .execute()
        )

        if not result.data:

            return (
                "Receipt not found.",
                404,
            )

        transaction = result.data[0]

        allowed = (
            is_admin()
            or transaction["user_id"]
            == session["user_id"]
        )

        if not allowed:

            return (
                "Unauthorized.",
                403,
            )

        return render_template(
            "receipt.html",
            transaction=transaction,
        )

    except Exception as error:

        log_error(
            "receipt",
            error,
        )

        return (
            "Unable to load receipt.",
            500,
        )


# ============================================================
# WAMD TRANSACTION
# ============================================================

@app.route(
    "/wamd/<reference>",
    methods=["GET"],
)
def wamd_transaction(reference):

    if not is_logged_in():

        return (
            "Authentication required.",
            401,
        )

    try:

        result = (
            supabase
            .table("transactions")
            .select("*")
            .eq(
                "reference",
                reference,
            )
            .limit(1)
            .execute()
        )

        if not result.data:

            return (
                "Transaction not found.",
                404,
            )

        transaction = result.data[0]

        allowed = (
            is_admin()
            or transaction["user_id"]
            == session["user_id"]
        )

        if not allowed:

            return (
                "Unauthorized.",
                403,
            )

        template_names = [
            "wamd.html",
            "wamd-payment.html",
            "wamd_payment.html",
        ]

        for template_name in template_names:

            template_path = os.path.join(
                app.template_folder,
                template_name,
            )

            if os.path.isfile(
                template_path
            ):

                return render_template(
                    template_name,
                    transaction=transaction,
                )

        return jsonify(
            {
                "ok": True,
                "transaction":
                    transaction,
            }
        )

    except Exception as error:

        log_error(
            "wamd_transaction",
            error,
        )

        return (
            "Unable to load WAMD transaction.",
            500,
        )


# ============================================================
# ADMIN DASHBOARD
# ============================================================

@app.route(
    "/admin",
    methods=["GET"],
)
def admin_dashboard():

    if not is_logged_in():

        return redirect(
            url_for("login")
        )

    if not is_admin():

        return redirect(
            url_for("chapaa")
        )

    return render_template(
        "admin.html"
    )


# ============================================================
# ADMIN TRANSACTIONS
# ============================================================

@app.route(
    "/admin/transactions",
    methods=["GET"],
)
def admin_transactions():

    guard = require_admin()

    if guard:
        return guard

    try:

        result = (
            supabase
            .table("transactions")
            .select("*")
            .order(
                "created_at",
                desc=True,
            )
            .limit(500)
            .execute()
        )

        return jsonify(
            {
                "ok": True,
                "transactions":
                    result.data or [],
            }
        )

    except Exception as error:

        log_error(
            "admin_transactions",
            error,
        )

        return error_response(
            "Unable to load transactions.",
            500,
        )


# ============================================================
# ADMIN RECONCILIATION CSV
# ============================================================

@app.route(
    "/admin/reconcile.csv",
    methods=["GET"],
)
def admin_reconcile_csv():

    guard = require_admin()

    if guard:
        return guard

    date_filter = (
        request.args.get(
            "date",
            "",
        )
        .strip()
    )

    try:

        query = (
            supabase
            .table("transactions")
            .select("*")
            .order(
                "created_at",
                desc=True,
            )
        )

        if date_filter:

            query = (
                query
                .gte(
                    "created_at",
                    f"{date_filter}T00:00:00+00:00",
                )
                .lt(
                    "created_at",
                    f"{date_filter}T23:59:59.999999+00:00",
                )
            )

        result = (
            query
            .limit(5000)
            .execute()
        )

        output = io.StringIO()

        writer = csv.writer(
            output
        )

        writer.writerow(
            [
                "reference",
                "user_id",
                "direction",
                "status",
                "source_currency",
                "destination_currency",
                "input_amount",
                "exchange_rate",
                "gross_amount",
                "fee_amount",
                "total_amount",
                "safaricom_fee_kes",
                "safaricom_fee_kwd",
                "margin_kwd",
                "profit_kwd",
                "payment_method",
                "payment_reference",
                "created_at",
                "completed_at",
            ]
        )

        for tx in result.data or []:

            writer.writerow(
                [
                    tx.get("reference"),
                    tx.get("user_id"),
                    tx.get("direction"),
                    tx.get("status"),
                    tx.get("source_currency"),
                    tx.get("destination_currency"),
                    tx.get("input_amount"),
                    tx.get("exchange_rate"),
                    tx.get("gross_amount"),
                    tx.get("fee_amount"),
                    tx.get("total_amount"),
                    tx.get("safaricom_fee_kes"),
                    tx.get("safaricom_fee_kwd"),
                    tx.get("margin_kwd"),
                    tx.get("profit_kwd"),
                    tx.get("payment_method"),
                    tx.get("payment_reference"),
                    tx.get("created_at"),
                    tx.get("completed_at"),
                ]
            )

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition":
                    "attachment; "
                    "filename=chequematez-reconciliation.csv"
            },
        )

    except Exception as error:

        log_error(
            "admin_reconcile_csv",
            error,
        )

        return (
            "Unable to generate reconciliation report.",
            500,
        )


# ============================================================
# ADMIN MONTHLY REPORT
# ============================================================

@app.route(
    "/admin/reports/monthly.csv",
    methods=["GET"],
)
def admin_monthly_report():

    guard = require_admin()

    if guard:
        return guard

    ym = (
        request.args.get(
            "ym",
            "",
        )
        .strip()
    )

    if (
        len(ym) != 7
        or ym[4] != "-"
    ):

        return (
            "Use YYYY-MM.",
            400,
        )

    try:

        year = int(
            ym[:4]
        )

        month = int(
            ym[5:7]
        )

        if month < 1 or month > 12:

            return (
                "Invalid month.",
                400,
            )

        start = (
            f"{year:04d}-"
            f"{month:02d}-01"
            "T00:00:00+00:00"
        )

        if month == 12:

            end = (
                f"{year + 1:04d}-01-01"
                "T00:00:00+00:00"
            )

        else:

            end = (
                f"{year:04d}-"
                f"{month + 1:02d}-01"
                "T00:00:00+00:00"
            )

        result = (
            supabase
            .table("transactions")
            .select("*")
            .gte(
                "created_at",
                start,
            )
            .lt(
                "created_at",
                end,
            )
            .order(
                "created_at",
                desc=False,
            )
            .limit(10000)
            .execute()
        )

        output = io.StringIO()

        writer = csv.writer(
            output
        )

        writer.writerow(
            [
                "reference",
                "direction",
                "status",
                "source_currency",
                "destination_currency",
                "input_amount",
                "exchange_rate",
                "gross_amount",
                "fee_amount",
                "total_amount",
                "safaricom_fee_kes",
                "safaricom_fee_kwd",
                "margin_kwd",
                "profit_kwd",
                "created_at",
                "completed_at",
            ]
        )

        for tx in result.data or []:

            writer.writerow(
                [
                    tx.get("reference"),
                    tx.get("direction"),
                    tx.get("status"),
                    tx.get("source_currency"),
                    tx.get("destination_currency"),
                    tx.get("input_amount"),
                    tx.get("exchange_rate"),
                    tx.get("gross_amount"),
                    tx.get("fee_amount"),
                    tx.get("total_amount"),
                    tx.get("safaricom_fee_kes"),
                    tx.get("safaricom_fee_kwd"),
                    tx.get("margin_kwd"),
                    tx.get("profit_kwd"),
                    tx.get("created_at"),
                    tx.get("completed_at"),
                ]
            )

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition":
                    "attachment; "
                    f"filename=chequematez-{ym}.csv"
            },
        )

    except Exception as error:

        log_error(
            "admin_monthly_report",
            error,
        )

        return (
            "Unable to generate monthly report.",
            500,
        )


# ============================================================
# ADMIN RISK
# ============================================================

@app.route(
    "/admin/risk/json",
    methods=["GET"],
)
def admin_risk():

    guard = require_admin()

    if guard:
        return guard

    try:

        result = (
            supabase
            .table("transactions")
            .select(
                """
                id,
                reference,
                user_id,
                status,
                input_amount,
                source_currency,
                destination_currency,
                created_at
                """
            )
            .in_(
                "status",
                [
                    "pending",
                    "manual_review",
                ],
            )
            .order(
                "created_at",
                desc=True,
            )
            .limit(500)
            .execute()
        )

        return jsonify(
            {
                "ok": True,
                "risk_transactions":
                    result.data or [],
            }
        )

    except Exception as error:

        log_error(
            "admin_risk",
            error,
        )

        return error_response(
            "Unable to load risk data.",
            500,
        )


# ============================================================
# ADMIN CREATE USER
# ============================================================

@app.route(
    "/admin/create-user",
    methods=["POST"],
)
def admin_create_user():

    guard = require_admin()

    if guard:
        return guard

    payload = (
        request.get_json(
            silent=True
        )
        or request.form.to_dict()
    )

    email = (
        str(
            payload.get(
                "email",
                "",
            )
        )
        .strip()
        .lower()
    )

    username = (
        str(
            payload.get(
                "username",
                "",
            )
        )
        .strip()
        .lower()
    )

    password = str(
        payload.get(
            "password",
            "",
        )
    )

    role = (
        str(
            payload.get(
                "role",
                "customer",
            )
        )
        .strip()
        .lower()
    )

    if not email:

        return error_response(
            "Email is required."
        )

    if not password:

        return error_response(
            "Password is required."
        )

    if role not in VALID_ROLES:

        return error_response(
            "Invalid role."
        )

    try:

        response = (
            supabase
            .auth
            .admin
            .create_user(
                {
                    "email": email,
                    "password": password,
                    "email_confirm": True,
                    "user_metadata": {
                        "username":
                            username,
                    },
                }
            )
        )

        user = response.user

        if not user:

            return error_response(
                "Unable to create user.",
                500,
            )

        profile = get_profile(
            user.id
        )

        if profile and (
            profile.get("role")
            != role
        ):

            (
                supabase
                .rpc(
                    "set_user_role",
                    {
                        "target_user_id":
                            user.id,

                        "new_role":
                            role,
                    },
                )
                .execute()
            )

        create_audit_event(
            actor_id=session[
                "user_id"
            ],
            entity_type="profile",
            entity_id=user.id,
            action="admin_user_created",
            new_values={
                "email":
                    email,

                "username":
                    username,

                "role":
                    role,
            },
        )

        return jsonify(
            {
                "ok": True,

                "user_id":
                    user.id,

                "email":
                    email,

                "username":
                    username,

                "role":
                    role,
            }
        ), 201

    except Exception as error:

        log_error(
            "admin_create_user",
            error,
        )

        return error_response(
            "Unable to create user.",
            500,
        )


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route(
    "/health",
    methods=["GET"],
)
def health():

    try:

        rate, _ = (
            get_active_exchange_rate()
        )

        return jsonify(
            {
                "ok": True,
                "service":
                    "ChequeMatez",
                "status":
                    "online",
                "supabase":
                    "connected",
                "exchange_rate_available":
                    rate > 0,
                "timestamp":
                    utc_iso(),
            }
        )

    except Exception as error:

        log_error(
            "health",
            error,
        )

        return jsonify(
            {
                "ok": False,
                "service":
                    "ChequeMatez",
                "status":
                    "degraded",
                "supabase":
                    "error",
                "timestamp":
                    utc_iso(),
            }
        ), 503


# ============================================================
# ERROR HANDLERS
# ============================================================

@app.errorhandler(404)
def handle_404(error):

    if request.path.startswith(
        "/api/"
    ):

        return error_response(
            "Endpoint not found.",
            404,
        )

    return (
        render_template(
            "chapaa.html"
        ),
        404,
    )


@app.errorhandler(500)
def handle_500(error):

    if request.path.startswith(
        "/api/"
    ):

        return error_response(
            "Internal server error.",
            500,
        )

    return (
        "ChequeMatez internal server error.",
        500,
    )


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "5000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )