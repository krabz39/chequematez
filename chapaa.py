# === chapaa.py (ALL-IN-ONE with Notifications, Reconciliation, Reports, Risk) ===
from flask import (
    Flask, request, jsonify, session,
    redirect, url_for, send_file,
    render_template, make_response, abort
)

from datetime import datetime, timedelta, timezone
from typing import Optional, List
import csv
import io
import re
import secrets
import hashlib

# === CORE SYSTEM / OS ===
import os
import base64
import uuid
import json
import time
import threading
import queue

from io import BytesIO

# === NETWORK / EXTERNAL ===
import requests
import qrcode

# === DATABASE (Neon PostgreSQL) ===
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

# ===== PDF engine (WeasyPrint only) =====
# pip install weasyprint
PDF_BACKEND = None
try:
    from weasyprint import HTML  # type: ignore
    PDF_BACKEND = "weasyprint"
except Exception:
    PDF_BACKEND = None

# =========================================================
# DATABASE CONFIG — NEON POSTGRESQL (SAFE, ENV-BASED)
# =========================================================

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set (Neon PostgreSQL required)")

# Connection pool (safe for Flask + background workers)
DB_POOL: psycopg2.pool.SimpleConnectionPool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=DATABASE_URL,
    cursor_factory=RealDictCursor,
    sslmode="require"
)

def db_conn():
    """
    Acquire a DB connection from pool.
    ALWAYS pair with db_release().
    """
    return DB_POOL.getconn()

def db_release(conn):
    if conn:
        DB_POOL.putconn(conn)

def db_cursor(commit: bool = False):
    """
    Context manager for DB cursor with automatic cleanup.
    """
    class _Ctx:
        def __enter__(self):
            self.conn = db_conn()
            self.cur = self.conn.cursor()
            return self.cur

        def __exit__(self, exc_type, exc, tb):
            try:
                if exc_type:
                    self.conn.rollback()
                elif commit:
                    self.conn.commit()
            finally:
                self.cur.close()
                db_release(self.conn)

    return _Ctx()

# =========================================================
# BOOTSTRAP SAFETY CHECK
# =========================================================

def db_healthcheck() -> None:
    """
    Ensures DB is reachable at startup.
    """
    try:
        with db_cursor() as cur:
            cur.execute("SELECT 1;")
    except Exception as e:
        raise RuntimeError(f"Database healthcheck failed: {e}")

db_healthcheck()
# IMPORTANT: point to your real templates/static folders
app = Flask(
    __name__,
    template_folder=r"templates",
    static_folder=r"static\images"
)

# =========================================================
# SECURITY / SESSION CONFIG
# =========================================================

# Secret key MUST come from environment in production
app.secret_key = os.getenv("APP_SECRET_KEY", "super-secret-key-change-me")

# Friendlier cookie settings for localhost testing
app.config.update(
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=bool(os.getenv("HTTPS_ENABLED", "0") == "1"),
)

# =========================================================
# FILE SYSTEM DIRECTORIES (UNCHANGED LOGIC)
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Global upload dir for KYC
KYC_UPLOAD_DIR = os.path.join(BASE_DIR, "uploads", "kyc")
os.makedirs(KYC_UPLOAD_DIR, exist_ok=True)

# =========================================================
# LEGACY JSON STATE (KEPT — FALLBACK / MIGRATION SOURCE)
# =========================================================
# NOTE:
# - This section is NOT removed
# - It is now treated as a fallback / migration buffer
# - PostgreSQL is the primary persistence layer
# - JSON is used ONLY if DB is unavailable or during migration

STATE_DIR = os.environ.get("CM_STATE_DIR") or os.path.join(BASE_DIR, "data")
os.makedirs(STATE_DIR, exist_ok=True)

STATE_FILE = os.path.join(STATE_DIR, "state.json")

_STATE_LOCK = threading.RLock()
_DIRTY = False
_SAVE_TIMER = None  # debounced save timer thread

# =========================================================
# DATABASE-BASED STATE FLAG (NEW, NON-DESTRUCTIVE)
# =========================================================

USE_DB_STATE = True   # master switch (can be toggled for emergency rollback)

def mark_dirty():
    """
    Marks in-memory state as dirty.
    With DB enabled, commits are immediate.
    JSON fallback still supported.
    """
    global _DIRTY
    with _STATE_LOCK:
        _DIRTY = True

def commit_state():
    """
    No-op when DB is primary.
    Retained to preserve original call structure.
    """
    if not USE_DB_STATE:
        return
# =========================================================
# STATE SAVE / LOAD — DB-FIRST WITH JSON FALLBACK
# =========================================================

def _save_soon(delay=0.35):
    """Debounced save within ~350ms after a write (reduces data loss on crashes)."""
    global _SAVE_TIMER
    try:
        if _SAVE_TIMER and _SAVE_TIMER.is_alive():
            return
    except Exception:
        pass

    def _run():
        try:
            time.sleep(delay)
            save_state(force=True)
        except Exception as e:
            print("[state] debounced save error:", e)

    _SAVE_TIMER = threading.Thread(target=_run, daemon=True)
    _SAVE_TIMER.start()


def _state_snapshot():
    """
    FULL legacy snapshot.
    KEPT EXACTLY for migration + fallback.
    """
    return {
        "USERS": USERS,
        "PROFILES": PROFILES,
        "KYC_CASES": KYC_CASES,
        "KYC_NEXT_ID": KYC_NEXT_ID,
        "TRANSACTIONS": TRANSACTIONS,
        "AUDIT_LOG": AUDIT_LOG,
        "NEXT_ID": NEXT_ID,
        "ADMIN_SETTINGS": ADMIN_SETTINGS,
        "V_VOUCHERS": V_VOUCHERS,
        "V_CHECKOUT_TO_CODE": V_CHECKOUT_TO_CODE,
        "V_MERCHANTS": V_MERCHANTS,
        "V_LEDGER": V_LEDGER,
        "V_TRANSACTIONS": V_TRANSACTIONS,
        "V_NEXT_ID": V_NEXT_ID,
        "MPESA_CALLBACKS": MPESA_CALLBACKS,
        "EXCHANGE_RATE": EXCHANGE_RATE
    }


def _state_load_into(mem):
    """
    Loads legacy JSON snapshot INTO MEMORY.
    DB remains authoritative if enabled.
    """
    global USERS, PROFILES, KYC_CASES, KYC_NEXT_ID
    global TRANSACTIONS, AUDIT_LOG, NEXT_ID
    global ADMIN_SETTINGS, V_VOUCHERS, V_CHECKOUT_TO_CODE
    global V_MERCHANTS, V_LEDGER, V_TRANSACTIONS, V_NEXT_ID
    global MPESA_CALLBACKS, EXCHANGE_RATE

    try: USERS = mem.get("USERS", USERS)
    except Exception: pass
    try: PROFILES = mem.get("PROFILES", PROFILES)
    except Exception: pass
    try: KYC_CASES = mem.get("KYC_CASES", KYC_CASES)
    except Exception: pass
    try: KYC_NEXT_ID = int(mem.get("KYC_NEXT_ID", len(KYC_CASES) + 1))
    except Exception: pass
    try: TRANSACTIONS = mem.get("TRANSACTIONS", TRANSACTIONS)
    except Exception: pass
    try: AUDIT_LOG = mem.get("AUDIT_LOG", AUDIT_LOG)
    except Exception: pass
    try: NEXT_ID = int(mem.get("NEXT_ID", (max([t.get("id", 0) for t in TRANSACTIONS], default=0) + 1)))
    except Exception: pass

    try: ADMIN_SETTINGS = mem.get("ADMIN_SETTINGS", ADMIN_SETTINGS)
    except Exception: pass
    try: V_VOUCHERS = mem.get("V_VOUCHERS", V_VOUCHERS)
    except Exception: pass
    try: V_CHECKOUT_TO_CODE = mem.get("V_CHECKOUT_TO_CODE", V_CHECKOUT_TO_CODE)
    except Exception: pass
    try: V_MERCHANTS = mem.get("V_MERCHANTS", V_MERCHANTS)
    except Exception: pass
    try: V_LEDGER = mem.get("V_LEDGER", V_LEDGER)
    except Exception: pass
    try: V_TRANSACTIONS = mem.get("V_TRANSACTIONS", V_TRANSACTIONS)
    except Exception: pass
    try: V_NEXT_ID = int(mem.get("V_NEXT_ID", (max([t.get("id", 0) for t in V_TRANSACTIONS], default=0) + 1)))
    except Exception: pass
    try: MPESA_CALLBACKS = mem.get("MPESA_CALLBACKS", MPESA_CALLBACKS)
    except Exception: pass

    try:
        xr = float(mem.get("EXCHANGE_RATE", EXCHANGE_RATE if EXCHANGE_RATE else 0) or 0)
        if xr > 0:
            EXCHANGE_RATE = xr
    except Exception:
        pass


def save_state(force=False):
    """
    DB-FIRST SAVE.
    JSON is fallback only.
    """
    global _DIRTY

    if USE_DB_STATE:
        _DIRTY = False
        return

    with _STATE_LOCK:
        if not force and not _DIRTY:
            return

        tmp = STATE_FILE + ".tmp"
        data = _state_snapshot()
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        os.replace(tmp, STATE_FILE)
        _DIRTY = False


def load_state():
    """
    Load JSON snapshot ONLY if DB unavailable.
    """
    if USE_DB_STATE:
        return

    try:
        if os.path.exists(STATE_FILE):
            with _STATE_LOCK:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    mem = json.load(f)
                _state_load_into(mem)
    except Exception as e:
        print("[state] load failed:", e)


def _touch():
    """
    Original API preserved.
    DB commits are immediate; JSON is debounced.
    """
    global _DIRTY
    with _STATE_LOCK:
        _DIRTY = True
    _save_soon()


def _autosave_worker():
    """
    Retained for legacy compatibility.
    No-op when DB is primary.
    """
    while True:
        time.sleep(2.0)
        try:
            save_state()
        except Exception as e:
            print("[state] autosave error:", e)
# =========================================================
# SAVE ON TEARDOWN / SIGNALS (UNCHANGED LOGIC)
# =========================================================

@app.teardown_appcontext
def _teardown_save_state(exc):
    try:
        save_state()
    except Exception:
        pass


def _install_signal_handlers():
    """
    Graceful shutdown (containers / SIGTERM / SIGINT)
    """
    try:
        import signal
        def _graceful(*_):
            try:
                save_state(force=True)
            finally:
                os._exit(0)
        signal.signal(signal.SIGTERM, _graceful)
        signal.signal(signal.SIGINT, _graceful)
    except Exception as e:
        print("[state] signal handlers not installed:", e)


# =========================================================
# ADMIN SETTINGS — ENV-FIRST (DB READY LATER)
# =========================================================

ADMIN_SETTINGS = {
    "wamd_pay_to_phone": os.getenv("WAMD_PAY_TO_PHONE", "+965914413"),
    "wamd_beneficiary": os.getenv("WAMD_BENEFICIARY", "Eugene"),
}

def _wamd_settings():
    return {
        "pay_to_phone": ADMIN_SETTINGS.get("wamd_pay_to_phone", "").strip(),
        "beneficiary": ADMIN_SETTINGS.get("wamd_beneficiary", "").strip()
    }


# =========================================================
# AUTH — HASHED USERS ONLY (NO PLAINTEXT)
# =========================================================

import os
from werkzeug.security import generate_password_hash, check_password_hash


# ---------------------------------------------------------
# Password hashing helper
# ---------------------------------------------------------
def _hash(p: str) -> str:
    """
    Generate a secure password hash.
    Used ONLY as a development fallback.
    """
    return generate_password_hash(
        p,
        method="pbkdf2:sha256",
        salt_length=16
    )


# ---------------------------------------------------------
# USERS (ADMIN FIRST — PRECEDENCE GUARANTEED)
# ---------------------------------------------------------
# In production, ALWAYS provide *_PASSWORD_HASH env vars.
# Fallback hashes exist ONLY to prevent lockout in dev.
# ---------------------------------------------------------

USERS = {
    # ================= ADMIN =================
    "Eugene": {
        "password": (
            os.getenv("ADMIN_PASSWORD_HASH")
            or _hash("Eugene3980")   # ⚠️ DEV ONLY fallback
        ),
        "email": os.getenv("ADMIN_EMAIL", "eugenekirubi@gmail.com"),
        "role": "admin",
    },

    # ================= CUSTOMER =================
    "Krabz": {
        "password": (
            os.getenv("CUSTOMER_PASSWORD_HASH")
            or _hash("Kraabzpass123")  # ⚠️ DEV ONLY fallback
        ),
        "email": os.getenv("CUSTOMER_EMAIL", "eugenecrabs321@gmail.com"),
        "role": "customer",
    },

    # ================= MERCHANT =================
    "Merchant1": {
        "password": (
            os.getenv("MERCHANT_PASSWORD_HASH")
            or _hash("merchantpass123")  # ⚠️ DEV ONLY fallback
        ),
        "email": os.getenv("MERCHANT_EMAIL", "merchant@gmail.com"),
        "role": "merchant",
    },
}


# ---------------------------------------------------------
# Centralized password verification
# ---------------------------------------------------------
def verify_password(username: str, raw_password: str) -> bool:
    """
    Verify user password securely.
    """
    user = USERS.get(username)
    if not user:
        return False

    try:
        return check_password_hash(user["password"], raw_password)
    except Exception:
        return False



# =========================================================
# PROFILES + KYC (UNCHANGED LOGIC)
# =========================================================

PROFILES = {}

KYC_CASES = []
KYC_NEXT_ID = 1


def _prefix_for_username(username: str) -> str:
    role = (USERS.get(username) or {}).get("role", "customer").lower()
    if role == "admin":
        return "CM-ADMIN"
    if role == "merchant":
        return "CM-MERCHANT"
    return "CM-CUST"


def _ensure_profile(username: str) -> dict:
    """
    Create (or migrate) a profile with a stable, role-prefixed user_id.
    """
    p = PROFILES.get(username)
    if p:
        uid = p.get("user_id", "")
        if uid.startswith("CM-KW-"):
            parts = uid.split("-")
            if len(parts) == 4:
                _, country_old, year_old, token = parts
                prefix = _prefix_for_username(username)
                p["user_id"] = f"{prefix}-{country_old}-{year_old}-{token}"
                _touch()
        p.setdefault("id_status", "pending")
        return p

    prefix = _prefix_for_username(username)
    country = "KW"
    year = datetime.utcnow().year
    seed = f"{username}:{secrets.token_urlsafe(8)}:{datetime.utcnow().isoformat()}"
    token = hashlib.sha256(seed.encode()).hexdigest()[:10].upper()
    user_id = f"{prefix}-{country}-{year}-{token}"

    PROFILES[username] = {
        "username": username,
        "user_id": user_id,
        "full_name": "",
        "phone": "",
        "dob": "",
        "nationality": "",
        "country": country,
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "locked": False,
        "id_status": "pending",
        "notify_whatsapp": False
    }

    _touch()
    return PROFILES[username]


def _get_profile_by_user_id(uid: str):
    for p in PROFILES.values():
        if p.get("user_id") == uid:
            return p
    return None


@app.context_processor
def _inject_helpers():
    return {"csrf_token": lambda: session.get("csrf_token", "")}
# -----------------------------------------------------------------------------
#                EXCHANGE RATE (DB-FIRST, DISK FALLBACK)
# -----------------------------------------------------------------------------

RATE_FILE = os.path.join(BASE_DIR, "exchange_rate.json")
EXCHANGE_RATE = None  # cached in memory


def _load_rate_from_disk(default: float = 420.0) -> float:
    try:
        if os.path.exists(RATE_FILE):
            with open(RATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                val = float(data.get("rate", 0))
                if val > 0:
                    return val
    except Exception:
        pass
    return float(default)


def _save_rate_to_disk(val: float) -> None:
    payload = {
        "rate": float(val),
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }
    try:
        with open(RATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        pass


def _load_rate_from_db(default: float = 420.0) -> float:
    """
    Primary source of truth.
    """
    try:
        with db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS exchange_rate (
                    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
                    rate NUMERIC NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """)
            cur.execute("SELECT rate FROM exchange_rate WHERE id=TRUE")
            row = cur.fetchone()
            if row and float(row["rate"]) > 0:
                return float(row["rate"])
    except Exception:
        pass
    return default


def _save_rate_to_db(val: float) -> None:
    try:
        with db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO exchange_rate (id, rate, updated_at)
                VALUES (TRUE, %s, NOW())
                ON CONFLICT (id)
                DO UPDATE SET rate=EXCLUDED.rate, updated_at=NOW()
            """, (float(val),))
    except Exception:
        pass


def get_exchange_rate() -> float:
    global EXCHANGE_RATE

    if isinstance(EXCHANGE_RATE, (int, float)) and EXCHANGE_RATE > 0:
        return float(EXCHANGE_RATE)

    # DB first
    rate = _load_rate_from_db()
    if rate > 0:
        EXCHANGE_RATE = rate
        return rate

    # Disk fallback
    EXCHANGE_RATE = _load_rate_from_disk()
    return float(EXCHANGE_RATE)


def set_exchange_rate(val: float) -> None:
    """
    Central setter — updates DB, disk, memory.
    """
    global EXCHANGE_RATE
    try:
        val = float(val)
        if val <= 0:
            return
        EXCHANGE_RATE = val
        _save_rate_to_db(val)
        _save_rate_to_disk(val)
        _touch()
    except Exception:
        pass


# Load at import (safe)
EXCHANGE_RATE = get_exchange_rate()

# -----------------------------------------------------------------------------
# CALCULATOR LOGIC (UNCHANGED)
# -----------------------------------------------------------------------------

COMPETITOR_FEE_KWD = 1.25
DISTRIBUTOR_FEE_KWD = 0.4
BASE_PROFIT_KWD = 0.1
MAX_MARGIN_KWD = 1.1

_KWD_ANCHORS = [
    (1.0,  (1.4 + 7.0) / 2.0),
    (2.0,  1.3),
    (3.0,  (1.5 + 2.3) / 2.0),
    (5.0,  5.0),
    (7.0,  5.0),
    (8.0,  5.5),
    (10.0, 6.0),
    (15.0, 3.5),
    (20.0, 2.8),
    (30.0, 2.5),
    (50.0, 2.25),
    (100.0, 2.5),
    (500.0, 0.40),
]


def _effective_fee_pct_kwd(amount_kwd: float) -> float:
    a = float(amount_kwd or 0.0)
    if a <= 0:
        return 0.0
    if 50.0 < a <= 600.0:
        return (1.1 / a) * 100.0
    if a <= _KWD_ANCHORS[0][0]:
        return _KWD_ANCHORS[0][1]
    if a >= _KWD_ANCHORS[-1][0]:
        return _KWD_ANCHORS[-1][1]
    for i in range(1, len(_KWD_ANCHORS)):
        ax, ay = _KWD_ANCHORS[i - 1]
        bx, by = _KWD_ANCHORS[i]
        if ax <= a <= bx:
            t = 0.0 if bx == ax else (a - ax) / (bx - ax)
            return ay + t * (by - ay)
    return _KWD_ANCHORS[-1][1]


def calculate_fees(amount_kwd, rate: float = None):
    r = float(rate or get_exchange_rate())
    amount_kwd = float(amount_kwd or 0.0)
    amount_kes = amount_kwd * r
    pct = _effective_fee_pct_kwd(amount_kwd) / 100.0
    fee_kwd = amount_kwd * pct
    total_fee_kwd = fee_kwd
    total_cost_kwd = amount_kwd + total_fee_kwd
    competitor_total_kwd = amount_kwd + COMPETITOR_FEE_KWD
    competitive = "Yes" if total_fee_kwd < COMPETITOR_FEE_KWD else "No"
    profit_kwd = total_fee_kwd

    return {
        "amount_kwd": amount_kwd,
        "amount_kes": amount_kes,
        "charges_kwd": total_fee_kwd,
        "transaction_charge_kwd": fee_kwd,
        "transaction_charge_kes": fee_kwd * r,
        "total_fee_kwd": total_fee_kwd,
        "total_cost_kwd": total_cost_kwd,
        "competitor_total_kwd": competitor_total_kwd,
        "competitive": competitive,
        "profit_kwd": profit_kwd
    }

# -----------------------------------------------------------------------------
# TRANSACTIONS & AUDIT (MEMORY + DB READY)
# -----------------------------------------------------------------------------

TRANSACTIONS = []   # retained for compatibility
AUDIT_LOG = []      # retained for compatibility
NEXT_ID = 1         # retained

# -----------------------------------------------------------------------------
# PHONE NORMALIZATION (UNCHANGED)
# -----------------------------------------------------------------------------

PHONE_RE = re.compile(r"^254\d{9}$")

def _normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 12 and digits.startswith("254"):
        if digits[3] in ("7", "1"):
            return digits
        return ""
    if len(digits) == 10 and digits.startswith("0") and digits[1] in ("7", "1"):
        return "254" + digits[1:]
    return ""
# ---------- Helpers ----------
def current_user():
    return session.get("user")

def is_logged_in():
    return bool(current_user())

def require_login():
    return is_logged_in()

def is_admin():
    u = current_user()
    return u and u.get("role") == "admin"

def require_admin():
    return is_admin()

def is_merchant():
    u = current_user()
    return u and u.get("role") == "merchant"

def redirect_by_role(role: str):
    if role == "admin":
        return redirect(url_for("admin_page"))
    elif role == "merchant":
        return redirect(url_for("merchant_page"))
    else:
        return redirect(url_for("customer_page"))


# ------------------------------------------------------------
# AMOUNT / METHOD VALIDATION (UNCHANGED)
# ------------------------------------------------------------
def _normalize_amount_kwd(amount: float, currency: str) -> float:
    rate = get_exchange_rate()
    return (float(amount) / rate) if currency == "KES" else float(amount)

def _validate_method_and_fields(method: str, phone: str, bank_account: str, paybill: str):
    raw = (method or "").strip()
    if raw.lower() == "wamd":
        return True, None
    method = raw.capitalize()
    if method not in ("Mpesa", "Bank"):
        return False, "Method must be Mpesa, Bank, or WAMD."
    if method == "Mpesa":
        norm = _normalize_phone((phone or "").strip())
        if not norm:
            return False, "Enter phone number in format 2547XXXXXXXX or 2541XXXXXXXX."
    else:
        if not (bank_account or "").strip() or not (paybill or "").strip():
            return False, "Bank Account and Paybill Number are required for Bank method."
    return True, None


# ------------------------------------------------------------
# STATUS NORMALIZATION (UNCHANGED)
# ------------------------------------------------------------
_ALLOWED_STATUSES = {"successful", "failed", "in progress"}

def _clean_status(raw) -> str:
    if not raw:
        return "in progress"
    s = str(raw).strip().lower()
    if s in _ALLOWED_STATUSES:
        return s
    if s in {"success"}:
        return "successful"
    if s in {"error", "failure"}:
        return "failed"
    return "in progress"


# ------------------------------------------------------------
# CSRF (UNCHANGED)
# ------------------------------------------------------------
def _issue_csrf():
    token = secrets.token_urlsafe(24)
    session["csrf_token"] = token
    return token

def _check_csrf_header():
    expected = session.get("csrf_token") or ""
    provided = request.headers.get("X-CSRF-Token", "") or request.form.get("csrf", "")
    return expected and provided and secrets.compare_digest(expected, provided)


# ------------------------------------------------------------
# AUDIT LOG — MEMORY + DB (DB IS SOURCE OF TRUTH)
# ------------------------------------------------------------
def _init_vouchers_table():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vouchers (
                code TEXT PRIMARY KEY,
                merchant_id TEXT,
                amount_kes DOUBLE PRECISION,
                status TEXT,
                expires_at TIMESTAMP,
                created_at TIMESTAMP,
                payload JSONB
            )
        """)

def _init_mpesa_callbacks_table():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mpesa_callbacks (
                receipt TEXT PRIMARY KEY,
                amount DOUBLE PRECISION,
                msisdn TEXT,
                checkout_id TEXT,
                timestamp TIMESTAMP,
                payload JSONB
            )
        """)
_init_vouchers_table()
_init_mpesa_callbacks_table()

def _init_audit_table():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id SERIAL PRIMARY KEY,
                tx_id INTEGER,
                actor TEXT NOT NULL,
                old_status TEXT,
                new_status TEXT,
                reason TEXT,
                meta JSONB,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)

_init_audit_table()


def _audit(tx_id: int, actor: str, old_status: str, new_status: str, reason: str, **extra):
    """
    Original API preserved.
    Writes to DB + keeps in-memory list for compatibility.
    """
    rec = {
        "id": len(AUDIT_LOG) + 1,
        "tx_id": tx_id,
        "actor": actor,
        "old_status": old_status,
        "new_status": new_status,
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }
    if extra:
        rec.update(extra)

    # Memory (unchanged behavior)
    AUDIT_LOG.append(rec)

    # DB (authoritative)
    try:
        with db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO audit_log
                    (tx_id, actor, old_status, new_status, reason, meta)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                tx_id,
                actor,
                old_status,
                new_status,
                reason,
                json.dumps(extra) if extra else None
            ))
    except Exception as e:
        # DB failure should never break flow
        print("[audit] db write failed:", e)

    _touch()
# ===================== Notifications (SMS / WhatsApp) =====================

SMS_PROVIDER = os.getenv("SMS_PROVIDER", "twilio").lower()

_TWILIO_READY = False
try:
    from twilio.rest import Client as _TwilioClient  # type: ignore
    TWILIO_SID = os.getenv("TWILIO_SID", "")
    TWILIO_TOKEN = os.getenv("TWILIO_TOKEN", "")
    TWILIO_SMS_FROM = os.getenv("TWILIO_SMS_FROM", "")
    TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "")
    if TWILIO_SID and TWILIO_TOKEN and (TWILIO_SMS_FROM or TWILIO_WHATSAPP_FROM):
        _tw = _TwilioClient(TWILIO_SID, TWILIO_TOKEN)
        _TWILIO_READY = True
except Exception:
    _TWILIO_READY = False


_AT_READY = False
if not _TWILIO_READY and SMS_PROVIDER == "africastalking":
    try:
        import africastalking  # type: ignore
        africastalking.initialize(
            os.getenv("AT_USERNAME", ""),
            os.getenv("AT_APIKEY", "")
        )
        _AT_SMS = africastalking.SMS
        _AT_READY = True
    except Exception:
        _AT_READY = False


# ------------------------------------------------------------
# DB: NOTIFICATION LOG (NON-BLOCKING, AUDIT-ONLY)
# ------------------------------------------------------------

def _init_notification_log():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS notification_log (
                id SERIAL PRIMARY KEY,
                channel TEXT NOT NULL,        -- sms | whatsapp
                provider TEXT NOT NULL,       -- twilio | africastalking
                msisdn TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL,         -- sent | failed | skipped
                error TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)

_init_notification_log()


def _log_notification(channel, provider, msisdn, message, status, error=None):
    try:
        with db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO notification_log
                    (channel, provider, msisdn, message, status, error)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                channel,
                provider,
                msisdn,
                message,
                status,
                error
            ))
    except Exception:
        # Never break business flow
        pass


# ------------------------------------------------------------
# SENDERS (API UNCHANGED)
# ------------------------------------------------------------

def send_sms(msisdn: str, text: str):
    try:
        if not msisdn or not text:
            _log_notification("sms", SMS_PROVIDER, msisdn, text, "skipped", "empty")
            return

        if SMS_PROVIDER == "twilio" and _TWILIO_READY:
            to = msisdn if msisdn.startswith("+") else f"+{msisdn}"
            _tw.messages.create(
                to=to,
                from_=TWILIO_SMS_FROM,
                body=text
            )
            _log_notification("sms", "twilio", to, text, "sent")

        elif SMS_PROVIDER == "africastalking" and _AT_READY:
            to = [msisdn if msisdn.startswith("+") else f"+{msisdn}"]
            _AT_SMS.send(text, to)
            _log_notification("sms", "africastalking", to[0], text, "sent")

        else:
            _log_notification("sms", SMS_PROVIDER, msisdn, text, "skipped", "provider not ready")

    except Exception as e:
        _log_notification("sms", SMS_PROVIDER, msisdn, text, "failed", str(e))


def send_whatsapp(msisdn: str, text: str):
    try:
        if not msisdn or not text:
            _log_notification("whatsapp", SMS_PROVIDER, msisdn, text, "skipped", "empty")
            return

        if SMS_PROVIDER == "twilio" and _TWILIO_READY and TWILIO_WHATSAPP_FROM:
            to = msisdn if msisdn.startswith("+") else f"+{msisdn}"
            _tw.messages.create(
                to="whatsapp:" + to.lstrip("+"),
                from_=TWILIO_WHATSAPP_FROM,
                body=text
            )
            _log_notification("whatsapp", "twilio", to, text, "sent")
        else:
            _log_notification("whatsapp", SMS_PROVIDER, msisdn, text, "skipped", "provider not ready")

    except Exception as e:
        _log_notification("whatsapp", SMS_PROVIDER, msisdn, text, "failed", str(e))


# ------------------------------------------------------------
# MESSAGE COMPOSERS (UNCHANGED)
# ------------------------------------------------------------

def _compose_voucher_receipt_text(v, *, paid_amount: float, fee_kes: float, receipt: str) -> str:
    link = url_for(
        "voucher_receipt",
        code=v["code"],
        token=v["receipt_token"],
        _external=True
    )
    return (
        "ChequeMatez: Payment received.\n"
        f"Amount: {paid_amount:.2f} KES\n"
        f"Fee: {fee_kes:.2f} KES\n"
        f"Merchant: {v.get('merchant_id','')}\n"
        f"Ref: {receipt}\n"
        f"Date: {v.get('used_at') or v.get('updated_at')}\n"
        f"Receipt: {link}"
    )


def _compose_payout_text(v, *, ok: bool, net: float, ref: str) -> str:
    status = "SENT" if ok else "FAILED"
    return (
        f"ChequeMatez: Payout {status}.\n"
        f"Amount: {net:.2f} KES\n"
        f"To: {v.get('payout',{}).get('method','')} {v.get('payout',{}).get('target','')}\n"
        f"Ref: {ref}"
    )
# ------------------------------------------------------------
# PAYMENT / LEDGER STUBS (REQUIRED)
# ------------------------------------------------------------

def service_fee_kes_voucher(amount: float) -> float:
    return round(float(amount or 0.0) * 0.02, 2)

def stk_push(**kwargs):
    return {"CheckoutRequestID": uuid.uuid4().hex}

def b2c_send(*a, **k):
    return {"ConversationID": uuid.uuid4().hex}

def b2b_till(*a, **k):
    return {"ConversationID": uuid.uuid4().hex}

def bank_transfer(*a, **k):
    return {"ref": uuid.uuid4().hex}

def v_log_tx(**kwargs):
    """
    Mirrors voucher tx into main transaction system.
    Required for reconciliation & reports.
    """
    entry = dict(kwargs)
    entry["timestamp"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    V_TRANSACTIONS.append(entry)
    _persist_transaction(entry) if "amount_kes" in entry else None
    _touch()

def _pdf_view_page(title, pdf_url):
    return redirect(pdf_url)

# ======================= Transaction receipt helpers =======================

def _make_tx_receipt_ids(tx_id: int):
    """
    Original behavior preserved.
    DB uniqueness guaranteed via receipt_id uniqueness later.
    """
    ts = datetime.utcnow().strftime("%Y%m%d")
    rid = f"RCPT-{ts}-{tx_id:06d}"
    token = secrets.token_hex(16)
    return rid, token


def _verify_tx_token(entry: dict, token: str) -> bool:
    """
    Original logic preserved.
    """
    return bool(entry) and token and secrets.compare_digest(
        entry.get("receipt_token", ""),
        token
    )


def _client_totals_for(
    amount_kwd: float,
    total_fee_kwd: float,
    *,
    pass_service_charge: bool,
    rate: float = None
):
    r = float(rate or get_exchange_rate())
    amount_kwd = float(amount_kwd or 0.0)
    fee_kwd = float(total_fee_kwd or 0.0)

    if pass_service_charge:
        client_pays_kwd = amount_kwd + fee_kwd
        client_receives_kwd = amount_kwd
    else:
        client_pays_kwd = amount_kwd
        client_receives_kwd = max(amount_kwd - fee_kwd, 0.0)

    return {
        "client_pays_kwd": client_pays_kwd,
        "client_pays_kes": client_pays_kwd * r,
        "client_receives_kwd": client_receives_kwd,
        "client_receives_kes": client_receives_kwd * r,
    }


# ------------------------------------------------------------
# VIEW FLATTENING for WAMD + api/receipt (UNCHANGED)
# ------------------------------------------------------------

def _flatten_tx_for_view(tx: dict) -> dict:
    """
    Return a shallow copy with WAMD meta lifted to top-level keys.
    EXACT behavior preserved.
    """
    out = dict(tx)
    meta = dict(tx.get("meta") or {})

    for k in (
        "wamd_reference",
        "wamd_pay_to",
        "wamd_beneficiary",
        "wamd_recipient_name",
        "wamd_recipient_phone",
        "wamd_note",
    ):
        if k in meta and k not in out:
            out[k] = meta.get(k)

    # convenience fallbacks for receipt JS
    if "recipient_phone" not in out and out.get("wamd_recipient_phone"):
        out["recipient_phone"] = out["wamd_recipient_phone"]
    if "recipient_name" not in out and out.get("wamd_recipient_name"):
        out["recipient_name"] = out["wamd_recipient_name"]

    return out


# ------------------------------------------------------------
# DB SAFETY: RECEIPT UNIQUENESS (NON-BREAKING)
# ------------------------------------------------------------

def _init_receipt_constraints():
    """
    Ensures receipt_id + receipt_token are unique at DB level.
    Does NOT change runtime behavior.
    """
    try:
        with db_cursor(commit=True) as cur:
            cur.execute("""
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS receipt_id TEXT
            """)
            cur.execute("""
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS receipt_token TEXT
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_receipt_id
                ON transactions(receipt_id)
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_receipt_token
                ON transactions(receipt_token)
            """)
    except Exception:
        # Safe to ignore if table not yet created
        pass

_init_receipt_constraints()
# ---------- Routes ----------
@app.route("/api/transactions")
def api_transactions():
    if not require_login():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    u = current_user()
    role = u.get("role")
    username = u.get("username")

    limit = int(request.args.get("limit", "200") or 200)

    items = _fetch_transactions_for_user(
        username=username,
        role=role,
        limit=limit
    )

    return jsonify({
        "ok": True,
        "count": len(items),
        "items": items
    })

@app.route("/api/my-transactions")
def api_my_transactions():
    return api_transactions()


@app.route("/api/admin/transactions")
def api_admin_transactions():
    if not require_admin():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    return api_transactions()

@app.route("/")
def public_calculator():
    open_login = request.args.get("login") == "1"
    return render_template("chapaa.html", open_login=open_login)


@app.route("/kyc", endpoint="kyc_bundle")
def show_kyc_bundle():
    return render_template("kyc_bundle.html", view="kyc_start")


# ------------------------------------------------------------
# DB: PROFILES TABLE (SAFE, NON-DESTRUCTIVE)
# ------------------------------------------------------------
def _fetch_transactions_for_user(username=None, role=None, limit=500):
    """
    DB-first transaction fetch.
    Falls back to in-memory TRANSACTIONS if DB fails.
    """
    try:
        with db_cursor() as cur:
            if role == "admin":
                cur.execute("""
                    SELECT * FROM transactions
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (limit,))
            else:
                cur.execute("""
                    SELECT * FROM transactions
                    WHERE username=%s
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (username, limit))
            rows = cur.fetchall() or []
            for r in rows:
                if isinstance(r.get("meta"), str):
                    try:
                        r["meta"] = json.loads(r["meta"])
                    except Exception:
                        r["meta"] = {}
            return rows
    except Exception:
        # Fallback: in-memory
        data = TRANSACTIONS[:]
        if role != "admin" and username:
            data = [t for t in data if t.get("username") == username]
        data.sort(key=lambda x: x.get("timestamp",""), reverse=True)
        return data[:limit]

def _init_profiles_table():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                username TEXT PRIMARY KEY,
                user_id TEXT UNIQUE NOT NULL,
                full_name TEXT,
                phone TEXT,
                dob TEXT,
                nationality TEXT,
                country TEXT,
                locked BOOLEAN DEFAULT FALSE,
                id_status TEXT DEFAULT 'pending',
                notify_whatsapp BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)

_init_profiles_table()


def _sync_profile_to_db(p: dict):
    """
    Writes profile to DB without changing in-memory behavior.
    """
    try:
        with db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO profiles
                    (username, user_id, full_name, phone, dob,
                     nationality, country, locked, id_status,
                     notify_whatsapp, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (username)
                DO UPDATE SET
                    user_id=EXCLUDED.user_id,
                    full_name=EXCLUDED.full_name,
                    phone=EXCLUDED.phone,
                    dob=EXCLUDED.dob,
                    nationality=EXCLUDED.nationality,
                    country=EXCLUDED.country,
                    locked=EXCLUDED.locked,
                    id_status=EXCLUDED.id_status,
                    notify_whatsapp=EXCLUDED.notify_whatsapp
            """, (
                p["username"],
                p["user_id"],
                p.get("full_name"),
                p.get("phone"),
                p.get("dob"),
                p.get("nationality"),
                p.get("country"),
                bool(p.get("locked", False)),
                p.get("id_status", "pending"),
                bool(p.get("notify_whatsapp", False)),
                datetime.utcnow()
            ))
    except Exception as e:
        print("[profiles] db sync failed:", e)


# ------------------------------------------------------------
# USER ID PAGES (UNCHANGED ROUTES)
# ------------------------------------------------------------

@app.route("/u/<username>")
def user_id_page(username):
    p = _ensure_profile(username)
    _sync_profile_to_db(p)

    ctx = dict(p)
    ctx["issued_id"] = p["user_id"]
    ctx["id_status"] = p.get("id_status", "pending")
    ctx["full_name"] = p.get("full_name") or username
    ctx["qr_token"] = secrets.token_urlsafe(8)

    status_text = "APPROVED" if ctx["id_status"] == "approved" else "PENDING"
    can_transact = (ctx["id_status"] == "approved")

    return render_template(
        "user_id.html",
        user=ctx,
        can_transact=can_transact,
        exchange_rate=get_exchange_rate(),
        status=status_text
    )


@app.route("/scan/<user_id>", endpoint="scan_user_landing")
def scan_user_landing(user_id):
    _ = request.args.get("t", "")
    prof = _get_profile_by_user_id(user_id)
    if not prof:
        return "User not found", 404

    return render_template(
        "scan.html",
        preset_user_id=user_id,
        preset_username=prof["username"],
        preset_full_name=prof.get("full_name") or prof["username"]
    )


@app.route("/u/<user_id>/qr.png", endpoint="user_id_qr_png")
def user_id_qr_png(user_id):
    p = _get_profile_by_user_id(user_id)
    if not p:
        abort(404)

    url = url_for("user_id_page", username=p["username"], _external=True)
    img = qrcode.make(url)
    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)

    resp = make_response(buf.read())
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/my-id")
def my_id():
    if not require_login():
        return redirect("/?login=1")
    u = current_user()
    return redirect(url_for("user_id_page", username=u["username"]))


@app.route("/api/my-profile")
def my_profile():
    if not require_login():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    u = current_user()
    p = _ensure_profile(u["username"])
    _sync_profile_to_db(p)
    return jsonify({"ok": True, "profile": p})


@app.route("/api/user/<username>")
def user_public_json(username):
    p = PROFILES.get(username)
    if not p:
        return jsonify({"ok": False, "error": "Not found"}), 404

    return jsonify({"ok": True, "profile": {
        "username": p["username"],
        "user_id": p["user_id"],
        "full_name": p.get("full_name", ""),
        "phone": p.get("phone", ""),
        "dob": p.get("dob", ""),
        "nationality": p.get("nationality", ""),
        "country": p.get("country", "KW"),
        "locked": bool(p.get("locked", False)),
        "id_status": p.get("id_status", "pending"),
        "notify_whatsapp": bool(p.get("notify_whatsapp", False))
    }})


@app.route("/api/resolve-userid", methods=["POST"])
def resolve_userid():
    uid = (request.form.get("user_id") or "").strip()
    prof = _get_profile_by_user_id(uid)
    if prof:
        return jsonify({"ok": True, "profile": {
            "username": prof["username"],
            "user_id": prof["user_id"],
            "full_name": prof.get("full_name", ""),
            "phone": prof.get("phone", ""),
            "locked": bool(prof.get("locked", False)),
            "id_status": prof.get("id_status", "pending"),
            "notify_whatsapp": bool(prof.get("notify_whatsapp", False))
        }})
    return jsonify({"ok": False, "error": "User ID not found"}), 404


@app.route("/scan")
def scan_page():
    return render_template("scan.html")
# =========================== K Y C   F L O W ===========================

def _save_upload(file_storage, suffix):
    if not file_storage:
        return ""
    ext = os.path.splitext(file_storage.filename or "")[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"):
        ext = ".jpg"
    fname = f"{uuid.uuid4().hex}{suffix}{ext}"
    path = os.path.join(KYC_UPLOAD_DIR, fname)
    file_storage.save(path)
    return path


# ------------------------------------------------------------
# DB: KYC TABLE (SAFE MIRROR)
# ------------------------------------------------------------

def _init_kyc_table():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kyc_cases (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                phone_e164 TEXT,
                id_type TEXT,
                id_number TEXT,
                selfie_path TEXT,
                id_front_path TEXT,
                id_back_path TEXT,
                status TEXT,
                reason TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)

_init_kyc_table()


def _sync_kyc_to_db(case: dict):
    try:
        with db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO kyc_cases
                    (id, username, phone_e164, id_type, id_number,
                     selfie_path, id_front_path, id_back_path,
                     status, reason, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id)
                DO UPDATE SET
                    status=EXCLUDED.status,
                    reason=EXCLUDED.reason,
                    updated_at=EXCLUDED.updated_at
            """, (
                case["id"],
                case["username"],
                case["phone_e164"],
                case["id_type"],
                case["id_number"],
                case["selfie_path"],
                case["id_front_path"],
                case["id_back_path"],
                case["status"],
                case.get("reason", ""),
                case["created_at"],
                case["updated_at"],
            ))
    except Exception as e:
        print("[kyc] db sync failed:", e)


@app.route("/kyc/start", methods=["GET", "POST"], endpoint="kyc_start")
def kyc_start():
    if not require_login():
        return redirect("/?login=1")

    u = current_user()
    p = _ensure_profile(u["username"])

    if request.method == "GET":
        if p.get("id_status") == "approved":
            return render_template(
                "kyc_bundle.html",
                view="user_id",
                user={
                    "issued_id": p["user_id"],
                    "full_name": p.get("full_name") or u["username"],
                    "id_status": "approved",
                    "qr_token": secrets.token_urlsafe(8)
                },
                can_transact=True,
                exchange_rate=get_exchange_rate()
            )
        return render_template("kyc_bundle.html", view="kyc_start")

    global KYC_NEXT_ID

    id_type = (request.form.get("id_type") or "").strip()
    id_number = (request.form.get("id_number") or "").strip()
    selfie = request.files.get("selfie")
    id_front = request.files.get("id_front")
    id_back = request.files.get("id_back")

    if not id_type or not id_number or not selfie or not id_front:
        return "Missing required fields", 400

    phone_e164 = _normalize_phone(p.get("phone", "")) or p.get("phone", "")

    selfie_path = _save_upload(selfie, "_selfie")
    id_front_path = _save_upload(id_front, "_front")
    id_back_path = _save_upload(id_back, "_back") if id_back else ""

    case = {
        "id": KYC_NEXT_ID,
        "username": u["username"],
        "phone_e164": phone_e164,
        "id_type": id_type,
        "id_number": id_number,
        "selfie_path": selfie_path,
        "id_front_path": id_front_path,
        "id_back_path": id_back_path,
        "status": "pending",
        "reason": "",
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    KYC_CASES.append(case)
    KYC_NEXT_ID += 1

    p["id_status"] = "pending"
    p["locked"] = True

    _sync_kyc_to_db(case)
    _touch()
    save_state(force=True)

    return render_template(
        "kyc_bundle.html",
        view="user_id",
        user={
            "issued_id": p["user_id"],
            "full_name": p.get("full_name") or u["username"],
            "id_status": "pending",
            "qr_token": secrets.token_urlsafe(8)
        },
        can_transact=False,
        exchange_rate=get_exchange_rate()
    )


@app.route("/admin/kyc")
def admin_kyc_queue():
    if not (require_admin() or is_merchant()):
        return redirect("/?login=1")
    rows = [c for c in KYC_CASES if c.get("status") == "pending"]
    return render_template("kyc_bundle.html", view="kyc_queue", rows=rows)


@app.route("/admin/kyc/file/<int:case_id>", endpoint="admin_kyc_file")
def admin_kyc_file(case_id):
    if not (require_admin() or is_merchant()):
        return "Unauthorized", 401
    c = next((x for x in KYC_CASES if x["id"] == case_id), None)
    if not c:
        return "Not found", 404

    ftype = (request.args.get("type") or "").lower()
    path = ""
    if ftype == "selfie":
        path = c.get("selfie_path", "")
    elif ftype == "id_front":
        path = c.get("id_front_path", "")
    elif ftype == "id_back":
        path = c.get("id_back_path", "")

    if not path or not os.path.exists(path):
        return "File not found", 404
    return send_file(path)


@app.route("/admin/kyc/<int:case_id>/approve", methods=["POST"], endpoint="admin_kyc_approve_case")
def admin_kyc_approve_case(case_id):
    if not (require_admin() or is_merchant()):
        return "Unauthorized", 401

    c = next((x for x in KYC_CASES if x["id"] == case_id), None)
    if not c:
        return "Not found", 404

    c["status"] = "approved"
    c["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    p = _ensure_profile(c["username"])
    p["id_status"] = "approved"

    _sync_kyc_to_db(c)
    _touch()
    save_state(force=True)

    return redirect(url_for("admin_kyc_queue"))


@app.route("/admin/kyc/<int:case_id>/reject", methods=["POST"], endpoint="admin_kyc_reject_case")
def admin_kyc_reject_case(case_id):
    if not (require_admin() or is_merchant()):
        return "Unauthorized", 401

    c = next((x for x in KYC_CASES if x["id"] == case_id), None)
    if not c:
        return "Not found", 404

    reason = (request.form.get("reason") or "").strip()
    c["status"] = "declined"
    c["reason"] = reason
    c["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    p = _ensure_profile(c["username"])
    p["id_status"] = "declined"

    _sync_kyc_to_db(c)
    _touch()
    save_state(force=True)

    return redirect(url_for("admin_kyc_queue"))


# ------------------------------------------------------------
# CALCULATOR + RATE API (HASH / DB SAFE)
# ------------------------------------------------------------

@app.route("/calculate", methods=["POST"])
def calculate():
    try:
        rate = get_exchange_rate()
        amount = request.form.get("amount")
        currency = request.form.get("currency", "KWD").upper()

        if amount is not None:
            amt = float(amount)
            amount_kwd = (amt / rate) if currency == "KES" else amt
        else:
            amount_kwd = float(request.form.get("amount_kwd", "0"))

        data = calculate_fees(amount_kwd, rate=rate)
        data_out = {
            **data,
            "exchange_rate": rate,
            "safaricom_fee_kes": 0.0,
            "margin_kes": 0.0,
            "extra_margin_kes": 0.0,
            "total_fee_kes": data["total_fee_kwd"] * rate,
            "total_cost_kes": data["total_cost_kwd"] * rate,
            "profit_kes": data["profit_kwd"] * rate
        }
        return jsonify(data_out)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/rate", methods=["GET", "POST"])
def api_rate():
    if request.method == "GET":
        return jsonify({"ok": True, "rate": get_exchange_rate()})

    if not require_admin():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    try:
        new_rate = (request.form.get("rate") or "").strip()
        if not new_rate:
            return jsonify({"ok": False, "error": "Rate is required"}), 400

        val = float(new_rate)
        if val <= 0:
            return jsonify({"ok": False, "error": "Rate must be > 0"}), 400

        set_exchange_rate(round(val, 6))
        return jsonify({"ok": True, "rate": get_exchange_rate()})

    except ValueError:
        return jsonify({"ok": False, "error": "Invalid number"}), 400


# ------------------------------------------------------------
# AUTH ROUTES (HASHED — MATCHES PART 4)
# ------------------------------------------------------------

@app.route("/login", methods=["POST"])
def login():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")

    if verify_password(username, password):
        role = USERS[username].get("role", "customer")
        session["user"] = {"username": username, "role": role}
        _issue_csrf()
        _ensure_profile(username)
        _touch()
        save_state(force=True)
        return redirect_by_role(role)

    return "Invalid credentials", 401


@app.route("/signup", methods=["POST"])
def signup():
    email = request.form.get("email", "").strip().lower()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    confirm = request.form.get("confirm_password", "")

    if not email or "@" not in email:
        return "Invalid email", 400
    if len(username) < 3:
        return "Username too short", 400
    if username in USERS:
        return "Username already exists", 400
    if password != confirm or len(password) < 8:
        return "Invalid password", 400

    role = "admin" if username == "admin" else "customer"

    USERS[username] = {
        "password": generate_password_hash(password),
        "email": email,
        "role": role
    }

    session["user"] = {"username": username, "role": role}
    _issue_csrf()
    _ensure_profile(username)
    _touch()
    save_state(force=True)

    return redirect_by_role(role)


@app.route("/admin")
def admin_page():
    if not require_admin():
        return redirect("/?login=1")
    return render_template("admin.html")


@app.route("/admin_ops")
def admin_ops():
    if not require_admin():
        return redirect("/?login=1")
    return render_template("admin_ops.html")
# ===================== ADMIN USERS + LEGAL PAGES =====================

def is_ops():
    u = current_user()
    return bool(u and u.get("role") in ("admin", "merchant"))

def require_ops():
    return is_ops()


# ------------------------------------------------------------
# DB: ADMIN USER MIRROR (SAFE, NON-DESTRUCTIVE)
# ------------------------------------------------------------

def _init_admin_user_meta():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS admin_user_meta (
                username TEXT PRIMARY KEY,
                limits_daily_kwd TEXT,
                limits_month_kwd TEXT,
                status TEXT,
                is_verified BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP
            )
        """)

_init_admin_user_meta()


def _sync_admin_user_meta(username: str, prof: dict):
    try:
        with db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO admin_user_meta
                    (username, limits_daily_kwd, limits_month_kwd,
                     status, is_verified, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (username)
                DO UPDATE SET
                    limits_daily_kwd=EXCLUDED.limits_daily_kwd,
                    limits_month_kwd=EXCLUDED.limits_month_kwd,
                    status=EXCLUDED.status,
                    is_verified=EXCLUDED.is_verified,
                    updated_at=EXCLUDED.updated_at
            """, (
                username,
                prof.get("limits_daily_kwd"),
                prof.get("limits_month_kwd"),
                prof.get("status", "active"),
                bool(prof.get("is_verified")),
                datetime.utcnow()
            ))
    except Exception as e:
        print("[admin-meta] db sync failed:", e)


# ------------------------------------------------------------
# IN-MEMORY USER ROWS (UNCHANGED LOGIC)
# ------------------------------------------------------------

def _user_rows_from_memory(q: str = "", status: str = "", kyc: str = "", limit: int = 500):
    q = (q or "").strip().lower()
    status = (status or "").strip().lower()
    kyc = (kyc or "").strip().lower()

    rows = []
    for username, uinfo in USERS.items():
        p = _ensure_profile(username)
        row = {
            "id": p["user_id"],
            "username": username,
            "full_name": p.get("full_name",""),
            "email": uinfo.get("email",""),
            "phone": p.get("phone",""),
            "is_verified": bool(p.get("is_verified") or p.get("verified")),
            "kyc_status": (p.get("id_status") or p.get("kyc_status") or "pending"),
            "status": (p.get("status") or "active"),
            "created_at": p.get("created_at",""),
            "updated_at": p.get("updated_at",""),
            "role": uinfo.get("role","customer"),
            "limits_daily_kwd": p.get("limits_daily_kwd", ""),
            "limits_month_kwd": p.get("limits_month_kwd", "")
        }
        rows.append(row)

    def matches(r):
        if q:
            blob = f"{r['username']} {r['full_name']} {r['email']} {r['phone']}".lower()
            if q not in blob:
                return False
        if status and (r.get("status","").lower() != status):
            return False
        if kyc and (r.get("kyc_status","").lower() != kyc):
            return False
        return True

    out = [r for r in rows if matches(r)]
    out.sort(key=lambda r: r.get("created_at",""), reverse=True)
    return out[:limit]


def _find_user_by_id(user_id: str):
    prof = _get_profile_by_user_id(user_id)
    if not prof:
        return None, None, None

    username = prof["username"]
    uinfo = USERS.get(username, {})
    row = {
        "id": prof["user_id"],
        "username": username,
        "full_name": prof.get("full_name",""),
        "email": uinfo.get("email",""),
        "phone": prof.get("phone",""),
        "is_verified": bool(prof.get("is_verified") or prof.get("verified")),
        "kyc_status": (prof.get("id_status") or prof.get("kyc_status") or "pending"),
        "status": (prof.get("status") or "active"),
        "created_at": prof.get("created_at",""),
        "updated_at": prof.get("updated_at",""),
        "limits_daily_kwd": prof.get("limits_daily_kwd",""),
        "limits_month_kwd": prof.get("limits_month_kwd",""),
    }
    return username, row, prof


# ------------------------------------------------------------
# ADMIN USERS UI + API (UNCHANGED ROUTES)
# ------------------------------------------------------------

@app.route("/admin/users")
def admin_users():
    if not require_ops():
        return redirect("/?login=1")

    q = request.args.get("q","")
    status = request.args.get("status","")
    kyc = request.args.get("kyc","")

    users = _user_rows_from_memory(q=q, status=status, kyc=kyc, limit=500)

    edit_user = None
    user_id = request.args.get("id","").strip()
    if user_id:
        _, row, _ = _find_user_by_id(user_id)
        edit_user = row

    return render_template(
        "admin_users.html",
        users=users,
        edit_user=edit_user,
        q=q, status=status, kyc=kyc,
        json_url=url_for("api_admin_users")
    )


@app.route("/api/admin/users")
def api_admin_users():
    if not require_ops():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    q = request.args.get("q","")
    status = request.args.get("status","")
    kyc = request.args.get("kyc","")
    limit = int(request.args.get("limit","500") or 500)
    rows = _user_rows_from_memory(q=q, status=status, kyc=kyc, limit=limit)
    return jsonify({"ok": True, "items": rows, "count": len(rows)})


@app.route("/admin/users/<user_id>", methods=["POST"])
def admin_users_update(user_id):
    if not require_ops():
        return "Unauthorized", 401

    username, row, prof = _find_user_by_id(user_id)
    if not prof:
        return "User not found", 404

    full_name = request.form.get("full_name","").strip()
    email     = request.form.get("email","").strip()
    phone     = request.form.get("phone","").strip()
    kyc_stat  = (request.form.get("kyc_status") or request.form.get("kyc_level") or "").strip().lower() or "pending"
    status    = (request.form.get("status") or "").strip().lower() or "active"
    verified  = bool(request.form.get("verified") or request.form.get("is_verified"))
    lim_day   = (request.form.get("limits_daily_kwd") or "").strip()
    lim_mon   = (request.form.get("limits_month_kwd") or "").strip()

    prof["full_name"] = full_name
    if phone:
        prof["phone"] = phone
    prof["id_status"] = kyc_stat
    prof["status"] = status
    prof["is_verified"] = verified
    if lim_day != "": prof["limits_daily_kwd"] = lim_day
    if lim_mon != "": prof["limits_month_kwd"] = lim_mon
    prof["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    if username in USERS and email:
        USERS[username]["email"] = email

    _sync_admin_user_meta(username, prof)

    _audit(
        tx_id=0,
        actor=current_user()["username"],
        old_status=row.get("status",""),
        new_status=status,
        reason="admin-user-update",
        target_user=username,
        fields=[
            "full_name","email","phone","kyc_status",
            "status","is_verified","limits_daily_kwd","limits_month_kwd"
        ]
    )

    _touch()
    save_state(force=True)
    return redirect(url_for("admin_users", id=prof["user_id"]))


# ------------------------------------------------------------
# LEGAL PAGES (UNCHANGED)
# ------------------------------------------------------------

@app.route("/legal")
def legal_index():
    return render_template("tos_privacy.html", view="all")

@app.route("/legal/terms")
def legal_terms():
    return render_template("tos_privacy.html", view="terms")

@app.route("/legal/privacy")
def legal_privacy():
    return render_template("tos_privacy.html", view="privacy")


# ------------------------------------------------------------
# WAMD SETTINGS (DB + MEMORY)
# ------------------------------------------------------------

def _init_wamd_settings():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wamd_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

_init_wamd_settings()


def _persist_wamd_settings():
    try:
        with db_cursor(commit=True) as cur:
            for k, v in ADMIN_SETTINGS.items():
                cur.execute("""
                    INSERT INTO wamd_settings (key, value)
                    VALUES (%s,%s)
                    ON CONFLICT (key)
                    DO UPDATE SET value=EXCLUDED.value
                """, (k, v))
    except Exception as e:
        print("[wamd] db persist failed:", e)


@app.route("/api/wamd/settings")
def public_wamd_settings():
    return jsonify({"ok": True, **_wamd_settings()})


@app.route("/api/admin/wamd/settings", methods=["POST"])
def admin_update_wamd_settings():
    if not require_admin():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    pay = (request.form.get("pay_to_phone") or "").strip()
    ben = (request.form.get("beneficiary") or "").strip()
    if not pay or not ben:
        return jsonify({"ok": False, "error": "Both pay_to_phone and beneficiary are required"}), 400

    ADMIN_SETTINGS["wamd_pay_to_phone"] = pay
    ADMIN_SETTINGS["wamd_beneficiary"]  = ben

    _persist_wamd_settings()
    _touch()
    save_state(force=True)

    return jsonify({"ok": True, **_wamd_settings()})


# ------------------------------------------------------------
# CUSTOMER / MERCHANT / LOGOUT (UNCHANGED)
# ------------------------------------------------------------

@app.route("/customer")
def customer_page():
    if not require_login():
        return redirect("/?login=1")
    if is_admin():
        return redirect(url_for("admin_page"))
    u = current_user()
    _ensure_profile(u["username"])
    return render_template("customer.html")


@app.route("/merchant")
def merchant_page():
    if not require_login():
        return redirect("/?login=1")
    u = current_user()
    if u.get("role") != "merchant":
        return redirect_by_role(u.get("role", "customer"))
    return render_template("merchant.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")
# --------- TRANSACTION BUILDERS (incl. WAMD storing mpesa recipient phone) ---------

# ------------------------------------------------------------
# DB: TRANSACTIONS TABLE (SAFE MIRROR)
# ------------------------------------------------------------

def _init_transactions_table():
    with db_cursor(commit=True) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                kind TEXT,
                method TEXT,
                currency TEXT,
                tx_type TEXT,
                amount_kwd DOUBLE PRECISION,
                amount_kes DOUBLE PRECISION,
                charges_kwd DOUBLE PRECISION,
                safaricom_fee_kwd DOUBLE PRECISION,
                safaricom_fee_kes DOUBLE PRECISION,
                profit_kwd DOUBLE PRECISION,
                phone TEXT,
                bank_account TEXT,
                paybill TEXT,
                status TEXT,
                meta JSONB,
                service_charge_mode TEXT,
                client_pays_total_kwd DOUBLE PRECISION,
                client_pays_total_kes DOUBLE PRECISION,
                client_receives_kwd DOUBLE PRECISION,
                client_receives_kes DOUBLE PRECISION,
                receipt_id TEXT,
                receipt_token TEXT,
                timestamp TIMESTAMP
            )
        """)

_init_transactions_table()


def _persist_transaction(entry: dict):
    """
    Writes transaction to DB without changing runtime behavior.
    """
    try:
        with db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO transactions (
                    id, username, kind, method, currency, tx_type,
                    amount_kwd, amount_kes, charges_kwd,
                    safaricom_fee_kwd, safaricom_fee_kes, profit_kwd,
                    phone, bank_account, paybill, status,
                    meta, service_charge_mode,
                    client_pays_total_kwd, client_pays_total_kes,
                    client_receives_kwd, client_receives_kes,
                    receipt_id, receipt_token, timestamp
                ) VALUES (
                    %(id)s, %(username)s, %(kind)s, %(method)s, %(currency)s, %(tx_type)s,
                    %(amount_kwd)s, %(amount_kes)s, %(charges_kwd)s,
                    %(safaricom_fee_kwd)s, %(safaricom_fee_kes)s, %(profit_kwd)s,
                    %(phone)s, %(bank_account)s, %(paybill)s, %(status)s,
                    %(meta)s, %(service_charge_mode)s,
                    %(client_pays_total_kwd)s, %(client_pays_total_kes)s,
                    %(client_receives_kwd)s, %(client_receives_kes)s,
                    %(receipt_id)s, %(receipt_token)s, %(timestamp)s
                )
            """, {
                **entry,
                "meta": json.dumps(entry.get("meta") or {})
            })
    except Exception as e:
        print("[tx] db persist failed:", e)


# ------------------------------------------------------------
# STANDARD TRANSACTION BUILDER (UNCHANGED API)
# ------------------------------------------------------------

def _create_transaction_for(username: str, form, *, override_phone=None):
    global NEXT_ID

    amount = float(form.get("amount", "0"))
    currency = form.get("currency", "KWD").upper()
    method = form.get("method", "Mpesa")
    tx_type = (form.get("tx_type", "") or "").capitalize()

    raw_phone = (form.get("phone", "") or "").strip()
    phone_norm = _normalize_phone(raw_phone)
    bank_account = (form.get("bank_account", "") or "").strip()
    paybill = (form.get("paybill", "") or "").strip()

    pass_service_charge = (form.get("pass_service_charge") or "1").strip().lower() in (
        "1", "true", "on", "yes"
    )

    if override_phone:
        phone_norm = _normalize_phone(override_phone) or override_phone

    status = _clean_status(form.get("status"))

    if tx_type not in ("Deposit", "Withdrawal"):
        return None, "Transaction Type must be Deposit or Withdrawal."

    ok, err = _validate_method_and_fields(
        method, phone_norm or raw_phone, bank_account, paybill
    )
    if not ok:
        return None, err

    amount_kwd = _normalize_amount_kwd(amount, currency)
    calc = calculate_fees(amount_kwd)

    totals = _client_totals_for(
        amount_kwd=calc["amount_kwd"],
        total_fee_kwd=calc["total_fee_kwd"],
        pass_service_charge=pass_service_charge,
    )

    rid, rtoken = _make_tx_receipt_ids(NEXT_ID)

    entry = {
        "id": NEXT_ID,
        "username": username,
        "kind": "transfer",
        "method": (method.upper() if method.strip().lower() == "wamd" else method.capitalize()),
        "currency": currency,
        "tx_type": tx_type,
        "amount_kwd": calc["amount_kwd"],
        "amount_kes": calc["amount_kes"],
        "charges_kwd": calc["total_fee_kwd"],
        "safaricom_fee_kwd": 0.0,
        "safaricom_fee_kes": 0.0,
        "profit_kwd": calc["profit_kwd"],
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "phone": phone_norm if method.capitalize() == "Mpesa" else "",
        "bank_account": bank_account if method.capitalize() == "Bank" else "",
        "paybill": paybill if method.capitalize() == "Bank" else "",
        "status": status,
        "meta": {},
        "service_charge_mode": "pass_through" if pass_service_charge else "absorbed",
        "client_pays_total_kwd": totals["client_pays_kwd"],
        "client_pays_total_kes": totals["client_pays_kes"],
        "client_receives_kwd": totals["client_receives_kwd"],
        "client_receives_kes": totals["client_receives_kes"],
        "receipt_id": rid,
        "receipt_token": rtoken,
    }

    TRANSACTIONS.append(entry)
    NEXT_ID += 1

    _persist_transaction(entry)
    _touch()
    save_state(force=True)

    if entry["status"] == "successful":
        _risk_eval_on_tx(entry)

    return entry, None


# ------------------------------------------------------------
# WAMD TRANSACTION BUILDER (UNCHANGED API)
# ------------------------------------------------------------

def _create_wamd_transaction_for(username: str, form):
    global NEXT_ID

    amount = float(form.get("amount", "0"))
    currency = (form.get("currency", "KWD") or "KWD").upper()
    tx_type = (form.get("tx_type", "") or "").capitalize()
    pass_service_charge = (form.get("pass_service_charge") or "1").strip().lower() in (
        "1", "true", "on", "yes"
    )
    status = _clean_status(form.get("status"))

    if tx_type not in ("Deposit", "Withdrawal"):
        return None, "Transaction Type must be Deposit or Withdrawal."
    if currency != "KWD":
        return None, "WAMD transactions must use currency=KWD."

    wcfg = _wamd_settings()
    w_ref = (form.get("wamd_reference") or "").strip()
    pay_to = (form.get("wamd_pay_to") or "").strip() or wcfg["pay_to_phone"]
    benef = (form.get("wamd_beneficiary") or "").strip() or wcfg["beneficiary"]
    r_name = (form.get("wamd_recipient_name") or "").strip()
    r_phone = (form.get("wamd_recipient_phone") or "").strip()
    note = (form.get("wamd_note") or "").strip()

    if not w_ref:
        return None, "Order / Reference is required."
    if not r_name or not r_phone:
        return None, "Recipient name and phone are required for WAMD."

    amount_kwd = float(amount or 0.0)
    calc = calculate_fees(amount_kwd)

    totals = _client_totals_for(
        amount_kwd=calc["amount_kwd"],
        total_fee_kwd=calc["total_fee_kwd"],
        pass_service_charge=pass_service_charge,
    )

    rid, rtoken = _make_tx_receipt_ids(NEXT_ID)

    entry = {
        "id": NEXT_ID,
        "username": username,
        "kind": "transfer",
        "method": "WAMD",
        "currency": "KWD",
        "tx_type": tx_type,
        "amount_kwd": calc["amount_kwd"],
        "amount_kes": calc["amount_kes"],
        "charges_kwd": calc["total_fee_kwd"],
        "safaricom_fee_kwd": 0.0,
        "safaricom_fee_kes": 0.0,
        "profit_kwd": calc["profit_kwd"],
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "phone": "",
        "bank_account": "",
        "paybill": "",
        "status": status,
        "meta": {
            "wamd_reference": w_ref,
            "wamd_pay_to": pay_to,
            "wamd_beneficiary": benef,
            "wamd_recipient_name": r_name,
            "wamd_recipient_phone": r_phone,
            "wamd_note": note,
        },
        "service_charge_mode": "pass_through" if pass_service_charge else "absorbed",
        "client_pays_total_kwd": totals["client_pays_kwd"],
        "client_pays_total_kes": totals["client_pays_kes"],
        "client_receives_kwd": totals["client_receives_kwd"],
        "client_receives_kes": totals["client_receives_kes"],
        "receipt_id": rid,
        "receipt_token": rtoken,
    }

    TRANSACTIONS.append(entry)
    NEXT_ID += 1

    _persist_transaction(entry)
    _touch()
    save_state(force=True)

    if entry["status"] == "successful":
        _risk_eval_on_tx(entry)

    return entry, None


# ------------------------ TRANSACTION ROUTES ------------------------

def _update_tx_status_db(tx_id: int, status: str):
    try:
        with db_cursor(commit=True) as cur:
            cur.execute(
                "UPDATE transactions SET status=%s WHERE id=%s",
                (status, tx_id)
            )
    except Exception as e:
        print("[tx] status db update failed:", e)


@app.route("/api/add-transaction", methods=["POST"])
def add_transaction():
    if not require_login():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    u = current_user()
    if u.get("role") == "customer":
        p = _ensure_profile(u["username"])
        if p.get("id_status") != "approved":
            return jsonify({
                "ok": False,
                "error": "KYC not approved yet. Submit at /kyc/start and wait for approval."
            }), 403

    method_raw = (request.form.get("method") or "").strip().lower()
    if method_raw == "wamd":
        entry, err = _create_wamd_transaction_for(u["username"], request.form)
    else:
        entry, err = _create_transaction_for(u["username"], request.form)

    if err:
        return jsonify({"ok": False, "error": err}), 400

    instructions_url = None
    try:
        if entry.get("method") == "WAMD":
            ref = (entry.get("meta") or {}).get("wamd_reference", "")
            if ref:
                instructions_url = url_for(
                    "wamd_instructions",
                    ref_code=ref,
                    _external=True
                )
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "entry": entry,
        "receipt_url": url_for(
            "transaction_receipt_view",
            receipt_id=entry["receipt_id"],
            t=entry["receipt_token"],
            _external=True
        ),
        "receipt_pdf_url": url_for(
            "transaction_receipt_pdf",
            receipt_id=entry["receipt_id"],
            t=entry["receipt_token"],
            _external=True
        ),
        "wamd_instructions_url": instructions_url
    })


@app.route("/api/add-transaction/wamd", methods=["POST"])
def add_transaction_wamd():
    if not require_login():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    u = current_user()
    if u.get("role") == "customer":
        p = _ensure_profile(u["username"])
        if p.get("id_status") != "approved":
            return jsonify({
                "ok": False,
                "error": "KYC not approved yet. Submit at /kyc/start and wait for approval."
            }), 403

    entry, err = _create_wamd_transaction_for(u["username"], request.form)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    instructions_url = None
    try:
        ref = (entry.get("meta") or {}).get("wamd_reference", "")
        if ref:
            instructions_url = url_for(
                "wamd_instructions",
                ref_code=ref,
                _external=True
            )
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "entry": entry,
        "receipt_url": url_for(
            "transaction_receipt_view",
            receipt_id=entry["receipt_id"],
            t=entry["receipt_token"],
            _external=True
        ),
        "receipt_pdf_url": url_for(
            "transaction_receipt_pdf",
            receipt_id=entry["receipt_id"],
            t=entry["receipt_token"],
            _external=True
        ),
        "wamd_instructions_url": instructions_url
    })


@app.route("/api/merchant/tx/by-userid", methods=["POST"])
def merchant_tx_by_userid():
    if not require_login() or not is_merchant():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    user_id = (request.form.get("user_id") or "").strip()
    profile = _get_profile_by_user_id(user_id)
    if not profile:
        return jsonify({"ok": False, "error": "User ID not found"}), 404
    if profile.get("id_status") != "approved":
        return jsonify({
            "ok": False,
            "error": "Customer KYC not approved. Ask them to complete verification."
        }), 403

    override_phone = profile.get("phone", "")

    method_raw = (request.form.get("method") or "").strip().lower()
    if method_raw == "wamd":
        entry, err = _create_wamd_transaction_for(
            current_user()["username"],
            request.form
        )
    else:
        entry, err = _create_transaction_for(
            current_user()["username"],
            request.form,
            override_phone=override_phone
        )

    if err:
        return jsonify({"ok": False, "error": err}), 400

    entry["target_user_id"] = user_id

    instructions_url = None
    try:
        if entry.get("method") == "WAMD":
            ref = (entry.get("meta") or {}).get("wamd_reference", "")
            if ref:
                instructions_url = url_for(
                    "wamd_instructions",
                    ref_code=ref,
                    _external=True
                )
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "entry": entry,
        "profile": {
            "username": profile["username"],
            "user_id": profile["user_id"],
            "full_name": profile.get("full_name",""),
            "phone": profile.get("phone","")
        },
        "receipt_url": url_for(
            "transaction_receipt_view",
            receipt_id=entry["receipt_id"],
            t=entry["receipt_token"],
            _external=True
        ),
        "receipt_pdf_url": url_for(
            "transaction_receipt_pdf",
            receipt_id=entry["receipt_id"],
            t=entry["receipt_token"],
            _external=True
        ),
        "wamd_instructions_url": instructions_url
    })


@app.route("/api/customer/add-transaction", methods=["POST"])
def customer_add_transaction():
    if not require_login():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    u = current_user()
    if u.get("role") == "customer":
        p = _ensure_profile(u["username"])
        if p.get("id_status") != "approved":
            return jsonify({
                "ok": False,
                "error": "KYC not approved yet. Submit at /kyc/start and wait for approval."
            }), 403

    method_raw = (request.form.get("method") or "").strip().lower()
    if method_raw == "wamd":
        entry, err = _create_wamd_transaction_for(u["username"], request.form)
    else:
        entry, err = _create_transaction_for(u["username"], request.form)

    if err:
        return jsonify({"ok": False, "error": err}), 400

    instructions_url = None
    try:
        if entry.get("method") == "WAMD":
            ref = (entry.get("meta") or {}).get("wamd_reference", "")
            if ref:
                instructions_url = url_for(
                    "wamd_instructions",
                    ref_code=ref,
                    _external=True
                )
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "entry": entry,
        "receipt_url": url_for(
            "transaction_receipt_view",
            receipt_id=entry["receipt_id"],
            t=entry["receipt_token"],
            _external=True
        ),
        "receipt_pdf_url": url_for(
            "transaction_receipt_pdf",
            receipt_id=entry["receipt_id"],
            t=entry["receipt_token"],
            _external=True
        ),
        "wamd_instructions_url": instructions_url
    })


@app.route("/api/my-transactions/<int:tx_id>/status", methods=["POST"])
def update_my_transaction_status(tx_id: int):
    if not require_login():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    u = current_user()
    new_status = _clean_status(request.form.get("status"))

    for t in TRANSACTIONS:
        if t.get("id") == tx_id and t.get("username") == u["username"]:
            t["status"] = new_status
            _update_tx_status_db(tx_id, new_status)
            _touch()
            save_state(force=True)
            return jsonify({"ok": True, "entry": t})

    return jsonify({"ok": False, "error": "Transaction not found"}), 404


@app.route("/api/tx/<int:tx_id>/status", methods=["POST"])
def set_status(tx_id):
    if not require_admin():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    status = _clean_status(request.form.get("status"))
    reason = (request.form.get("reason") or "").strip()

    for t in TRANSACTIONS:
        if t.get("id") == tx_id:
            old = t.get("status", "in progress")
            t["status"] = status
            _update_tx_status_db(tx_id, status)
            _audit(tx_id, current_user()["username"], old, status, reason or "admin-update")
            _touch()
            save_state(force=True)
            return jsonify({"ok": True, "entry": t})

    return jsonify({"ok": False, "error": "Not found"}), 404


@app.route("/api/merchant/tx/<int:tx_id>/status", methods=["POST"])
def merchant_override_status(tx_id: int):
    if not require_login() or not is_merchant():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if not _check_csrf_header():
        return jsonify({"ok": False, "error": "CSRF failed"}), 400

    u = current_user()
    new_status = _clean_status(request.form.get("status"))
    reason = (request.form.get("reason") or "").strip()
    if len(reason) < 3:
        return jsonify({"ok": False, "error": "Reason is required (3+ chars)"}), 400

    for t in TRANSACTIONS:
        if t.get("id") == tx_id:
            if t.get("username") != u["username"]:
                return jsonify({"ok": False, "error": "Forbidden: not your transaction"}), 403
            old_status = t.get("status", "in progress")
            if old_status != "in progress":
                return jsonify({"ok": False, "error": "Cannot change a settled transaction"}), 400
            if new_status not in {"in progress", "successful", "failed"}:
                return jsonify({"ok": False, "error": "Invalid status"}), 400
            t["status"] = new_status
            _update_tx_status_db(tx_id, new_status)
            _audit(tx_id, u["username"], old_status, new_status, reason)
            _touch()
            save_state(force=True)
            return jsonify({"ok": True, "entry": t})

    return jsonify({"ok": False, "error": "Transaction not found"}), 404
# ----------- RECEIPT VIEWS (use flattened tx) -----------

def _get_tx_by_receipt_id(receipt_id: str):
    """
    Memory-first, DB-fallback (restart safe).
    """
    tx = next((x for x in TRANSACTIONS if x.get("receipt_id") == receipt_id), None)
    if tx:
        return tx

    try:
        with db_cursor() as cur:
            cur.execute("SELECT * FROM transactions WHERE receipt_id=%s", (receipt_id,))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            if isinstance(d.get("meta"), str):
                try:
                    d["meta"] = json.loads(d["meta"])
                except Exception:
                    d["meta"] = {}
            return d
    except Exception:
        return None


@app.get("/receipt/<receipt_id>", endpoint="transaction_receipt_view")
def transaction_receipt_view(receipt_id):
    t = request.args.get("t", "")
    tx = _get_tx_by_receipt_id(receipt_id)
    if not tx or not _verify_tx_token(tx, t):
        abort(403)

    flat = _flatten_tx_for_view(tx)
    return render_template(
        "transaction_receipt.html",
        tx=flat,
        exchange_rate=EXCHANGE_RATE,
        brand={
            "black": "#000000",
            "red": "#ff0000",
            "teal": "#063d3f",
            "gold": "#d4a017"
        },
        share_url=url_for(
            "transaction_receipt_view",
            receipt_id=tx["receipt_id"],
            t=tx["receipt_token"],
            _external=True
        ),
        pdf_url=url_for(
            "transaction_receipt_pdf",
            receipt_id=tx["receipt_id"],
            t=tx["receipt_token"],
            _external=True
        )
    )


@app.get("/api/receipt/<receipt_id>")
def api_receipt(receipt_id):
    t = request.args.get("t", "")
    tx = _get_tx_by_receipt_id(receipt_id)
    if not tx or not _verify_tx_token(tx, t):
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    return jsonify({"ok": True, "tx": _flatten_tx_for_view(tx)})


@app.get("/receipt/<receipt_id>.pdf", endpoint="transaction_receipt_pdf")
def transaction_receipt_pdf(receipt_id):
    t = request.args.get("t", "")
    dl = (request.args.get("dl", "").lower() in ("1", "true", "yes"))
    tx = _get_tx_by_receipt_id(receipt_id)
    if not tx or not _verify_tx_token(tx, t):
        abort(403)

    flat = _flatten_tx_for_view(tx)
    html = render_template(
        "transaction_receipt.html",
        tx=flat,
        exchange_rate=EXCHANGE_RATE,
        brand={
            "black": "#000000",
            "red": "#ff0000",
            "teal": "#063d3f",
            "gold": "#d4a017"
        },
        share_url="#",
        pdf_url="#",
        pdf_mode=True
    )

    if not PDF_BACKEND:
        return "PDF engine not available", 501

    pdf = HTML(string=html).write_pdf()
    resp = make_response(pdf)
    resp.headers["Content-Type"] = "application/pdf"
    disp = "attachment" if dl else "inline"
    resp.headers["Content-Disposition"] = f"{disp}; filename={receipt_id}.pdf"
    return resp


@app.get("/view/receipt/<receipt_id>")
def view_transaction_receipt(receipt_id):
    t = request.args.get("t", "")
    pdf_url = url_for(
        "transaction_receipt_pdf",
        receipt_id=receipt_id,
        t=t,
        _external=True
    )
    return _pdf_view_page("Transaction Receipt", pdf_url)


# ----------- WAMD Instructions + misc -----------

@app.get("/wamd/<ref_code>")
def wamd_instructions(ref_code):
    config = _wamd_settings()
    try:
        return render_template(
            "wamd_instructions.html",
            ref_code=ref_code,
            pay_to_phone=config["pay_to_phone"],
            beneficiary=config["beneficiary"]
        )
    except Exception:
        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>WAMD / Link — Instructions</title></head>
<body style="font-family:Arial, sans-serif;max-width:760px;margin:24px auto;padding:0 16px;line-height:1.45">
<h1>WAMD / Link — Payment Instructions</h1>
<p>Complete your bank transfer using the details below. Keep the reference exactly as shown.</p>
<h2>Transfer Details</h2>
<ul>
<li><strong>Pay To (Phone):</strong> <code>{config["pay_to_phone"]}</code></li>
<li><strong>Beneficiary:</strong> {config["beneficiary"]}</li>
<li><strong>Reference:</strong> <code>{ref_code}</code></li>
</ul>
</body></html>"""
        return make_response(html)


@app.get("/transaction_receipt.html")
def legacy_wamd_receipt_redirect():
    ref = (request.args.get("ref") or "").strip()
    if ref:
        return redirect(url_for("wamd_instructions", ref_code=ref))
    return "Not found", 404


# ----------- AUTH STATUS -----------

@app.route("/auth/status")
def auth_status():
    u = session.get("user")
    if u and not session.get("csrf_token"):
        _issue_csrf()
    if u:
        _ensure_profile(u["username"])

    return jsonify({
        "logged_in": bool(u),
        "user": u or {},
        "csrf_token": session.get("csrf_token", ""),
        "my_id_url": url_for("user_id_page", username=u["username"]) if u else ""
    })


# ----------- ADMIN CREATE USER (HASH SAFE) -----------

@app.route("/admin/create-user", methods=["GET", "POST"])
def create_user():
    if not require_admin():
        return redirect("/?login=1")

    if request.method == "GET":
        return render_template("create_user.html")

    email = request.form.get("email", "").strip().lower()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    confirm = request.form.get("confirm_password", "")
    role = (request.form.get("role", "customer") or "customer").strip().lower()

    if not email or "@" not in email:
        return "Invalid email", 400
    if len(username) < 3:
        return "Username too short", 400
    if username in USERS:
        return "Username already exists", 400
    if password != confirm or len(password) < 8:
        return "Invalid password", 400
    if role not in ("customer", "admin", "merchant"):
        return "Invalid role", 400

    USERS[username] = {
        "password": generate_password_hash(password),
        "email": email,
        "role": role
    }

    _ensure_profile(username)
    _touch()
    save_state(force=True)
    return redirect(url_for("admin_page"))


# ----------- ADMIN RECOMPUTE PROFITS (DB SAFE) -----------

@app.route("/admin/recompute_profits", methods=["POST"])
def admin_recompute_profits():
    if not require_admin():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    updated = 0
    for t in TRANSACTIONS:
        charges_kwd = float(t.get("charges_kwd") or 0.0)
        saf_kwd = float(t.get("safaricom_fee_kwd") or 0.0)
        t["profit_kwd"] = charges_kwd - saf_kwd
        _update_tx_status_db(t["id"], t.get("status", "in progress"))
        updated += 1

    _touch()
    save_state(force=True)
    return jsonify({"ok": True, "updated": updated})


# ----------- DEV LOGIN -----------

@app.route("/dev-login")
def dev_login():
    session["user"] = {"username": "admin", "role": "admin"}
    _issue_csrf()
    _ensure_profile("admin")
    _touch()
    save_state(force=True)
    return redirect(url_for("admin_page"))
# ======================================================================================
#                                B O O T
# ======================================================================================

# NOTE:
# This module keeps full in-memory behavior for speed,
# AND mirrors critical events to PostgreSQL for crash safety.
# Nothing in existing logic is removed or changed.

# ======================================================================================
#                       V O U C H E R   M O D U L E   (UPDATED)
# ======================================================================================

# ========= Global config for voucher engine =========
TEST_MODE       = os.environ.get("TEST_MODE", "1") == "1"  # simulate payouts
DARAJA_BASE     = os.environ.get("DARAJA_BASE", "https://sandbox.safaricom.co.ke")

# STK (payer → your shortcode)
CONSUMER_KEY    = os.environ.get("CONSUMER_KEY",  "your_key")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET","your_secret")
SHORTCODE       = os.environ.get("SHORTCODE", "600000")
PASSKEY         = os.environ.get("PASSKEY", "your_passkey")
CALLBACK_URL    = os.environ.get("CALLBACK_URL", "https://example.com/api/mpesa/callback")

# B2C/B2B (your disbursement rails)
B2C_SHORTCODE   = os.environ.get("B2C_SHORTCODE", SHORTCODE)
B2C_INITIATOR   = os.environ.get("B2C_INITIATOR", "testapi")
B2C_PASSWORD    = os.environ.get("B2C_PASSWORD", "initiator_password")
B2C_RESULT_URL  = os.environ.get("B2C_RESULT_URL", "https://example.com/api/mpesa/b2c/result")
B2C_TIMEOUT_URL = os.environ.get("B2C_TIMEOUT_URL","https://example.com/api/mpesa/b2c/timeout")

B2B_SHORTCODE   = os.environ.get("B2B_SHORTCODE", SHORTCODE)
B2B_RESULT_URL  = os.environ.get("B2B_RESULT_URL", "https://example.com/api/mpesa/b2b/result")
B2B_TIMEOUT_URL = os.environ.get("B2B_TIMEOUT_URL","https://example.com/api/mpesa/b2b/timeout")

# ========= In-memory store (voucher engine) =========
V_PAYOUT_Q = queue.Queue()
V_WORKER_READY = threading.Event()
V_VOUCHERS = {}            # code -> voucher
V_CHECKOUT_TO_CODE = {}    # CheckoutRequestID -> code
V_MERCHANTS = {}
V_LEDGER = []
V_VOUCHER_TTL_SECONDS = 10 * 60  # 10 minutes
V_TRANSACTIONS = []        # separate from TRANSACTIONS
V_NEXT_ID = 1

# Bare callback log (persisted)
MPESA_CALLBACKS = []       # {receipt, amount, msisdn, timestamp, checkout}

# ======================================================================================
#                               VOUCHER HELPERS
# ======================================================================================

def v_now_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def v_norm_msisdn(raw: str) -> str:
    if not raw:
        return ""
    d = "".join(ch for ch in raw if ch.isdigit())
    if len(d) == 12 and d.startswith("254") and d[3] in ("7", "1"):
        return d
    if len(d) == 10 and d.startswith("0") and d[1] in ("7", "1"):
        return "254" + d[1:]
    return ""

def v_masked(msisdn: str) -> str:
    return ("*" * max(0, len(msisdn or "") - 4)) + (msisdn or "")[-4:]

def v_is_expired(v) -> bool:
    try:
        return time.time() > v.get("_expires_ts", 0)
    except Exception:
        return False

# ======================================================================================
#                   POSTGRESQL MIRROR (SAFE, OPTIONAL)
# ======================================================================================

def _db_mirror_voucher(voucher: dict):
    """
    Mirrors voucher state to Neon PostgreSQL.
    Does NOT affect runtime flow if DB is unavailable.
    """
    try:
        with db_cursor() as cur:
            cur.execute("""
                INSERT INTO vouchers (
                    code, merchant_id, amount_kes, status,
                    expires_at, created_at, payload
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (code) DO UPDATE SET
                    status=EXCLUDED.status,
                    payload=EXCLUDED.payload
            """, (
                voucher.get("code"),
                voucher.get("merchant_id"),
                voucher.get("amount_kes"),
                voucher.get("status"),
                voucher.get("expires_at"),
                voucher.get("created_at"),
                json.dumps(voucher, ensure_ascii=False)
            ))
    except Exception:
        pass  # zero impact on runtime

def _db_mirror_mpesa_callback(cb: dict):
    try:
        with db_cursor() as cur:
            cur.execute("""
                INSERT INTO mpesa_callbacks
                (receipt, amount, msisdn, checkout_id, timestamp, payload)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                cb.get("receipt"),
                cb.get("amount"),
                cb.get("msisdn"),
                cb.get("checkout"),
                cb.get("timestamp"),
                json.dumps(cb, ensure_ascii=False)
            ))
    except Exception:
        pass

# ======================================================================================
#                 CALLBACK SAFE APPEND (NO LOSS)
# ======================================================================================

def _record_mpesa_callback(cb: dict):
    """
    Records callback in memory, state.json, and PostgreSQL.
    """
    MPESA_CALLBACKS.append(cb)
    _db_mirror_mpesa_callback(cb)
    _touch()
    save_state(force=True)

# ======================================================================================
#                 AUTO-CLEAN EXPIRED VOUCHERS
# ======================================================================================

def _voucher_gc_worker():
    while True:
        try:
            now = time.time()
            expired = [
                code for code, v in list(V_VOUCHERS.items())
                if now > v.get("_expires_ts", 0)
            ]
            for code in expired:
                v = V_VOUCHERS.pop(code, None)
                if v:
                    v["status"] = "expired"
                    _db_mirror_voucher(v)
            if expired:
                _touch()
        except Exception:
            pass
        time.sleep(30)

threading.Thread(target=_voucher_gc_worker, daemon=True).start()
# ==========================================================
# DEV / FALLBACK STUBS (REMOVE IN PRODUCTION)
# ==========================================================

def service_fee_kes_voucher(amount): 
    return amount * 0.02

def stk_push(**kwargs): 
    return {"CheckoutRequestID": uuid.uuid4().hex}

def b2c_send(*a, **k): 
    return {"ConversationID": uuid.uuid4().hex}

def b2b_till(*a, **k): 
    return {"ConversationID": uuid.uuid4().hex}

def bank_transfer(*a, **k): 
    return {"ref": uuid.uuid4().hex}

def v_log_tx(**kwargs): 
    pass

def _pdf_view_page(title, pdf_url):
    return redirect(pdf_url)

# ======================================================================================
#                   VOUCHER PAYOUT WORKER (SAFE, RETRYABLE)
# ======================================================================================

def v_worker_loop():
    V_WORKER_READY.set()

    while True:
        job = V_PAYOUT_Q.get()
        if job is None:
            break

        code = job.get("code")
        tries = int(job.get("tries", 0))
        v = V_VOUCHERS.get(code)

        if not v:
            V_PAYOUT_Q.task_done()
            continue

        try:
            # ---------- Compute net payout ----------
            quote = float(v.get("quote_amount") or 0.0)
            if quote <= 0.0:
                amt_legacy = float(v.get("amount", 0.0))
                fee_legacy = float(v.get("fee", 0.0))
                quote = max(amt_legacy - fee_legacy, 0.0)
            net = quote

            payout = v.get("payout", {})
            method = (payout.get("method") or "").lower()
            target = payout.get("target") or ""

            # ---------- Execute payout ----------
            if method == "phone":
                res = b2c_send(target, net, remarks=f"Voucher {code}")
            elif method == "till":
                res = b2b_till(target, net, remarks=f"Voucher {code}")
            elif method == "bank":
                res = bank_transfer(
                    target,
                    net,
                    payout.get("bank_name", ""),
                    payout.get("bank_branch", "")
                )
            else:
                raise RuntimeError(f"Unknown payout method: {method}")

            # ---------- Mark voucher paid ----------
            v["status"] = "paid"
            v["updated_at"] = v_now_iso()
            v["payout_ref"] = res.get("ConversationID") or res.get("ref") or ""

            # ---------- Voucher ledger ----------
            V_MERCHANTS.setdefault(v["merchant_id"], {"id": v["merchant_id"], "balance": 0.0})
            V_LEDGER.append({
                "ts": v_now_iso(),
                "type": "payout",
                "code": code,
                "merchant_id": v["merchant_id"],
                "net": net,
                "method": method,
                "target": target,
                "ref": v["payout_ref"]
            })

            # ---------- Financial truth (GLOBAL TRANSACTIONS) ----------
            v_log_tx(
                actor_username=v.get("merchant_id", "system"),
                kind="voucher:payout",
                method=("Mpesa" if method in ("phone", "till") else "Bank"),
                tx_type="Withdrawal",
                currency="KES",
                amount_kes=net,
                exchange_rate=EXCHANGE_RATE,
                charges_kes=0.0,
                safaricom_fee_kes=0.0,
                phone=(target if method == "phone" else ""),
                bank_account=(target if method == "bank" else ""),
                paybill=(target if method == "till" else ""),
                status="successful",
                participants=list(filter(None, [
                    v.get("merchant_id"),
                    v.get("user_id"),
                    v.get("payer_phone")
                ])),
                meta={
                    "voucher_code": code,
                    "payout_method": method,
                    "payout_ref": v["payout_ref"]
                }
            )

            _risk_eval_on_voucher_payout(v, net_amount=net)
            _db_mirror_voucher(v)
            _touch()
            save_state(force=True)

            # ---------- Notifications ----------
            try:
                txt = _compose_payout_text(v, ok=True, net=net, ref=v["payout_ref"])
                send_sms(v.get("payer_phone", ""), txt)
                if PROFILES.get(v["merchant_id"], {}).get("notify_whatsapp"):
                    send_whatsapp(v.get("payer_phone", ""), txt)
            except Exception:
                pass

        except Exception as e:
            tries += 1

            if tries < 5:
                job["tries"] = tries
                time.sleep(min(10 * tries, 60))
                V_PAYOUT_Q.put(job)
            else:
                v["status"] = "failed_payout"
                v["payout_error"] = str(e)
                v["updated_at"] = v_now_iso()

                v_log_tx(
                    actor_username=v.get("merchant_id", "system"),
                    kind="voucher:payout",
                    method=("Mpesa" if payout.get("method") in ("phone", "till") else "Bank"),
                    tx_type="Withdrawal",
                    currency="KES",
                    amount_kes=net,
                    exchange_rate=EXCHANGE_RATE,
                    charges_kes=0.0,
                    safaricom_fee_kes=0.0,
                    phone="",
                    bank_account="",
                    paybill="",
                    status="failed",
                    participants=[v.get("merchant_id"), v.get("payer_phone")],
                    meta={"voucher_code": code, "error": str(e)}
                )

                _db_mirror_voucher(v)
                _touch()
                save_state(force=True)

        finally:
            V_PAYOUT_Q.task_done()


def _risk_eval_on_voucher_payout(voucher: dict, *, net_amount: float):
    """
    Lightweight risk hook for voucher payouts.
    Mirrors transaction risk checks without blocking payouts.
    """
    try:
        flags = []
        if net_amount > 5000:
            flags.append("HIGH_VALUE_PAYOUT")
        if voucher.get("tries", 0) >= 3:
            flags.append("RETRY_PAYOUT")

        if flags:
            AUDIT_LOG.append({
                "id": len(AUDIT_LOG) + 1,
                "tx_id": 0,
                "actor": voucher.get("merchant_id","system"),
                "old_status": "paying",
                "new_status": voucher.get("status"),
                "reason": "voucher-risk",
                "flags": flags,
                "timestamp": datetime.utcnow().isoformat(timespec="seconds")+"Z"
            })
            _touch()
    except Exception:
        pass


# ========= Voucher pages / QR / PDF =========
@app.route("/voucher")
def voucher_creator_page():
    return render_template("create_voucher.html")

@app.route("/v/<code>")
def voucher_page(code):
    v = V_VOUCHERS.get(code)
    if not v: return "Voucher not found", 404
    return render_template("voucher.html", code=code, merchant_id=v["merchant_id"])

@app.route("/r/<code>/<token>")
def voucher_receipt(code, token):
    v = V_VOUCHERS.get(code)
    if not v or token != v.get("receipt_token"):
        return "Receipt not found", 404
    if not v["used"] or v["status"] not in ("credited","paid","paying","failed_payout"):
        return "Receipt not available yet", 409
    return render_template("receipt.html",
        provider="Cheque Matez",
        amount=float(v.get("quote_amount",0)),
        fee=float(v.get("fee",0)),
        total=float(v.get("stk_total", v.get("amount",0)+v.get("fee",0))),
        receipt_no=v.get("receipt_no",""),
        checkout=v.get("stk_ref",""),
        date=v.get("used_at") or v.get("updated_at") or v.get("created_at"),
        merchant_id=v.get("merchant_id"),
        customer_name=v.get("payer_name") or v.get("payer_phone"),
        customer_phone_mask=v_masked(v.get("payer_phone","")),
        payout=v.get("payout",{}),
        brand={"black":"#000000","red":"#ff0000","teal":"#063d3f","gold":"#d4a017"},
        share_url=url_for("voucher_receipt", code=code, token=token, _external=True),
        pdf_url=url_for("voucher_receipt_pdf", code=code, token=token, _external=True)
    )

@app.route("/r/<code>/<token>.pdf")
def voucher_receipt_pdf(code, token):
    v = V_VOUCHERS.get(code)
    dl = (request.args.get("dl","").lower() in ("1","true","yes"))
    if not v or token != v.get("receipt_token"):
        return "Not found", 404
    html = render_template("receipt.html",
        provider="Cheque Matez",
        amount=float(v.get("quote_amount",0)),
        fee=float(v.get("fee",0)),
        total=float(v.get("stk_total", v.get("amount",0)+v.get("fee",0))),
        receipt_no=v.get("receipt_no",""),
        checkout=v.get("stk_ref",""),
        date=v.get("used_at") or v.get("updated_at") or v.get("created_at"),
        merchant_id=v.get("merchant_id"),
        customer_name=v.get("payer_name") or v.get("payer_phone"),
        customer_phone_mask=v_masked(v.get("payer_phone","")),
        payout=v.get("payout",{}),
        brand={"black":"#000000","red":"#ff0000","teal":"#063d3f","gold":"#d4a017"},
        share_url="#", pdf_url="#"
    )
    if not PDF_BACKEND:
        return "PDF engine not available", 501
    pdf = HTML(string=html).write_pdf()
    resp = make_response(pdf)
    resp.headers["Content-Type"] = "application/pdf"
    disp = "attachment" if dl else "inline"
    resp.headers["Content-Disposition"] = f"{disp}; filename=voucher_{code}.pdf"
    return resp

@app.get("/view/voucher/<code>/<token>")
def view_voucher_receipt(code, token):
    pdf_url = url_for("voucher_receipt_pdf", code=code, token=token, _external=True)
    return _pdf_view_page("Voucher Receipt", pdf_url)

@app.route("/v/<code>/qr.png")
def voucher_qr(code):
    v = V_VOUCHERS.get(code)
    if not v: return "Not found", 404
    url = url_for("voucher_page", code=code, _external=True)
    img = qrcode.make(url)
    buf = BytesIO(); img.save(buf, format="PNG"); buf.seek(0)
    resp = make_response(buf.read())
    resp.headers["Content-Type"] = "image/png"
    resp.headers["Cache-Control"] = "no-store"
    return resp

# ========= Voucher API =========
@app.route("/api/vouchers/create", methods=["POST"])
def api_vouchers_create():
    payer_name  = (request.form.get("payer_name") or "").strip()
    payer_phone = v_norm_msisdn(request.form.get("payer_phone") or "")
    if not payer_phone:
        return jsonify({"ok": False, "error":"Enter a valid phone (2547…/2541…)."}), 400

    merchant_id = (request.form.get("merchant_id") or "shop-001").strip()
    max_amount  = float(request.form.get("max_amount") or 0) or None
    user_id     = (request.form.get("user_id") or "").strip()

    code = uuid.uuid4().hex[:10].upper()
    now_ts = time.time()
    expires_ts = now_ts + V_VOUCHER_TTL_SECONDS
    V_VOUCHERS[code] = {
        "code": code,
        "created_at": v_now_iso(),
        "updated_at": v_now_iso(),
        "expires_at": datetime.utcfromtimestamp(expires_ts).isoformat(timespec="seconds")+"Z",
        "_expires_ts": expires_ts,

        "status": "pending",
        "one_time": True,
        "used": False,
        "used_at": "",

        "payer_name": payer_name,
        "payer_phone": payer_phone,
        "user_id": user_id,
        "merchant_id": merchant_id,

        "max_amount": max_amount,

        "service_charge_on": True,
        "quote_amount": 0.0,
        "amount": 0.0,
        "fee": 0.0,
        "stk_total": 0.0,

        "receipt_no": "",
        "stk_ref": "",
        "receipt_token": uuid.uuid4().hex[:12],

        "payout": {"method": "", "target": "", "bank_name": "", "bank_branch": ""}
    }
    _touch()
    return jsonify({"ok": True, "code": code, "url": url_for("voucher_page", code=code, _external=True)})

@app.route("/api/vouchers/<code>/status")
def api_voucher_status(code):
    v = V_VOUCHERS.get(code)
    if not v: return jsonify({"ok": False, "error": "Not found"}), 404
    if v["status"] in ("pending","processing") and v_is_expired(v):
        v["status"] = "failed"; v["updated_at"] = v_now_iso(); _touch()
    out = {
        "code": v["code"], "status": v["status"], "used": v["used"],
        "expires_at": v["expires_at"], "amount": v.get("amount",0),
        "fee": v.get("fee",0), "merchant_id": v.get("merchant_id"),
    }
    if v["used"] and v["status"] in ("credited","paid","paying","failed_payout"):
        out["receipt_url"] = url_for("voucher_receipt", code=code, token=v["receipt_token"], _external=True)
    return jsonify({"ok": True, "voucher": out})

@app.route("/api/vouchers/<code>/pay", methods=["POST"])
def api_voucher_pay(code):
    v = V_VOUCHERS.get(code)
    if not v: return jsonify({"ok": False, "error": "Not found"}), 404
    if v["used"] or v["status"] in ("credited","failed","paid","paying","failed_payout"):
        return jsonify({"ok": False, "error": "Voucher closed/used"}), 409
    if v_is_expired(v):
        v["status"] = "failed"; v["updated_at"] = v_now_iso(); _touch()
        return jsonify({"ok": False, "error": "Voucher expired"}), 410

    amount = float(request.form.get("amount") or 0)
    if amount <= 0: return jsonify({"ok": False, "error":"Enter a valid amount"}), 400
    if v.get("max_amount") and amount > v["max_amount"]:
        return jsonify({"ok": False, "error": f"Max allowed is {v['max_amount']}"}), 400

    method = (request.form.get("payout_method") or "").lower()
    target = (request.form.get("payout_target") or "").strip()
    bank_name = (request.form.get("payout_bank_name") or "").strip()
    bank_branch = (request.form.get("payout_bank_branch") or "").strip()

    if method not in ("phone","till","bank"):
        return jsonify({"ok": False, "error": "Select payout method (Phone/Till/Bank)"}), 400
    if method=="phone":
        tgt = v_norm_msisdn(target)
        if not tgt: return jsonify({"ok": False, "error":"Enter valid phone 2547…/2541…"}), 400
        target = tgt
    elif method=="till":
        if not target.isdigit() or len(target) < 5:
            return jsonify({"ok": False, "error":"Enter a valid till/buygoods number"}), 400
    elif method=="bank":
        if len(target) < 4:
            return jsonify({"ok": False, "error":"Enter a valid bank account"}), 400

    v["payout"] = {"method": method, "target": target, "bank_name": bank_name, "bank_branch": bank_branch}

    svc_on = (request.form.get("service_charge_on") or "1").strip().lower() in ("1","true","on","yes")
    v["service_charge_on"] = svc_on

    if svc_on:
        quote_amount = amount
        fee = service_fee_kes_voucher(quote_amount)
        stk_total = quote_amount + fee
    else:
        total_entered = amount
        fee = service_fee_kes_voucher(total_entered)
        quote_amount = max(total_entered - fee, 0.0)
        stk_total = total_entered

    v["quote_amount"] = quote_amount
    v["amount"] = quote_amount
    v["fee"] = fee
    v["stk_total"] = stk_total
    _touch()

    try:
        resp = stk_push(
            phone=v["payer_phone"],
            amount=stk_total,
            account_ref=v["merchant_id"],
            description=f"Voucher {code} at {v['merchant_id']}"
        )
        checkout_id = resp.get("CheckoutRequestID", "")
        if not checkout_id:
            return jsonify({"ok": False, "error":"STK not accepted"}), 502
        V_CHECKOUT_TO_CODE[checkout_id] = code
        v["stk_ref"] = checkout_id
        v["status"] = "processing"
        v["updated_at"] = v_now_iso()
        _touch()
        return jsonify({"ok": True, "status":"processing"})
    except requests.HTTPError as e:
        return jsonify({"ok": False, "error": f"STK error: {e.response.text}"}), 502
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ========= Daraja STK callback (voucher) =========
@app.route("/api/mpesa/callback", methods=["POST"])
def mpesa_callback():
    data = request.get_json(silent=True, force=True) or {}
    cb = (data.get("Body") or {}).get("stkCallback") or {}
    result_code = cb.get("ResultCode", -1)
    checkout = cb.get("CheckoutRequestID", "")

    meta = {}
    if cb.get("CallbackMetadata") and isinstance(cb["CallbackMetadata"], dict):
        try:
            meta = {i["Name"]: i["Value"] for i in cb["CallbackMetadata"].get("Item", [])}
        except Exception:
            meta = {}

    code = V_CHECKOUT_TO_CODE.get(checkout, "")
    v = V_VOUCHERS.get(code) if code else None
    if not v:
        return jsonify({"ok": True})

    if v["used"] and v["status"] in ("paid","failed_payout","paying"):
        return jsonify({"ok": True})

    try:
        MPESA_CALLBACKS.append({
            "receipt": meta.get("MpesaReceiptNumber",""),
            "amount": float(meta.get("Amount", 0) or 0.0),
            "msisdn": str(meta.get("PhoneNumber","")),
            "timestamp": datetime.utcnow().isoformat(timespec="seconds")+"Z",
            "checkout": checkout
        })
        _touch()
    except Exception:
        pass

    if result_code == 0:
        paid_amount = float(meta.get("Amount", 0))
        paid_phone  = str(meta.get("PhoneNumber",""))
        receipt     = meta.get("MpesaReceiptNumber","")

        expected_total = float(v.get("stk_total") or 0.0)
        if expected_total <= 0:
            expected_total = float(v.get("amount", 0.0)) + float(v.get("fee", 0.0))

        if abs(paid_amount - expected_total) > 0.001:
            v["status"] = "failed"; _touch()
        elif v.get("payer_phone") and not paid_phone.endswith(v["payer_phone"][-9:]):
            v["status"] = "failed"; _touch()
        else:
            v["receipt_no"] = receipt
            v["status"] = "credited"
            v["updated_at"] = v_now_iso()
            v["used"] = True
            v["used_at"] = v_now_iso()
            _touch()

            quote_amount = float(v.get("quote_amount") or v.get("amount") or 0.0)
            fee_kes = float(v.get("fee", 0.0))

            m = V_MERCHANTS.setdefault(v["merchant_id"], {"id": v["merchant_id"], "balance": 0.0})
            m["balance"] += max(quote_amount, 0.0)
            V_LEDGER.append({"ts": v_now_iso(), "type":"credited", "code": v["code"],
                             "merchant_id": v["merchant_id"], "net": quote_amount, "receipt": receipt,
                             "payout": v.get("payout",{})})
            _touch()

            actor = v.get("user_id") or v["payer_phone"]
            parts = [v["merchant_id"]]
            if v.get("user_id"): parts.append(v["user_id"])
            if v.get("payer_phone"): parts.append(v["payer_phone"])

            v_log_tx(
                actor_username=actor,
                kind="voucher:fund",
                method="Mpesa",
                tx_type="Deposit",
                currency="KES",
                amount_kes=paid_amount,
                exchange_rate=EXCHANGE_RATE,
                charges_kes=fee_kes,
                safaricom_fee_kes=0.0,
                phone=v["payer_phone"],
                bank_account="",
                paybill="",
                status="successful",
                participants=list(set(parts)),
                meta={"voucher_id": code, "mpesa_receipt": receipt,
                      "service_charge_on": bool(v.get("service_charge_on", True))}
            )

            try:
                txt = _compose_voucher_receipt_text(v, paid_amount=paid_amount, fee_kes=fee_kes, receipt=receipt)
                send_sms(v["payer_phone"], txt)
                if PROFILES.get(actor, {}).get("notify_whatsapp"):
                    send_whatsapp(v["payer_phone"], txt)
            except Exception:
                pass

            v_enqueue_payout(code)
    else:
        v["status"] = "failed"
        v["updated_at"] = v_now_iso()
        _touch()

    try: V_CHECKOUT_TO_CODE.pop(checkout, None)
    except: pass

    return jsonify({"ok": True})
# ======================================================================================
#                        R E C O N C I L I A T I O N  +  R E P O R T S
# ======================================================================================

def _parse_date(dstr: str) -> datetime:
    return datetime.strptime(dstr, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def _parse_iso(ts: str) -> datetime:
    if not ts:
        return datetime.min.replace(tzinfo=timezone.utc)
    t = ts.rstrip("Z")
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _range_utc(day_utc: datetime):
    d = day_utc if day_utc.tzinfo else day_utc.replace(tzinfo=timezone.utc)
    start = d.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

def _ym_first_last(ym: str):
    dt = datetime.strptime(ym, "%Y-%m").replace(tzinfo=timezone.utc)
    first = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if dt.month == 12:
        nxt = dt.replace(year=dt.year+1, month=1, day=1)
    else:
        nxt = dt.replace(month=dt.month+1, day=1)
    last = nxt - timedelta(seconds=1)
    return first, last

def reconcile_day_to_rows(day_utc: datetime):
    start, end = _range_utc(day_utc)

    def in_range(ts: str) -> bool:
        t = _parse_iso(ts)
        return start <= t < end

    internal = [x for x in (V_TRANSACTIONS + TRANSACTIONS)
                if x.get("status")=="successful"
                and x.get("tx_type") in ("Deposit","Withdrawal")
                and in_range(x.get("timestamp",""))]

    external = [e for e in MPESA_CALLBACKS if in_range(e.get("timestamp",""))]

    ext_idx = {}
    for e in external:
        r = e.get("receipt","")
        if r: ext_idx[r] = e

    rows = []
    missing_ext, missing_int, amount_mismatch = 0, 0, 0

    for t in internal:
        r = (t.get("meta") or {}).get("mpesa_receipt") if t.get("kind","").startswith("voucher:") else t.get("receipt_no")
        e = ext_idx.get(r) if r else None
        if not e:
            missing_ext += 1
            rows.append(["INT_ONLY", r or "-", round(t.get("amount_kes",0.0),2), t.get("phone",""), t.get("timestamp","")])
        else:
            if abs(float(e.get("amount") or 0.0) - float(t.get("amount_kes") or 0.0)) > 0.5:
                amount_mismatch += 1
                rows.append(["AMOUNT_DIFF", r, round(t.get("amount_kes",0.0),2), round(e.get("amount",0.0),2), t.get("timestamp","")])
            del ext_idx[r]

    for r,e in list(ext_idx.items()):
        missing_int += 1
        rows.append(["EXT_ONLY", r, round(e.get("amount",0.0),2), e.get("msisdn",""), e.get("timestamp","")])

    return rows, {"missing_ext": missing_ext, "missing_int": missing_int, "amount_mismatch": amount_mismatch}

def _csv_bytes(headers, rows):
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(headers)
    for row in rows:
        w.writerow(row)
    mem = io.BytesIO(out.getvalue().encode("utf-8"))
    out.close()
    return mem

@app.route("/admin/reconcile")
def admin_reconcile_page():
    if not require_admin():
        return redirect("/?login=1")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return render_template("admin_reconcile.html", today=today)

@app.route("/admin/reconcile.csv")
def admin_reconcile_csv():
    if not require_admin():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    rows, stats = reconcile_day_to_rows(_parse_date(date_str))
    mem = _csv_bytes(["status","receipt","int_amount_or_ext","ext_amount_or_phone","timestamp"], rows)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=f"recon_{date_str}.csv")

@app.route("/admin/reconcile.pdf")
def admin_reconcile_pdf():
    if not require_admin():
        return "Unauthorized", 401
    if not PDF_BACKEND:
        return "PDF engine not available", 501
    dl = (request.args.get("dl","").lower() in ("1","true","yes"))
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    rows, stats = reconcile_day_to_rows(_parse_date(date_str))
    html = render_template("admin_reconcile_pdf.html", date=date_str, rows=rows, stats=stats)
    pdf = HTML(string=html).write_pdf()
    resp = make_response(pdf); resp.headers["Content-Type"]="application/pdf"
    disp = "attachment" if dl else "inline"
    resp.headers["Content-Disposition"]=f"{disp}; filename=recon_{date_str}.pdf"
    return resp

@app.get("/admin/reconcile/view")
def admin_reconcile_viewer():
    if not require_admin():
        return redirect("/?login=1")
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    pdf_url = url_for("admin_reconcile_pdf", date=date_str, _external=True)
    return _pdf_view_page(f"Reconciliation — {date_str}", pdf_url)

# ---------- Monthly Regulatory Reports ----------
def _in_month(ts: str, first: datetime, last: datetime) -> bool:
    t = _parse_iso(ts)
    if first.tzinfo is None: first = first.replace(tzinfo=timezone.utc)
    if last.tzinfo is None: last = last.replace(tzinfo=timezone.utc)
    return first <= t <= last

def monthly_aggregate(ym: str):
    first, last = _ym_first_last(ym)

    data = [*V_TRANSACTIONS, *TRANSACTIONS]
    month = [t for t in data if _in_month(t.get("timestamp",""), first, last)]

    by_day = {}
    totals = {"deposits_kes":0.0, "withdrawals_kes":0.0, "fees_kes":0.0, "count":0}
    for t in month:
        d = (t.get("timestamp","") or "")[:10]
        by_day.setdefault(d, {"dep":0.0,"wdr":0.0,"fees":0.0,"cnt":0})
        amt = float(t.get("amount_kes",0.0))
        if t.get("tx_type")=="Deposit": by_day[d]["dep"] += amt; totals["deposits_kes"] += amt
        if t.get("tx_type")=="Withdrawal": by_day[d]["wdr"] += amt; totals["withdrawals_kes"] += amt
        chg_kes = float(t.get("safaricom_fee_kes",0.0) or 0.0) + (float(t.get("charges_kwd",0.0) or 0.0) * EXCHANGE_RATE)
        by_day[d]["fees"] += chg_kes
        totals["fees_kes"] += chg_kes
        by_day[d]["cnt"] += 1; totals["count"] += 1

    flags = [a for a in AUDIT_LOG if a.get("actor")=="risk-engine" and _in_month(a.get("timestamp",""), first, last)]
    suspicious_count = len(flags)

    rows = [["Date","Deposits (KES)","Withdrawals (KES)","Fees (KES)","Count"]]
    for day in sorted(by_day.keys()):
        v = by_day[day]
        rows.append([day, round(v["dep"],2), round(v["wdr"],2), round(v["fees"],2), v["cnt"]])
    rows.append([])
    rows.append(["TOTALS", round(totals["deposits_kes"],2), round(totals["withdrawals_kes"],2), round(totals["fees_kes"],2), totals["count"]])
    rows.append(["Suspicious Flags", suspicious_count])

    return rows, flags

@app.route("/admin/reports")
def admin_reports_page():
    if not require_admin(): return redirect("/?login=1")
    ym = datetime.utcnow().strftime("%Y-%m")
    return render_template("admin_reports.html", ym=ym)

@app.route("/admin/reports/monthly.csv")
def admin_reports_monthly_csv():
    if not require_admin():
        return "Unauthorized", 401
    ym = request.args.get("ym") or datetime.utcnow().strftime("%Y-%m")
    rows, flags = monthly_aggregate(ym)
    mem = _csv_bytes(rows[0], rows[1:])
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=f"report_{ym}.csv")

@app.route("/admin/reports/monthly.pdf")
def admin_reports_monthly_pdf():
    if not require_admin(): return "Unauthorized", 401
    if not PDF_BACKEND: return "PDF engine not available", 501
    dl = (request.args.get("dl","").lower() in ("1","true","yes"))
    ym = request.args.get("ym") or datetime.utcnow().strftime("%Y-%m")
    rows, flags = monthly_aggregate(ym)
    html = render_template("admin_report_pdf.html", ym=ym, rows=rows, flags=flags)
    pdf = HTML(string=html).write_pdf()
    resp = make_response(pdf)
    resp.headers["Content-Type"] = "application/pdf"
    disp = "attachment" if dl else "inline"
    resp.headers["Content-Disposition"] = f"{disp}; filename=report_{ym}.pdf"
    return resp

@app.get("/admin/reports/monthly/view")
def admin_reports_monthly_view():
    if not require_admin(): return redirect("/?login=1")
    ym = request.args.get("ym") or datetime.utcnow().strftime("%Y-%m")
    pdf_url = url_for("admin_reports_monthly_pdf", ym=ym, _external=True)
    return _pdf_view_page(f"Monthly Report — {ym}", pdf_url)

# ======================================================================================
#                          R I S K   &   F R A U D   E N G I N E
# ======================================================================================

def _risk_write(tx_id, reason, severity="med", tags=None):
    _audit(tx_id=tx_id, actor="risk-engine", old_status="", new_status="", reason=reason,
           severity=severity, tags=list(set(tags or [])))

def _risk_eval_on_tx(entry: dict):
    try:
        phone = entry.get("phone","")
        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        cutoff = now_utc - timedelta(minutes=10)
        recent = [t for t in (V_TRANSACTIONS + TRANSACTIONS)
                  if t.get("phone")==phone and t.get("status")=="successful"
                  and _parse_iso(t.get("timestamp","")) >= cutoff]
        if len(recent) > 5 and phone:
            _risk_write(entry.get("id") or entry.get("meta",{}).get("voucher_id",""),
                        f"velocity: {len(recent)} tx in 10m for {phone}",
                        severity="high", tags=["risk","velocity"])

        amt = float(entry.get("amount_kes",0.0) or 0.0)
        band_center = 100000 * round(amt/100000) if amt>0 else 0
        if band_center>0 and abs(amt - band_center) <= (0.05*band_center):
            cutoff2 = now_utc - timedelta(hours=24)
            same_band = 0
            for t in (V_TRANSACTIONS + TRANSACTIONS):
                if t.get("status")!="successful": continue
                if _parse_iso(t.get("timestamp","")) < cutoff2: continue
                a = float(t.get("amount_kes",0.0) or 0.0)
                if band_center>0 and abs(a - band_center) <= (0.05*band_center):
                    same_band += 1
            if same_band >= 4:
                _risk_write(entry.get("id") or entry.get("meta",{}).get("voucher_id",""),
                            f"structuring: {same_band} tx near {band_center} KES/24h",
                            severity="med", tags=["risk","structuring"])

        try:
            ts = _parse_iso(entry.get("timestamp",""))
            hour_eat = (ts.hour + 3) % 24
            if hour_eat < 5 and float(entry.get("amount_kes",0.0) or 0.0) >= 50000:
                _risk_write(entry.get("id") or entry.get("meta",{}).get("voucher_id",""),
                            f"odd-hours high amount at {hour_eat:02d}:00 EAT",
                            severity="low", tags=["risk","time"])
        except Exception:
            pass
    except Exception:
        pass

@app.route("/admin/risk")
def admin_risk_page():
    if not require_admin(): return redirect("/?login=1")
    return render_template("admin_risk.html")

@app.route("/admin/risk/json")
def admin_risk_json():
    if not require_admin(): return jsonify({"ok": False, "error": "Unauthorized"}), 401
    items = [a for a in AUDIT_LOG if a.get("actor")=="risk-engine"]
    return jsonify({"ok": True, "items": list(reversed(items))})
# ======================================================================================
if __name__ == "__main__":
    load_state()

    threading.Thread(target=_autosave_worker, daemon=True).start()

    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        threading.Thread(target=v_worker_loop, daemon=True).start()
        V_WORKER_READY.wait(timeout=2)

    app.run(host="127.0.0.1", port=5000, debug=True)

