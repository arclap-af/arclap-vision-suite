"""Local-users authentication.

Simplest possible auth that's still safe: bcrypt-hashed passwords in
SQLite, signed session cookies (HMAC-SHA256), no JWTs, no external
identity provider. Two roles: 'admin' and 'viewer'.

Bootstrap
---------
First run creates a default admin user and prints the auto-generated
password to stdout exactly once. Set ARCLAP_DISABLE_AUTH=1 to bypass
auth entirely (single-user dev mode).

Why not JWT
-----------
JWT is overkill for a single-process suite. Cookies + an HMAC secret
work perfectly here, are stateless across workers, and don't need a
key-rotation story.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from threading import Lock

_LOCK = Lock()
_DB_PATH: Path | None = None
_SECRET: bytes = b""

SESSION_COOKIE = "arclap_session"
SESSION_TTL = 30 * 24 * 3600   # 30 days

# Roles in increasing privilege order
ROLES = ("viewer", "admin")


def _conn() -> sqlite3.Connection:
    assert _DB_PATH is not None
    c = sqlite3.connect(str(_DB_PATH))
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c


def init(db_path: Path, secret_path: Path) -> None:
    """Set up the auth DB + load/generate the HMAC secret."""
    global _DB_PATH, _SECRET
    _DB_PATH = Path(db_path)
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    secret_path = Path(secret_path)
    if secret_path.is_file():
        _SECRET = secret_path.read_bytes().strip()
    else:
        _SECRET = secrets.token_bytes(32)
        secret_path.write_bytes(_SECRET)
        try:
            os.chmod(secret_path, 0o600)
        except Exception:
            pass

    with _LOCK:
        c = _conn()
        try:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    salt TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'viewer',
                    created_at REAL NOT NULL,
                    last_login_at REAL,
                    disabled INTEGER NOT NULL DEFAULT 0
                );
            """)
            c.commit()
            # Bootstrap admin if the table is empty
            n = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if n == 0:
                pw = secrets.token_urlsafe(12)
                _create_user_unlocked(c, "admin", pw, "admin")
                print(
                    "\n" + "=" * 60 + "\n"
                    "  ARCLAP AUTH BOOTSTRAP — first-run admin password\n" + "=" * 60 + "\n"
                    f"  username: admin\n  password: {pw}\n"
                    f"  Change it via POST /api/auth/change-password\n"
                    + "=" * 60 + "\n",
                    flush=True,
                )
        finally:
            c.close()


# ─── Password hashing ────────────────────────────────────────────────────────
def _hash(password: str, salt: bytes) -> str:
    """PBKDF2-HMAC-SHA256, 200_000 iterations. Slow enough to resist offline
    cracking, fast enough that every login isn't 2 seconds."""
    derived = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
    return derived.hex()


def _create_user_unlocked(c: sqlite3.Connection, username: str,
                          password: str, role: str = "viewer") -> None:
    salt = secrets.token_bytes(16)
    h = _hash(password, salt)
    c.execute(
        "INSERT INTO users(username, password_hash, salt, role, created_at) "
        "VALUES(?,?,?,?,?)",
        (username, h, salt.hex(), role, time.time()),
    )
    c.commit()


def create_user(username: str, password: str, role: str = "viewer") -> None:
    if role not in ROLES:
        raise ValueError(f"role must be one of {ROLES}")
    with _LOCK:
        c = _conn()
        try:
            _create_user_unlocked(c, username, password, role)
        finally:
            c.close()


def verify_password(username: str, password: str) -> dict | None:
    """Returns the user row dict on success, None on failure."""
    with _LOCK:
        c = _conn()
        try:
            c.row_factory = sqlite3.Row
            row = c.execute(
                "SELECT * FROM users WHERE username = ? AND disabled = 0",
                (username,),
            ).fetchone()
            if not row:
                return None
            salt = bytes.fromhex(row["salt"])
            if not hmac.compare_digest(_hash(password, salt), row["password_hash"]):
                return None
            c.execute(
                "UPDATE users SET last_login_at = ? WHERE username = ?",
                (time.time(), username),
            )
            c.commit()
            return dict(row)
        finally:
            c.close()


def change_password(username: str, old_password: str, new_password: str) -> bool:
    if not verify_password(username, old_password):
        return False
    with _LOCK:
        c = _conn()
        try:
            salt = secrets.token_bytes(16)
            c.execute(
                "UPDATE users SET password_hash = ?, salt = ? WHERE username = ?",
                (_hash(new_password, salt), salt.hex(), username),
            )
            c.commit()
            return True
        finally:
            c.close()


# ─── Session cookies (HMAC-signed) ──────────────────────────────────────────
def make_session(username: str, role: str) -> str:
    payload = {"u": username, "r": role, "exp": int(time.time()) + SESSION_TTL}
    body = json.dumps(payload, separators=(",", ":")).encode()
    sig = hmac.new(_SECRET, body, hashlib.sha256).hexdigest()
    return body.hex() + "." + sig


def parse_session(cookie: str | None) -> dict | None:
    if not cookie or "." not in cookie:
        return None
    body_hex, sig = cookie.rsplit(".", 1)
    try:
        body = bytes.fromhex(body_hex)
    except ValueError:
        return None
    expected = hmac.new(_SECRET, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(body.decode())
    except Exception:
        return None
    if int(payload.get("exp", 0)) < int(time.time()):
        return None
    return payload


def list_users() -> list[dict]:
    with _LOCK:
        c = _conn()
        try:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                "SELECT username, role, created_at, last_login_at, disabled FROM users"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            c.close()


def disable_user(username: str) -> bool:
    with _LOCK:
        c = _conn()
        try:
            n = c.execute(
                "UPDATE users SET disabled = 1 WHERE username = ?", (username,)
            ).rowcount
            c.commit()
            return n > 0
        finally:
            c.close()


def is_auth_disabled() -> bool:
    return os.environ.get("ARCLAP_DISABLE_AUTH", "").lower() in {"1", "true", "yes"}
