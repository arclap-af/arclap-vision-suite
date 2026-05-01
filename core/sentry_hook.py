"""Sentry / OpenTelemetry error reporting hook.

Activated only when SENTRY_DSN env var is set. No-op otherwise. The
sentry-sdk dep is optional — install with: pip install sentry-sdk
"""
from __future__ import annotations

import logging
import os

_log = logging.getLogger(__name__)


def maybe_init_sentry() -> bool:
    """Init Sentry if SENTRY_DSN is set and sentry_sdk is installed.
    Returns True if reporting is now active, False otherwise."""
    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        return False
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
    except ImportError:
        _log.warning("SENTRY_DSN set but sentry-sdk not installed; "
                     "pip install sentry-sdk to enable")
        return False

    sample_rate = float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.10"))
    env_name = os.environ.get("SENTRY_ENVIRONMENT", "production")
    release = os.environ.get("ARCLAP_RELEASE", "unknown")

    sentry_sdk.init(
        dsn=dsn,
        integrations=[FastApiIntegration(), StarletteIntegration()],
        traces_sample_rate=sample_rate,
        environment=env_name,
        release=release,
        send_default_pii=False,   # we never want user PII in Sentry
        attach_stacktrace=True,
    )
    _log.info("Sentry initialised (env=%s, release=%s)", env_name, release)
    return True
