from __future__ import annotations

import json
import os
import sys
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..models import AnomalyEvent

_TIMEOUT = 5   # seconds per POST — applies inside the background thread


class WebhookDispatcher:
    """POSTs AnomalyEvent alerts to a Discord or Slack incoming webhook.

    Fire-and-forget: dispatch() spawns a daemon thread and returns immediately
    so the main math engine is never blocked by network I/O, even if the
    Discord API is slow or multiple channels fire simultaneously.

    URL is read from the WEBHOOK_URL environment variable by default.
    Pass url="" or omit the env var to run in silent/no-op mode (backtest
    and test environments where no real channel is wired up).
    """

    def __init__(self, url: str = "") -> None:
        self._url = url or os.environ.get("WEBHOOK_URL", "")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def dispatch(self, event: AnomalyEvent, keywords: Optional[List[str]] = None) -> bool:
        """Spawn a background POST. Returns True if a thread was launched, False if no-op."""
        if not self._url:
            return False
        payload = self._build_payload(event, keywords or [])
        threading.Thread(
            target=self._execute_post,
            args=(payload,),
            daemon=True,
        ).start()
        return True

    def _execute_post(self, payload: Dict[str, Any]) -> None:
        """Runs in background thread. Logs a specific message per failure type; never raises."""
        try:
            req = urllib.request.Request(
                self._url,
                data=json.dumps(payload).encode(),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "DiscordBot (SurgeDetectionEngine, 1.0)",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                if resp.status not in (200, 204):
                    print(f"  [webhook] unexpected status {resp.status}", file=sys.stderr)
        except urllib.error.HTTPError as exc:          # must precede URLError (subclass)
            if exc.code == 429:
                print("  [webhook] rate-limited (HTTP 429) — alert dropped", file=sys.stderr)
            else:
                print(f"  [webhook] HTTP {exc.code} {exc.reason}", file=sys.stderr)
        except urllib.error.URLError as exc:           # DNS failure, connection refused, SSL
            print(f"  [webhook] network error — {exc.reason}", file=sys.stderr)
        except TimeoutError:                           # socket.timeout / read timeout
            print(f"  [webhook] timed out after {_TIMEOUT}s — alert dropped", file=sys.stderr)
        except Exception as exc:                       # encoding error or anything unexpected
            print(f"  [webhook] unexpected {type(exc).__name__}: {exc}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Payload builders
    # ------------------------------------------------------------------

    def _build_payload(self, event: AnomalyEvent, keywords: List[str]) -> Dict[str, Any]:
        dt     = datetime.fromtimestamp(event.window_end, tz=timezone.utc).strftime("%b %d %H:%M UTC")
        kw_str = ", ".join(keywords[:8]) if keywords else "—"
        if "discord.com" in self._url:
            return self._discord(event, dt, kw_str)
        if "hooks.slack.com" in self._url:
            return self._slack(event, dt, kw_str)
        return {"text": f"SURGE | {event.subreddit} | {dt} | count={event.count} z={event.z_score:.2f} | {kw_str}"}

    @staticmethod
    def _discord(event: AnomalyEvent, dt: str, kw_str: str) -> Dict[str, Any]:
        return {
            "embeds": [{
                "title":  f"Surge Detected — {event.subreddit}",
                "color":  0xE53935,
                "fields": [
                    {"name": "Channel",  "value": event.subreddit,           "inline": True},
                    {"name": "Time",     "value": dt,                         "inline": True},
                    {"name": "Count",    "value": str(event.count),           "inline": True},
                    {"name": "Z-Score",  "value": f"{event.z_score:.2f}",    "inline": True},
                    {"name": "Keywords", "value": kw_str,                     "inline": False},
                ],
                "footer": {"text": "Surge Detection Engine"},
            }]
        }

    @staticmethod
    def _slack(event: AnomalyEvent, dt: str, kw_str: str) -> Dict[str, Any]:
        return {
            "text": f"*Surge Detected — {event.subreddit}*",
            "attachments": [{
                "color":  "#E53935",
                "fields": [
                    {"title": "Channel",  "value": event.subreddit,        "short": True},
                    {"title": "Time",     "value": dt,                      "short": True},
                    {"title": "Count",    "value": str(event.count),        "short": True},
                    {"title": "Z-Score",  "value": f"{event.z_score:.2f}", "short": True},
                    {"title": "Keywords", "value": kw_str,                  "short": False},
                ],
                "footer": "Surge Detection Engine",
            }]
        }
