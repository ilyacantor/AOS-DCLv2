"""Resilient ground-truth reads for the e2e suite.

The Context/monitoring e2e specs fetch ground truth from /api/dcl/snapshots at
test time (read-only, allowed by the Playwright Acceptance rules). That endpoint
is bounded (#27/#35/#56) but, under the full-suite load convoy through the
shared dev 6543 pooler, it occasionally returns a transient empty body / non-200
(#88) — a single such blip used to fail a spec with a raw
`httpx.get(...).json()` JSONDecodeError, breaking B14 twice-identical.

This wrapper retries the GROUND-TRUTH READ ONLY (not the feature under test) and
fails LOUD with the last status + body excerpt after exhaustion. It does not
weaken any assertion: the expected values still come from the live endpoint at
test time — they are just fetched robustly against a known-transient blip. This
is the read-side half of the #60 condition-wait discipline.
"""

import time

import httpx


def get_snapshots(backend_url: str, *, attempts: int = 4, backoff_s: float = 0.5):
    """Return the live `snapshots` list, retrying transient blips. Raises with
    a diagnostic message (status + body excerpt) after `attempts` failures."""
    url = f"{backend_url}/api/dcl/snapshots"
    last = ""
    for i in range(attempts):
        try:
            resp = httpx.get(url, timeout=30.0)
            if resp.status_code == 200:
                body = resp.json()
                snaps = body.get("snapshots")
                if snaps is not None:
                    return snaps
                last = f"200 but no 'snapshots' key: {str(body)[:200]}"
            else:
                last = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except (httpx.HTTPError, ValueError) as e:  # ValueError covers JSON decode
            last = f"{type(e).__name__}: {e}"
        if i < attempts - 1:
            time.sleep(backoff_s * (i + 1))
    raise AssertionError(
        f"GET {url} did not return a usable snapshots payload after "
        f"{attempts} attempts (dev pooler transient, #88). Last: {last}"
    )
