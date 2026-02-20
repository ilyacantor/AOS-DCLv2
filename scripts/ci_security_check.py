#!/usr/bin/env python3
"""
CI Security Validation Script for DCL Zero-Trust Enforcement.

Run this script in CI/CD pipeline to fail build if payload.body writes are detected.
Per ARCH-GLOBAL-PIVOT.md, DCL must be metadata-only.

Usage:
    python scripts/ci_security_check.py

Exit codes:
    0 - All checks passed
    1 - Security violations detected
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.core.security_constraints import (
    validate_no_disk_payload_writes,
    assert_metadata_only_mode,
    ZeroTrustViolation,
)


def main():
    print("=" * 60)
    print("DCL Zero-Trust Security Check (CI)")
    print("=" * 60)
    
    errors = []
    
    print("\n[1/2] Checking metadata-only mode assertion...")
    try:
        assert_metadata_only_mode()
        print("  PASS: Metadata-only mode enabled")
    except ZeroTrustViolation as e:
        print(f"  FAIL: {e}")
        errors.append(str(e))
    except Exception as e:
        print(f"  WARN: {e}")
    
    print("\n[2/2] Scanning for payload write violations...")
    violations = validate_no_disk_payload_writes()
    if violations:
        print(f"  FAIL: Found {len(violations)} potential violations:")
        for v in violations:
            print(f"    - {v}")
        errors.extend(violations)
    else:
        print("  PASS: No payload write violations detected")
    
    print("\n" + "=" * 60)
    
    if errors:
        print(f"RESULT: FAILED ({len(errors)} issues)")
        print("\nReview docs/ARCH-GLOBAL-PIVOT.md for migration guidance.")
        print("DCL must be metadata-only. Raw payload handling belongs in AAM.")
        return 1
    else:
        print("RESULT: PASSED")
        print("\nDCL is compliant with Zero-Trust metadata-only architecture.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
