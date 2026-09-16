#!/usr/bin/env python3
"""Mint an engine-signed JWT — the stand-in for what the VS adapter would do
in-process with ctx.current_user().

  mint-jwt.py --sub alice --aud lakekeeper [--iss ...] [--claim k=v] [--ttl 300]

Prints the compact JWT on stdout. --decode prints header+claims of an existing
token instead (signature redacted).
"""
import argparse, base64, json, sys, time, uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
DEFAULT_KEY = HERE / "keys" / "engine-signing-key.pem"


def decode(token):
    h, p, _sig = token.split(".")

    def seg(s):
        return json.loads(base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)))

    return {"header": seg(h), "claims": seg(p), "signature": "<redacted>"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--decode", metavar="TOKEN")
    ap.add_argument("--sub")
    ap.add_argument("--aud", default="lakekeeper")
    ap.add_argument("--iss", default="http://engine-issuer:8090")
    ap.add_argument("--azp", default="lakehouse-engine")
    ap.add_argument("--scope", default="catalog")
    ap.add_argument("--ttl", type=int, default=300)
    ap.add_argument("--kid", default="lakehouse-engine-1")
    ap.add_argument("--key", default=str(DEFAULT_KEY))
    ap.add_argument("--claim", action="append", default=[],
                    help="extra claim k=v (repeatable); v is parsed as JSON when possible")
    ap.add_argument("--omit", action="append", default=[],
                    help="claim name to leave out (for probing which claims are mandatory)")
    args = ap.parse_args()

    if args.decode:
        json.dump(decode(args.decode), sys.stdout, indent=2)
        print()
        return

    if not args.sub:
        ap.error("--sub is required when minting")

    import jwt  # PyJWT

    now = int(time.time())
    claims = {
        "iss": args.iss,
        "sub": args.sub,
        "aud": args.aud,
        "exp": now + args.ttl,
        "iat": now,
        "nbf": now,
        "jti": str(uuid.uuid4()),
        "azp": args.azp,
        "scope": args.scope,
        "preferred_username": args.sub,
        "typ": "Bearer",
    }
    for kv in args.claim:
        k, _, v = kv.partition("=")
        try:
            claims[k] = json.loads(v)
        except json.JSONDecodeError:
            claims[k] = v
    for k in args.omit:
        claims.pop(k, None)

    key = Path(args.key).read_bytes()
    print(jwt.encode(claims, key, algorithm="RS256", headers={"kid": args.kid}))


if __name__ == "__main__":
    main()
