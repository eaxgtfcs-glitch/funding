"""
Utility script: encrypts exchange API keys from .env and prints the result to stdout.

Usage:
    python scripts/encrypt_keys.py --passphrase "my secret phrase"
    # or interactively:
    python scripts/encrypt_keys.py

Copy the printed output into your .env file.
"""
import argparse
import sys
from base64 import b64encode
from hashlib import pbkdf2_hmac

from dotenv import dotenv_values

_SALT = b"margin_monitor_salt"
_ITERATIONS = 100_000
_KEY_LEN = 32
_IV_LEN = 16

_SECRET_SUFFIXES = (
    "_API_KEY",
    "_API_SECRET",
    "_PASSPHRASE",
    "_SECRET_KEY",
    "_ZK_SEEDS",
    "_PRIVATE_KEY",
    "_ACCOUNT_ADDRESS",
    "_WALLET_ADDRESS",
    "_API_KEY_INDEX",
    "_ACCOUNT_INDEX",
)


def _derive_key_iv(passphrase: str) -> tuple[str, str]:
    derived = pbkdf2_hmac(
        "sha256",
        passphrase.encode("utf-8"),
        _SALT,
        _ITERATIONS,
        dklen=_KEY_LEN + _IV_LEN,
    )
    key_b64 = b64encode(derived[:_KEY_LEN]).decode()
    iv_b64 = b64encode(derived[_KEY_LEN:]).decode()
    return key_b64, iv_b64


def _should_encrypt(name: str) -> bool:
    upper = name.upper()
    return any(upper.endswith(s) for s in _SECRET_SUFFIXES)


def main() -> None:
    parser = argparse.ArgumentParser(description="Encrypt .env API keys")
    parser.add_argument("--passphrase", help="Passphrase for key derivation")
    args = parser.parse_args()

    if args.passphrase:
        passphrase = args.passphrase
    else:
        passphrase = input("Enter passphrase: ")

    if not passphrase:
        print("ERROR: passphrase must not be empty", file=sys.stderr)
        sys.exit(1)

    key_b64, iv_b64 = _derive_key_iv(passphrase)

    # Import here so the script can run from repo root without installing the package
    sys.path.insert(0, "../../scripts")
    from app.helper.decoder import encrypt_str

    env_vars = dotenv_values("../../.env")

    for name, value in env_vars.items():
        if value is None:
            value = ""
        if _should_encrypt(name):
            encrypted = encrypt_str(value, key_b64, iv_b64)
            print(f"{name}_ENC={encrypted}")
        else:
            print(f"{name}={value}")


if __name__ == "__main__":
    main()
