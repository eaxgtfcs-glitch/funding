"""
Utility script: decrypts exchange API keys from .env and prints the result to stdout.

Usage:
    python app/helper/decrypt_keys.py --passphrase "my secret phrase"
    # or interactively:
    python app/helper/decrypt_keys.py

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


def main() -> None:
    parser = argparse.ArgumentParser(description="Decrypt .env API keys")
    parser.add_argument("--passphrase", help="Passphrase for key derivation")
    parser.add_argument("--env-file", default="../../.env", help="Path to .env file with encrypted variables")
    args = parser.parse_args()

    if args.passphrase:
        passphrase = args.passphrase
    else:
        passphrase = input("Enter passphrase: ")

    if not passphrase:
        print("ERROR: passphrase must not be empty", file=sys.stderr)
        sys.exit(1)

    key_b64, iv_b64 = _derive_key_iv(passphrase)

    from app.helper.decoder import decrypt_str

    env_vars = dotenv_values(args.env_file)

    for name, value in env_vars.items():
        if value is None:
            value = ""
        if name.endswith("_ENC"):
            decrypted = decrypt_str(value, key_b64, iv_b64)
            print(f"{name[:-4]}={decrypted}")
        else:
            print(f"{name}={value}")


if __name__ == "__main__":
    main()
