import os
from base64 import b64encode
from hashlib import pbkdf2_hmac

from app.helper.decoder import decrypt_str

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


class KeyVault:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._unlocked = False

    def unlock(self, passphrase: str) -> None:
        key_b64, iv_b64 = _derive_key_iv(passphrase)
        store: dict[str, str] = {}
        for env_key, env_val in os.environ.items():
            if not env_key.endswith("_ENC"):
                continue
            original_name = env_key[:-4]  # strip _ENC
            decrypted = decrypt_str(env_val, key_b64, iv_b64)
            store[original_name] = decrypted
        self._store = store
        self._unlocked = True

    def get(self, env_var: str) -> str:
        if not self._unlocked:
            val = os.environ.get(env_var)
            if val is None:
                raise RuntimeError("KeyVault is locked — call unlock(passphrase) first")
            return val
        return self._store[env_var]

    @property
    def is_unlocked(self) -> bool:
        return self._unlocked


vault = KeyVault()
