"""Load Kraken API keys from a GPG-encrypted file.

GPG passphrase comes from an age-encrypted file. Paths are set by the
systemd unit via TRADING_PASSPHRASE_AGE_FILE and AGE_IDENTITY_FILE.
"""

import json
import os
import subprocess
from pathlib import Path

from logging_config import get_logger


class KeyLoader:
    """Loads Kraken API keys from a GPG-encrypted file."""

    def __init__(self, config):
        if 'keys_path' not in config:
            raise RuntimeError("Missing 'keys_path' in config")
        self.key_file_path = config['keys_path']
        self.logger = get_logger('KeyLoader')

    def load(self):
        """Decrypt the GPG file and return (api_key, secret). Raises on failure."""
        if not self.key_file_path.endswith('.gpg'):
            raise RuntimeError(f"Not a .gpg file: {self.key_file_path}")

        passphrase = self._get_passphrase()
        keys = self._decrypt_gpg(self.key_file_path, passphrase)

        api_key = keys.get('name')
        secret = keys.get('privateKey')
        if not api_key or not secret:
            raise RuntimeError("Decrypted file missing 'name' or 'privateKey'")

        return api_key, secret

    def _get_passphrase(self):
        """Decrypt the age-encrypted passphrase file."""
        age_file = os.environ['TRADING_PASSPHRASE_AGE_FILE']
        age_identity = os.environ['AGE_IDENTITY_FILE']

        if not Path(age_file).exists():
            raise RuntimeError(f"Age file not found: {age_file}")
        if not Path(age_identity).exists():
            raise RuntimeError(f"Age identity file not found: {age_identity}")

        result = subprocess.run(
            ['age', '-d', '-i', age_identity, age_file],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"age decryption failed: {result.stderr.strip()}"
            )
        passphrase = result.stdout.strip()
        if not passphrase:
            raise RuntimeError("age decrypted to empty passphrase")

        self.logger.info("Using GPG passphrase from age file")
        return passphrase

    def _decrypt_gpg(self, file_path, passphrase):
        """Decrypt the GPG file and return the parsed JSON. Raises on failure."""
        self.logger.info(f"Decrypting {file_path}")
        result = subprocess.run(
            ['gpg', '--quiet', '--batch',
             '--passphrase-fd', '0', '--decrypt', file_path],
            input=passphrase, capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"GPG decryption failed: {result.stderr.strip()}")
        return json.loads(result.stdout)
