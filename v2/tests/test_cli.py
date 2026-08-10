from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict
import unittest
from unittest.mock import patch

from zavant.cli import _backfill_storage, build_parser
from zavant.settings import Settings
from tests.fake_s3 import FakeS3Client


class FakeStsClient:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id

    def get_caller_identity(self) -> Dict[str, Any]:
        return {"Account": self.account_id}


class BackfillCliSafetyTests(unittest.TestCase):
    def settings(self, expected_account_id: str = "123456789012") -> Settings:
        return Settings(
            data_dir=Path(".local/lake"),
            mlb_api_base_url="https://statsapi.example.test",
            s3_bucket="example-bucket",
            s3_prefix="lake",
            expected_aws_account_id=expected_account_id,
        )

    def test_backfill_defaults_to_local_storage(self) -> None:
        args = build_parser().parse_args(["backfill-seasons", "2025"])

        self.assertEqual(args.storage, "local")

    def test_s3_backfill_verifies_expected_account(self) -> None:
        args = SimpleNamespace(bucket=None, storage="s3", prefix=None)
        s3_client = FakeS3Client()

        def client(service: str) -> object:
            if service == "sts":
                return FakeStsClient("123456789012")
            if service == "s3":
                return s3_client
            raise AssertionError(f"unexpected service: {service}")

        with patch(
            "zavant.cli.import_module",
            return_value=SimpleNamespace(client=client),
        ):
            storage = _backfill_storage(
                args,
                self.settings(),
                Path(".local/lake"),
            )

        self.assertIsNotNone(storage.raw_games)

    def test_s3_backfill_rejects_wrong_account(self) -> None:
        args = SimpleNamespace(bucket=None, storage="s3", prefix=None)

        with patch(
            "zavant.cli.import_module",
            return_value=SimpleNamespace(
                client=lambda service: FakeStsClient("999999999999")
            ),
        ):
            with self.assertRaisesRegex(ValueError, "refusing S3 backfill"):
                _backfill_storage(
                    args,
                    self.settings(),
                    Path(".local/lake"),
                )


if __name__ == "__main__":
    unittest.main()
