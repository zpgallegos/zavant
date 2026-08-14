from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LIGHTDASH_TEMPLATE = (
    REPOSITORY_ROOT / "infrastructure" / "lightdash-integration-stack.yaml"
)


class LightdashInfrastructureTests(unittest.TestCase):
    def test_workgroup_enforces_results_and_scan_limit(self) -> None:
        template = LIGHTDASH_TEMPLATE.read_text()

        self.assertIn("BytesScannedCutoffPerQuery: !Ref", template)
        self.assertIn("EnforceWorkGroupConfiguration: true", template)
        self.assertIn("PublishCloudWatchMetricsEnabled: true", template)
        self.assertIn("EncryptionOption: SSE_S3", template)

    def test_query_results_are_private_encrypted_and_expiring(self) -> None:
        template = LIGHTDASH_TEMPLATE.read_text()

        self.assertIn("Id: ExpireQueryResults", template)
        self.assertIn("SSEAlgorithm: AES256", template)
        self.assertIn("BlockPublicAcls: true", template)
        self.assertIn("RestrictPublicBuckets: true", template)
        self.assertIn("Sid: DenyInsecureTransport", template)

    def test_warehouse_identity_cannot_read_raw_data(self) -> None:
        template = LIGHTDASH_TEMPLATE.read_text()
        data_object_access = template[
            template.index("Sid: ReadDbtTableObjects") :
            template.index("Sid: UseQueryResultsBucket")
        ]

        self.assertIn(
            "${DataPrefix}/transformation/dbt/${EnvironmentName}/tables/*",
            template,
        )
        self.assertNotIn("${DataPrefix}/raw", template)
        self.assertNotIn("s3:PutObject", data_object_access)

    def test_warehouse_identity_can_read_athena_table_metadata(self) -> None:
        template = LIGHTDASH_TEMPLATE.read_text()

        self.assertIn("Action: athena:GetTableMetadata", template)
        self.assertIn(
            "arn:${AWS::Partition}:athena:${AWS::Region}:"
            "${AWS::AccountId}:datacatalog/AwsDataCatalog",
            template,
        )

    def test_access_key_is_not_managed_by_cloudformation(self) -> None:
        template = LIGHTDASH_TEMPLATE.read_text()

        self.assertIn("Type: AWS::IAM::User", template)
        self.assertNotIn("AWS::IAM::AccessKey", template)


if __name__ == "__main__":
    unittest.main()
