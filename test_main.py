import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json
import io

# Import the functions from your script
from main import (
    get_secret,
    get_access_token,
    get_workspace,
    get_reports,
    export_report,
    import_report,
    upload_report_to_gcs,
    main,
)


class TestPowerBIMigration(unittest.TestCase):

    @patch("powerbi_migration.secretmanager.SecretManagerServiceClient")
    def test_get_secret(self, mock_secret_manager_client):
        mock_client_instance = mock_secret_manager_client.return_value
        mock_access_secret_version = mock_client_instance.access_secret_version
        mock_access_secret_version.return_value.payload.data.decode.return_value = (
            '{"key": "value"}'
        )

        secret = get_secret("fake-secret-name")
        self.assertEqual(secret, '{"key": "value"}')

    @patch("powerbi_migration.requests.post")
    def test_get_access_token(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"access_token": "fake_token"}

        tenant_info = {
            "tenant_id": "fake_tenant_id",
            "client_id": "fake_client_id",
            "client_secret": "fake_client_secret",
            "username": "fake_username",
            "password": "fake_password",
        }

        token = get_access_token(tenant_info)
        self.assertEqual(token, "fake_token")

    @patch("powerbi_migration.requests.get")
    def test_get_workspace(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "value": [{"id": "fake_workspace_id", "name": "fake_workspace"}]
        }

        token = "fake_token"
        workspace_id = get_workspace(token, "fake_workspace")
        self.assertEqual(workspace_id, "fake_workspace_id")

    @patch("powerbi_migration.requests.get")
    def test_get_reports(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "value": [{"id": "fake_report_id", "name": "fake_report"}]
        }

        token = "fake_token"
        workspace_id = "fake_workspace_id"
        reports = get_reports(token, workspace_id)
        self.assertEqual(reports, [{"id": "fake_report_id", "name": "fake_report"}])

    @patch("powerbi_migration.requests.get")
    def test_export_report(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b"fake_report_content"

        token = "fake_token"
        group_id = "fake_group_id"
        report_id = "fake_report_id"

        with patch("builtins.open", mock_open()) as mock_file:
            try:
                content = export_report(token, group_id, report_id)
                self.assertEqual(content, b"fake_report_content")
            except Exception as e:
                self.fail(f"export_report raised an exception: {e}")

    @patch("powerbi_migration.requests.post")
    def test_import_report(self, mock_post):
        mock_post.return_value.status_code = 202
        mock_post.return_value.json.return_value = {"import_id": "fake_import_id"}

        token = "fake_token"
        group_id = "fake_group_id"
        report_name = "fake_report"
        report_content = b"fake_report_content"

        result = import_report(token, group_id, report_name, report_content)
        self.assertEqual(result, {"import_id": "fake_import_id"})

    @patch("powerbi_migration.storage.Client")
    def test_upload_report_to_gcs(self, mock_storage_client):
        mock_client_instance = mock_storage_client.return_value
        mock_bucket = mock_client_instance.bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        report_content = b"fake_report_content"
        report_name = "fake_report"

        os.environ["BUCKET_NAME"] = "fake_bucket"
        os.environ["BUCKET_DESTINATION_DIRECTORY"] = "fake_directory"

        upload_report_to_gcs(report_content, report_name)
        mock_blob.upload_from_string.assert_called_once_with(report_content)
        print(
            f"Report {report_name} uploaded to {os.getenv('BUCKET_DESTINATION_DIRECTORY')}/{report_name}.pbix."
        )

    @patch("powerbi_migration.get_secret")
    @patch("powerbi_migration.get_access_token")
    @patch("powerbi_migration.get_workspace")
    @patch("powerbi_migration.get_reports")
    @patch("powerbi_migration.export_report")
    @patch("powerbi_migration.upload_report_to_gcs")
    @patch("powerbi_migration.import_report")
    @patch.dict(
        os.environ,
        {
            "WORKSPACE_NAME": "fake_workspace",
            "IGNORE_REPORTS": '["ignore_report_1", "ignore_report_2"]',
            "PROJECT_ID": "fake_project_id",
        },
    )
    def test_main(
        self,
        mock_import_report,
        mock_upload_report_to_gcs,
        mock_export_report,
        mock_get_reports,
        mock_get_workspace,
        mock_get_access_token,
        mock_get_secret,
    ):
        # Setup the mocks
        mock_get_secret.return_value = json.dumps(
            {
                "tenant_vgm_info": {
                    "tenant_id": "fake_tenant_id",
                    "client_id": "fake_client_id",
                    "client_secret": "fake_client_secret",
                    "username": "fake_username",
                    "password": "fake_password",
                },
                "tenant_ft_info": {
                    "tenant_id": "fake_tenant_id",
                    "client_id": "fake_client_id",
                    "client_secret": "fake_client_secret",
                    "username": "fake_username",
                    "password": "fake_password",
                },
            }
        )

        mock_get_access_token.side_effect = ["fake_token_vgm", "fake_token_ft"]
        mock_get_workspace.side_effect = ["source_workspace_id", "target_workspace_id"]
        mock_get_reports.side_effect = [[{"id": "report_id_1", "name": "Report 1"}], []]
        mock_export_report.return_value = b"fake_report_content"
        mock_import_report.return_value = {"import_id": "fake_import_id"}

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            main()
            output = mock_stdout.getvalue()

        self.assertIn("INFO --Report Migration Started for:  Report 1", output)
        self.assertIn(
            "INFO --Report Migration Successful for Report ID:  {'import_id': 'fake_import_id'}",
            output,
        )
        self.assertIn("Migration took ", output)
        mock_upload_report_to_gcs.assert_called_once_with(
            b"fake_report_content", "Report 1"
        )


if __name__ == "__main__":
    unittest.main()
