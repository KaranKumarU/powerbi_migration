import json
import os
import time

import requests
from dotenv import load_dotenv
from google.cloud import secretmanager, storage

load_dotenv()

HTTP_OK = 200
workspace_name = os.getenv("WORKSPACE_NAME")
ignore_reports = os.getenv("IGNORE_REPORTS")
PROJECT_ID = os.getenv("PROJECT_ID")


# Secret
def get_secret(secret_name: str):
    secret_manager_client = secretmanager.SecretManagerServiceClient()
    request = {"name": f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"}
    response = secret_manager_client.access_secret_version(request)
    secret_string = response.payload.data.decode("UTF-8")
    return secret_string


# Access token
def get_access_token(tenant_info) -> str:
    tenant_id = tenant_info.get("tenant_id")
    client_id = tenant_info.get("client_id")
    client_secret = tenant_info.get("client_secret")
    username = tenant_info.get("username")
    password = tenant_info.get("password")

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    body = {
        "grant_type": "password",
        "resource": "https://analysis.windows.net/powerbi/api",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
    }
    response = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        headers=headers,
        data=body,
    )

    if response.status_code != HTTP_OK:
        print("Failed to get access token")
        print("Status Code:", response.status_code)
        print("Response Content:", response.content.decode())

    response.raise_for_status()
    return response.json().get("access_token")


# Workspace
def get_workspace(access_token, workspace_name):
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(
        "https://api.powerbi.com/v1.0/myorg/groups", headers=headers
    )
    response.raise_for_status()
    ws = [
        result
        for result in response.json()["value"]
        if result["name"] == workspace_name
    ]
    if len(ws) > 0:
        return ws[0].get("id")


# Report
def get_reports(access_token, workspace_id: str) -> list:
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports",
        headers=headers,
    )

    if response.status_code == HTTP_OK:
        ws = [
            {"id": result.get("id"), "name": result.get("name")}
            for result in response.json().get("value")
        ]
        return ws
    else:
        print(
            f"Error {response.status_code} -- Something went wrong when trying to retrieve the list of reports in the workspace {workspace_id}"
        )


# Export report
def export_report(access_token, group_id, report_id):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{report_id}/Export"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.content


# Import report
def import_report(access_token, group_id, report_name, report_content):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/imports?datasetDisplayName={report_name}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "multipart/form-data",
    }
    file = {"file": report_content}
    response = requests.post(url, headers=headers, files=file)
    if response.status_code != 202:
        print(response.json())
        return None
    return response.json()


# GCS Storge
def upload_report_to_gcs(report_content, report_name):
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    BUCKET_DESTINATION_DIRECTORY = os.getenv("BUCKET_DESTINATION_DIRECTORY")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    destination_blob_name = f"{BUCKET_DESTINATION_DIRECTORY}/{report_name}.pbix"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(report_content)
    print(f"Report {report_name} uploaded to {destination_blob_name}.")


# ------------------------------------------------------------------------------------------------------------------------------------------------------------------->
def main():

    secrets = json.loads(get_secret("vgm-secrets-powerbi-migration-tenant-creds"))
    tenant_vgm_info = secrets.get("tenant_vgm_info", {})
    tenant_ft_info = secrets.get("tenant_ft_info", {})

    # Get access token for Tenant VGM
    token_vgm = get_access_token(tenant_vgm_info)
    source_workspace_id = get_workspace(token_vgm, workspace_name)
    source_reports_dict = get_reports(token_vgm, source_workspace_id)
    print(
        f"\nNumber of reports in the VGM '{workspace_name}' workspace : {len(source_reports_dict)}"
    )

    # Get access token for Tenant Fasttrack
    token_ft = get_access_token(tenant_ft_info)
    target_workspace_id = get_workspace(token_ft, workspace_name)
    dst_reports_dict = get_reports(token_ft, target_workspace_id)
    dst_reports_dict_by_name = {
        report.get("name"): report for report in dst_reports_dict
    }
    print(
        f"Number of reports in the FastTrack '{workspace_name}' workspace : {len(dst_reports_dict)}"
    )

    for report in source_reports_dict:
        report_name = report.get("name")
        if report_name not in ignore_reports:
            # Check if the report already exists in the destination
            if report_name in dst_reports_dict_by_name:
                print(
                    f"\nINFO --Report {report_name} already exists in the destination. Skipping migration."
                )
                continue

            start_time = time.time()
            print("\nINFO --Report Migration Started for: ", report.get("name"))
            print(report.get("id"))
            # Export report from Tenant VGM
            report_content = export_report(
                token_vgm, source_workspace_id, report.get("id")
            )
            if report_content:
                upload_report_to_gcs(report_content, report_name)
                import_response = import_report(
                    token_ft, target_workspace_id, report.get("name"), report_content
                )
                print(
                    "INFO --Report Migration Successful for Report ID: ",
                    import_response,
                )
                dst_reports_dict.append(report)
            else:
                print("ERROR -- Report Migration Failed for:", report.get("name"))

            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Migration took  {execution_time:.2f} seconds!")


if __name__ == "__main__":
    main()
