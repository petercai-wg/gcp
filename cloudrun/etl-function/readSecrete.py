from google.cloud import secretmanager

## google-cloud-secret-manager==2.24.0


def access_secret_version(project_id, secret_id, version_id="latest"):
    """
    Accesses a secret version and returns its payload.
    Service Accout need to have "Secret Manager Secret Accessor" role to access the secret.
    Returns:  str: The secret payload as a string.
    """
    try:
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version.
        # Format: projects/{project_id}/secrets/{secret_id}/versions/{version_id}
        name = client.secret_version_path(project_id, secret_id, version_id)

        # Access the secret version.
        response = client.access_secret_version(request={"name": name})

        # Decode the payload. The payload is always a bytes object.
        # The secret payload can also be a dict, list, or other data structure, depending on how it was stored.
        ## secret_dict = json.loads(response.payload.data.decode("UTF-8"))
        payload = response.payload.data.decode("UTF-8")

        return payload

    except Exception as e:
        print(f"Error accessing secret: {e}")
        return None


# --- Example Usage ---
# PROJECT_ID = "project-xxxxx"
# SECRET_ID = "postgresql_password"
# # Optional: specify a version, otherwise "latest" is used by default in the function
# # VERSION_ID = "1"

# secret_value = access_secret_version(project_id=PROJECT_ID, secret_id=SECRET_ID)
# print(f"SECRET_ID {SECRET_ID}, Secret value: {secret_value}")

print("Secret Manager access function loaded successfully.")
