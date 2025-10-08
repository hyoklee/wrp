import globus_sdk

# Set this.
CLIENT_ID = "9f60b3dc-f895-4c68-8961-fd431399f523"

client = globus_sdk.NativeAppAuthClient(CLIENT_ID)
client.oauth2_start_flow()
authorize_url = client.oauth2_get_authorize_url()
print(f"Please go to this URL and login:\n\n{authorize_url}\n")

auth_code = input("Please enter the code you get after login here: ").strip()
token_response = client.oauth2_exchange_code_for_tokens(auth_code)

transfer = token_response.by_resource_server["transfer.api.globus.org"]
transfer_token = transfer["access_token"]
print(transfer_token)
