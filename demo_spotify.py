import requests
import base64
import datetime
from urllib.parse import urlencode
from pprint import pprint as pp


client_id = 'ab65d518ec1c4437b2493c2cbfbbfa6f'
client_secret = '263a4cfaabd44fc9a6d2471175c444f3'

# do a lookup for a token
# the token is for future requests

# we are going to use the Client Credentials Flow, see:
#   https://developer.spotify.com/documentation/general/guides/authorization-guide/#client-credentials-flow

# WE DO: login using those client id and secret key
# WE GET: access token

# to get the token:

token_url = 'https://accounts.spotify.com/api/token'
method = 'POST'

# with the POST method, we need to pass the REQUEST BODY PARAMETER, the grant_type value is indeed REQUIRED:
token_data = {
    'grant_type': 'client_credentials'
}

# the HEADER PARAMETER of the above request, must contain the 'Authorization' value, Base64 encoded string containing the client id and the client secret key
# the field must have the format " Authorization: Basic <base64 encoded client_id:client_secret> "
# egg-sample:
""" 
    >>> curl -X "POST" -H "Authorization: Basic ZjM4ZjAw...WY0MzE=" -d grant_type=client_credentials https://accounts.spotify.com/api/token
"""

client_creds = f"{client_id}:{client_secret}"
client_creds_b64 = base64.b64encode(client_creds.encode())

token_headers = {
    'Authorization': f"Basic {client_creds_b64.decode()}"
}

# now the authorization phase is complete. we can perform the actual request
r = requests.post(token_url, data=token_data, headers=token_headers)
print(r.json())
valid_request = r.status_code in range(200, 299)

if valid_request:
    token_response_data = r.json()
    now = datetime.datetime.now()
    access_token = token_response_data['access_token']
    expires_in = token_response_data['expires_in']   # time in seconds
    expires = now + datetime.timedelta(seconds=expires_in)
    did_expire = expires < now


headers  = {
    "Authorization": f"Bearer {access_token}"
}

endpoint = "https://api.spotify.com/v1/me/top/"
#data = urlencode({"q": "Time", "type": "track", "market": "ES"})
query = "artists"
#lookup_url = f"{endpoint}?{data}"
lookup_url = f"{endpoint}{query}"
print(lookup_url)
r = requests.get(lookup_url, headers=headers)
print(r.status_code)
pp(r.json())

print("TESTING GIT")







