#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import requests_oauthlib

# Fill these in with your credentials
#these can be found on amazon account
client_id     = client_id
client_secret =  client_secret
redirect_uri  = 'https://gameonmouthguards.com/account/login?return_url=%2Faccount'
scope = ["advertising::campaign_management"]


oauth = requests_oauthlib.OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)
authorization_url, state = oauth.authorization_url('https://www.amazon.com/ap/oa')

print("Navigate to the following URL in your browser, then auth with Amazon Advertising")
print("Once redirected, paste the resulting URL into the prompt below.")
print("")
print(authorization_url)
print("")

authorization_response = input('Redirect url: ').strip()

token = oauth.fetch_token(
    'https://api.amazon.com/auth/o2/token',
    authorization_response=authorization_response,
    client_secret=client_secret)

access_token = token['access_token']
refresh_token = token['refresh_token']

print("Refresh token:")
print(refresh_token)
