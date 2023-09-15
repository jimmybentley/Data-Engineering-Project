from fastapi import FastAPI, HTTPException, Request
from httpx import AsyncClient
from starlette.responses import RedirectResponse
import json
from confluent_kafka import Producer


app = FastAPI()

# Load Spotify application credentials from your config file
with open("config/config.json", "r") as config_file:
    config_data = json.load(config_file)

CLIENT_ID = config_data["spotify"]["client_id"]
CLIENT_SECRET = config_data["spotify"]["client_secret"]
REDIRECT_URI = "http://localhost:8000/callback" 
SCOPE = "user-read-recently-played"  # The scope determines the access level your application has.

# Kafka producer configuration
producer_config = {
    "bootstrap_servers": "localhost:9092",  # Replace with your Kafka broker(s)
}

@app.get("/login")
def login_spotify():
    STATE = generate_random_string(16)  # You can define your generate_random_string function
    #SCOPE = "user-read-private user-read-email"

    # Redirect the user to the Spotify Accounts service for authorization
    spotify_auth_url = f"https://accounts.spotify.com/authorize?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&scope={SCOPE}&response_type=code&state={STATE}"
    return RedirectResponse(url=spotify_auth_url)

# For generating a random string to use for the state parameter
def generate_random_string(length):
    import random
    import string
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

@app.get("/callback")
async def spotify_callback(
    code: str,
    state: str,
    request: Request,
):
    # Exchange the authorization code for an access token
    token_url = "https://accounts.spotify.com/api/token"
    data = {
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    auth = (CLIENT_ID, CLIENT_SECRET)

    async with AsyncClient() as client:
        response = await client.post(
            token_url,
            data=data,
            auth=auth,
        )

    if response.status_code == 200:
        print("Successfully obtained access token")
        token_data = response.json()
        # Extract the access token from token_data
        access_token = token_data.get("access_token")

        # Now, you can combine data streaming to Kafka with Spotify API requests using the access token.
        recently_played_data = await get_recently_played_with_token(access_token)
        return recently_played_data
    else:
        raise HTTPException(status_code=response.status_code, detail="Failed to obtain access token")


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


async def get_recently_played_with_token(access_token: str):
    # Spotify API endpoint URL
    url = "https://api.spotify.com/v1/me/player/recently-played?limit=50"

    # Headers with the provided Bearer Token
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    async with AsyncClient() as client:
        response = await client.get(url, headers=headers)
    # response = requests.get(url, headers=headers)

    if response.status_code == 200:
        recently_played_data = response.json()
        #print(recently_played_data)
        #Create a Kafka producer instance
        producer = Producer(read_ccloud_config("config/client.properties"))

        for item in recently_played_data['items']:
            # Send each item to a Kafka topic (e.g., "spotify_recently_played")
            producer.produce("spotify_recently_played", value=item)

        # Close the producer to flush any remaining messages
        producer.close()

        return recently_played_data
    else:
        raise HTTPException(status_code=response.status_code, detail="Spotify API request failed")