from enum import Enum
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Depends, Header
import httpx
from kafka import KafkaProducer
from starlette.responses import RedirectResponse

app = FastAPI()

# Enum for Spotify API response type
class ItemType(str, Enum):
    track = "track"
    episode = "episode"

# Pydantic model for a single track/episode item
class Item(BaseModel):
    name: str
    type: ItemType

# Pydantic model for the recently played tracks response
class RecentlyPlayedResponse(BaseModel):
    items: List[Item]

# Kafka producer configuration
producer_config = {
    "bootstrap_servers": "localhost:9092",  # Replace with your Kafka broker(s)
}

# Spotify application credentials
client_id = "your_client_id"
client_secret = "your_client_secret"
redirect_uri = "http://localhost:8888/callback"
scope = "user-read-recently-played"  # The scope determines the access level your application has.

@app.get("/authorize")
def authorize_spotify():
    # Redirect the user to the Spotify Accounts service for authorization
    spotify_auth_url = f"https://accounts.spotify.com/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope={scope}&response_type=code"
    return RedirectResponse(url=spotify_auth_url)

@app.get("/callback")
async def spotify_callback(
    code: str,
    request: Request,
):
    # Exchange the authorization code for an access token
    token_url = "https://accounts.spotify.com/api/token"
    data = {
        "code": code,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }
    auth = (client_id, client_secret)

    async with httpx.AsyncClient() as client:
        response = await client.post(
            token_url,
            data=data,
            auth=auth,
        )

    if response.status_code == 200:
        token_data = response.json()
        # Extract the access token from token_data
        access_token = token_data.get("access_token")
        # You can store the access token securely and use it to authenticate with the Spotify API.
        # Make sure to implement proper token management and security measures.

        # Now, you can combine data streaming to Kafka with Spotify API requests using the access token.
        recently_played_data = await get_recently_played_with_token(access_token)
        return recently_played_data
    else:
        raise HTTPException(status_code=response.status_code, detail="Failed to obtain access token")

async def get_recently_played_with_token(access_token: str):
    # Spotify API endpoint URL
    url = "https://api.spotify.com/v1/me/player/recently-played?limit=50"

    # Headers with the provided Bearer Token
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)

    if response.status_code == 200:
        recently_played_data = response.json()

        # Create a Kafka producer instance
        producer = KafkaProducer(**producer_config, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        for item in recently_played_data['items']:
            # Send each item to a Kafka topic (e.g., "spotify_recently_played")
            producer.send("spotify_recently_played", value=item.dict())

        # Close the producer to flush any remaining messages
        producer.close()

        return recently_played_data
    else:
        raise HTTPException(status_code=response.status_code, detail="Spotify API request failed")
