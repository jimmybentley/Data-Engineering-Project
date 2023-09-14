import unittest
import requests

class TestSpotifyAPI(unittest.TestCase):
    # authorization works
    def test_authorize_endpoint(self):
        response = requests.get("http://localhost:8000/authorize")
        return response

if __name__ == "__main__":
    unittest.main()
