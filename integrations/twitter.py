from __future__ import annotations

from typing import Any

import tweepy


class TwitterClient:
    """Thin wrapper around Tweepy for posting tweets and replying."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        access_token: str,
        access_token_secret: str,
    ) -> None:
        self._client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
        )

    def post_tweet(self, text: str) -> dict[str, Any]:
        """Post a tweet. Returns the tweet id and text."""
        response = self._client.create_tweet(text=text)
        data = response.data or {}
        return {"id": str(data.get("id", "")), "text": data.get("text", text)}

    def reply_to_tweet(self, text: str, reply_to_tweet_id: str) -> dict[str, Any]:
        """Reply to an existing tweet. Used for @threadreaderapp unroll."""
        response = self._client.create_tweet(
            text=text,
            in_reply_to_tweet_id=reply_to_tweet_id,
        )
        data = response.data or {}
        return {"id": str(data.get("id", "")), "text": data.get("text", text)}

    def get_me(self) -> dict[str, Any]:
        """Get the authenticated user's ID and username."""
        response = self._client.get_me(user_auth=True)
        data = response.data
        if not data:
            raise RuntimeError("Could not retrieve authenticated user info")
        return {"id": str(data.id), "username": data.username, "name": data.name}

    def get_user(self, username: str) -> dict[str, Any]:
        """Look up a user by username. Returns id and username."""
        response = self._client.get_user(username=username, user_auth=True)
        data = response.data
        if not data:
            raise ValueError(f"User not found: {username}")
        return {"id": str(data.id), "username": data.username, "name": data.name}

    def follow_user(self, username: str) -> dict[str, Any]:
        """Follow a user by username."""
        target = self.get_user(username)
        self._client.follow_user(target_user_id=target["id"])
        return {"success": True, "user_id": target["id"], "username": target["username"]}

    def unfollow_user(self, username: str) -> dict[str, Any]:
        """Unfollow a user by username."""
        target = self.get_user(username)
        self._client.unfollow_user(target_user_id=target["id"])
        return {"success": True, "user_id": target["id"], "username": target["username"]}

    def get_following(self, max_results: int = 100) -> list[dict[str, Any]]:
        """Get list of users the authenticated account is following."""
        me = self.get_me()
        response = self._client.get_users_following(id=me["id"], max_results=max_results, user_auth=True)
        users = response.data or []
        return [{"id": str(u.id), "username": u.username, "name": u.name} for u in users]
