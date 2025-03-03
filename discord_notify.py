import requests


def send_discord_notification(webhook_url, message, username=None, avatar_url=None):
    """
    Discord Webhookを使用して通知を送る関数

    Args:
        webhook_url (str): DiscordのWebhook URL
        message (str): 送信するメッセージ内容
        username (str, optional): 表示名 (デフォルト: None)
        avatar_url (str, optional): アイコンURL (デフォルト: None)
    """
    # Discord Webhookのデータ構築
    data = {
        "content": message,
    }
    if username:
        data["username"] = username
    if avatar_url:
        data["avatar_url"] = avatar_url

    try:
        response = requests.post(webhook_url, json=data)
        response.raise_for_status()  # エラー時に例外を送出
        print("Discord通知が正常に送信されました！")
    except requests.exceptions.RequestException as e:
        print(f"Discord通知の送信中にエラーが発生しました: {e}")
