import subprocess
import datetime
import os
from discord_notify import send_discord_notification


def send_discord_notification_if_configured(webhook_config, message):
    """
    Discord Webhook設定がある場合に通知を送信するヘルパー関数

    Args:
        webhook_config (dict): Discord Webhook Config（辞書形式）
        message (str): 送信するメッセージ
    """
    if webhook_config and 'webhook_url' in webhook_config:
        send_discord_notification(
            webhook_config['webhook_url'],
            message,
            username=webhook_config.get('username'),
            avatar_url=webhook_config.get('avatar_url')
        )


def backup_postgres_with_xz(db_name, user, password, host="localhost", port="5432", output_dir="./backups",
                            webhook_config=None):
    """
    PostgreSQLデータベースのバックアップを作成して.xz形式で圧縮する関数

    Args:
        db_name (str): バックアップ対象のデータベース名
        user (str): PostgreSQLユーザー名
        password (str): PostgreSQLユーザーパスワード
        host (str): PostgreSQLのホスト
        port (str): PostgreSQLのポート
        output_dir (str): バックアップファイルを保存するディレクトリ
        webhook_config (dict): Discord Webhook Config（辞書形式）
    """
    # 出力ディレクトリを確認・作成
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 現在の日付を取得してファイル名を作成
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(output_dir, f"{db_name}_backup_{timestamp}.sql.xz")

    # 環境変数でパスワードを提供
    os.environ['PGPASSWORD'] = password

    message = (
        f"📦 PostgreSQLバックアップを開始しました。"
    )
    send_discord_notification_if_configured(webhook_config, message)
    try:
        # バックアップと圧縮処理を連続して実行
        with open(backup_file, "wb") as f:
            pg_dump = subprocess.Popen(
                [
                    "pg_dump",
                    "-h", host,
                    "-p", str(port),  # 数値型を文字列に変換
                    "-U", user,
                    db_name
                ],
                stdout=subprocess.PIPE
            )
            subprocess.run(
                ["xz", "-5"],  # 圧縮率を最大 (-9) に設定
                stdin=pg_dump.stdout,
                stdout=f,
                check=True
            )

        print(f"バックアップ成功: {backup_file}")
        message = (
            f"🎉 PostgreSQLバックアップに成功！\n"
            f"**データベース名:** `{db_name}`\n"
            f"**保存場所:** `{backup_file}`"
        )
        send_discord_notification_if_configured(webhook_config, message)
    except subprocess.CalledProcessError as e:
        error_message = f"⚠️ バックアップ中にエラーが発生しました: {e}"
        print(error_message)
        send_discord_notification_if_configured(webhook_config, error_message)
    finally:
        # 環境変数のパスワードを削除
        del os.environ['PGPASSWORD']
