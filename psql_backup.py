import os
import time
import datetime
import subprocess
from discord_notify import send_discord_notification

DELETE_AFTER_DAYS = 3  # 古いバックアップを削除する日数
NOTIFICATION_START_MESSAGE = "📦 PostgreSQLバックアップを開始しました。"


def send_discord_notification_if_configured(webhook_config, message):
    """
    Discord Webhook設定がある場合に通知を送信するヘルパー関数。

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


def delete_old_backups(directory, days, webhook_config=None):
    """
    指定されたディレクトリ内の古いファイルを削除する関数。

    Args:
        directory (str): バックアップ保存用ディレクトリ
        days (int): 削除対象となる日数
        webhook_config (dict): Discord Webhook Config（通知用）
    """
    now = time.time()  # 現在のタイムスタンプ
    deleted_files = []  # 削除対象ファイルを記録
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        # ファイルかどうか確認（ディレクトリはスキップ）
        if os.path.isfile(file_path):
            file_mtime = os.path.getmtime(file_path)  # ファイルの最終更新時刻を取得
            # 指定日数以上経過している場合は削除
            if (now - file_mtime) > (days * 86400):  # 日数を秒に変換
                os.remove(file_path)
                deleted_files.append(file_path)
                print(f"削除: {file_path}")

    # 削除したファイルについて通知を送信
    if deleted_files:
        message = (
                f"🗑️ 古いバックアップファイルを削除しました。\n削除対象:\n" +
                "\n".join(f" - `{file}`" for file in deleted_files)
        )
        send_discord_notification_if_configured(webhook_config, message)
    else:
        print("削除対象の古いファイルはありません。")


def backup_postgres_with_xz(db_name, user, password, host="localhost", port="5432", output_dir="./backups",
                            webhook_config=None):
    """
    PostgreSQLデータベースのバックアップを作成して.xz形式で圧縮する関数。

    Args:
        db_name (str): バックアップ対象のデータベース名
        user (str): PostgreSQLユーザー名
        password (str): PostgreSQLユーザーパスワード
        host (str): PostgreSQLのホスト
        port (str): PostgreSQLのポート
        output_dir (str): バックアップファイルを保存するディレクトリ
        webhook_config (dict): Discord Webhook Config（辞書形式）
    """
    # バックアップ開始通知
    send_discord_notification_if_configured(webhook_config, NOTIFICATION_START_MESSAGE)

    # 出力ディレクトリを確認・作成
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 古いバックアップを削除
    delete_old_backups(output_dir, DELETE_AFTER_DAYS, webhook_config)

    # 現在の日付を取得してファイル名を作成
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(output_dir, f"{db_name}_backup_{timestamp}.sql.xz")

    # 環境変数でパスワードを設定
    os.environ['PGPASSWORD'] = password

    try:
        # バックアップと圧縮処理を実行
        with open(backup_file, "wb") as f:
            pg_dump = subprocess.Popen(
                [
                    "pg_dump",
                    "-h", host,
                    "-p", str(port),  # 数値ポートを文字列に変換
                    "-U", user,
                    "-j", "4",  # 並列処理を有効化して、4スレッドを使用
                    db_name
                ],
                stdout=subprocess.PIPE
            )
            subprocess.run(
                ["xz", "-5"],  # 圧縮率を指定
                stdin=pg_dump.stdout,
                stdout=f,
                check=True
            )

        # バックアップ成功通知
        print(f"バックアップ成功: {backup_file}")
        message = (
            f"🎉 PostgreSQLバックアップに成功！\n"
            f"**データベース名:** `{db_name}`\n"
            f"**保存場所:** `{backup_file}`"
        )
        send_discord_notification_if_configured(webhook_config, message)

    except subprocess.CalledProcessError as e:
        # バックアップエラー通知
        error_message = f"⚠️ バックアップ中にエラーが発生しました: {e}"
        print(error_message)
        send_discord_notification_if_configured(webhook_config, error_message)

    finally:
        # 環境変数を消去
        del os.environ['PGPASSWORD']
