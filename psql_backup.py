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
                            webhook_config=None, parallel_jobs=6):
    """
    PostgreSQLデータベースのバックアップを作成して.xz形式で圧縮する関数。

    Args:
        db_name (str): データベース名
        user (str): ユーザー名
        password (str): パスワード
        host (str): ホスト名（デフォルト: localhost）
        port (str): ポート番号（デフォルト: 5432）
        output_dir (str): 出力ディレクトリ
        webhook_config (dict): Discord Webhook Config（通知用）
        parallel_jobs (int): pg_dumpの並列処理に使用するジョブ数（デフォルト: 4）
    """
    # バックアップ開始通知
    send_discord_notification_if_configured(webhook_config, NOTIFICATION_START_MESSAGE)

    # 出力ディレクトリを確認・作成
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 古いバックアップを削除
    delete_old_backups(output_dir, DELETE_AFTER_DAYS, webhook_config)

    # 現在の日付を取得してフォルダ名を作成
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = os.path.join(output_dir, f"{db_name}_backup_{timestamp}_dir")
    compressed_file = os.path.join(output_dir, f"{db_name}_backup_{timestamp}.tar.xz")

    # 環境変数でパスワードを設定
    os.environ['PGPASSWORD'] = password

    try:
        # ディレクトリ形式でバックアップを実行
        subprocess.run(
            [
                "pg_dump",
                "-h", host,
                "-p", str(port),
                "-U", user,
                "--format=directory",  # ディレクトリ形式を使用
                "-j", str(parallel_jobs),  # 並列処理を指定
                "-f", backup_dir,
                db_name
            ],
            check=True
        )

        # 作成されたディレクトリを圧縮
        subprocess.run(
            ["tar", "-cJf", compressed_file, "-C", output_dir, f"{db_name}_backup_{timestamp}_dir"],
            check=True
        )

        # 圧縮したらディレクトリを削除
        subprocess.run(["rm", "-rf", backup_dir], check=True)

        # バックアップ成功通知
        print(f"バックアップ成功: {compressed_file}")
        message = (
            f"🎉 PostgreSQLバックアップに成功！\n"
            f"**データベース名:** `{db_name}`\n"
            f"**保存場所:** `{compressed_file}`"
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
