from psql_backup import backup_postgres_with_xz
from config import DB_CONFIG, BACKUP_DIR, DISCORD_CONFIG, DISCORD_ENABLED

if __name__ == "__main__":
    # 設定内容をターミナルにプリント
    print("=== 実行コンフィグ ===")
    print(f"バックアップ対象データベース: {DB_CONFIG['db_name']}")
    print(f"ユーザー名: {DB_CONFIG['user']}")
    print(f"ホスト: {DB_CONFIG['host']}")
    print(f"ポート: {DB_CONFIG['port']}")
    print(f"並列実行数: {DB_CONFIG['parallel_jobs']}")
    print(f"出力先ディレクトリ: {BACKUP_DIR}")
    print(f"Discord通知: {'有効' if DISCORD_ENABLED else '無効'}")
    if DISCORD_ENABLED:
        print(f"  Webhook URL: {DISCORD_CONFIG.get('webhook_url')}")
        print(f"  表示名: {DISCORD_CONFIG.get('username')}")
        print(f"  アイコン URL: {DISCORD_CONFIG.get('avatar_url')}")
    print("=====================")

    # PostgreSQL Backupを実行
    backup_postgres_with_xz(
        db_name=DB_CONFIG["db_name"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        parallel_jobs=DB_CONFIG["parallel_jobs"],
        output_dir=BACKUP_DIR,
        webhook_config=DISCORD_CONFIG if DISCORD_ENABLED else None,  # Discord機能無効の場合はNoneを渡す
    )
