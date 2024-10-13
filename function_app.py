import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import csv
import io
import os
from datetime import datetime

app = func.FunctionApp()

@app.function_name(name="CombinedDailyCsv")
@app.schedule(schedule="0 0 11 * * *", arg_name="timer", run_on_startup=True,
              use_monitor=False)
def main(timer: func.TimerRequest) -> None:
    logging.info('CSVファイルを結合しています...')

    # Blob Storageへの接続設定
    connection_string = os.getenv("AzureWebJobsStorage")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    try:
        # 現在のUTC日付
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day
        directory = f"{year}/{month:02d}/{day:02d}/"

        # コンテナと接続
        container_client = blob_service_client.get_container_client("miz-container")
        directories = set()

        # 処理を行うディレクトリ名を取得
        blob_list = container_client.list_blobs(name_starts_with=directory)
        for blob in blob_list:
            subdir = blob.name[len(directory):].split('/')[0]
            directories.add(subdir)

        # それぞれの機械に対してまとめる処理を実行
        for subdir in directories:
            subdir_path = f"{directory}{subdir}/"
            combine_csv_files_in_directory(container_client, subdir_path)

    except Exception as e:
        logging.error(f"エラーが発生しました: {str(e)}")

def combine_csv_files_in_directory(container_client, directory):
    """
    指定されたディレクトリ内にある全てのCSVファイルを、一つのCSVファイルに結合する。

    Args:
        container_client (BlobContainerClient): Blobストレージのコンテナクライアント
        directory (str): 結合対象のディレクトリパス（例: "2024/01/01/machine-01-data/"）

    Raises:
        Exception: Blobストレージからのファイル読み込みや書き込みに失敗した場合
    """
    output = None
    try:
        output = io.StringIO()
        csv_writer = None
        headers_written = False

        # 指定されたディレクトリ内のCSVファイルに対して実行
        blob_list = container_client.list_blobs(name_starts_with=directory)
        for blob in blob_list:
            if blob.name.endswith(".csv"):
                blob_client = container_client.get_blob_client(blob.name)
                csv_content = blob_client.download_blob().readall().decode('utf-8')
                csv_reader = csv.reader(io.StringIO(csv_content))

                # 最初のファイルのヘッダだけを書き込む
                if not headers_written:
                    headers = next(csv_reader)
                    csv_writer = csv.writer(output)
                    csv_writer.writerow(headers)
                    headers_written = True
                else:
                    next(csv_reader)

                # データの書き込み
                for row in csv_reader:
                    csv_writer.writerow(row)

        # メモリ上のCSVデータをBlobにアップロード
        output.seek(0)
        combined_blob_name = f"{directory}combined_data.csv"
        combined_blob_client = container_client.get_blob_client(combined_blob_name)
        combined_blob_client.upload_blob(output.getvalue(), overwrite=True)
        logging.info(f"結合されたファイルが {combined_blob_name} に保存されました")

    except Exception as e:
        logging.error(f"エラーが発生しました: {str(e)}")
    finally:
        # メモリバッファをクローズ
        if output:
            output.close()
