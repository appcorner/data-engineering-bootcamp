import csv
import pathlib

import scrapy
from scrapy.crawler import CrawlerProcess

import json
import os
import sys

from google.api_core import exceptions
from google.cloud import storage
from google.oauth2 import service_account

URL = "https://ทองคําราคา.com/"

DATA_FOLDER = "data"

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    keyfile = os.environ.get("KEYFILE_PATH")
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    project_id = "deb-01-385616"

    storage_client = storage.Client(
        project=project_id,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

class MySpider(scrapy.Spider):
    name = "gold_price_spider"
    start_urls = [URL,]

    def parse(self, response):
        header = response.css("#divDaily h3::text").get().strip()
        print(header)

        table = response.css("#divDaily .pdtable")
        # print(table)

        rows = table.css("tr")
        # rows = table.xpath("//tr")
        # print(rows)

        for row in rows:
            print(row.css("td::text").extract())
            # print(row.xpath("td//text()").extract())
        monthMap = {
            "มกราคม": "01",
            "กุมภาพันธ์": "02",
            "มีนาคม": "03",
            "เมษายน": "04",
            "พฤษภาคม": "05",
            "มิถุนายน": "06",
            "กรกฎาคม": "07",
            "สิงหาคม": "08",
            "กันยายน": "09",
            "ตุลาคม": "10",
            "พฤศจิกายน": "11",
            "ธันวาคม": "12",
        }
        # Write to CSV
        dateText = header.split(" ประจำวันที่ ")[-1]
        dateArr = dateText.split(" ")
        dateStr = f"{int(dateArr[2])-543}{monthMap[dateArr[1]]}{int(dateArr[0]):02d}"
        pathlib.Path(f'../{DATA_FOLDER}/{dateStr}/').mkdir(parents=True, exist_ok=True)
        with open(f"../{DATA_FOLDER}/{dateStr}/gold_price.csv", "wt") as f:
            writer = csv.writer(f)
            for row in rows:
                data = row.css("td::text").extract()
                writer.writerow(data)

        upload_blob(
            bucket_name="deb_buckket_100039",
            source_file_name=f"../{DATA_FOLDER}/{dateStr}/gold_price.csv",
            destination_blob_name=f"{dateStr}/gold_price.csv",
        )    


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()
