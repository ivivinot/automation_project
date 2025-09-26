# sftp_fetcher/sftp_data_fetcher.py

import os
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import paramiko
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
import tempfile

class SFTPDataFetcher:
    def __init__(self, hostname, username, password, folder='db', file_prefix="", keys: list[str] = None):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.folder = folder
        self.file_prefix = file_prefix
        self.ssh = None
        self.sftp = None
        self.keys = keys or None
        self.start_date = None
        self.end_date = None
        self.data_blocks = {}
        # Initialize Spark session
        self.spark = SparkSession.builder.appName("SFTPDataFetcher").getOrCreate()

    def set_date_range(self, start: str, end: str):
        self.start_date = datetime.strptime(start, "%d-%m-%Y")
        self.end_date = datetime.strptime(end, "%d-%m-%Y")

    def connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=self.hostname, username=self.username, password=self.password)
        self.sftp = self.ssh.open_sftp()
        print("✅ Connected to SFTP server.")

    def _filter_files_by_date(self, files):
        filtered = []
        for file in files:
            if self.file_prefix in file:
                try:
                    file_date_str = file.split("_")[-1]
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                    if self.start_date <= file_date <= self.end_date:
                        filtered.append(file)
                except Exception:
                    continue
        return sorted(filtered)

    def fetch_files(self):
        """Return list of files in the folder matching the date range and prefix."""
        all_files = self.sftp.listdir(self.folder)
        filtered = self._filter_files_by_date(all_files)
        print(f"📂 {len(filtered)} matching files found.")
        return filtered

    def fetch_rawdata(self, files):
        """Combine all raw CSV data from filtered files."""
        combined_raw_df = pd.DataFrame()

        for file in files:
            remote_path = f"{self.folder}/{file}"
            with self.sftp.open(remote_path, 'r') as remote_file:
                content = remote_file.read().decode("utf_8").strip()

            if not content:
                print(f"⚠️ Empty file: {file}")
                continue

            try:
                df = pd.read_csv(StringIO(content), delimiter=",", dtype=str)
                df = pd.concat([pd.DataFrame([df.columns.tolist()], columns=df.columns), df], ignore_index=True)
                df["datablock"] = df[df.columns[0]].astype(str).str[:3]
                combined_raw_df = pd.concat([combined_raw_df, df], ignore_index=True)
            except Exception as e:
                print(f"❌ Failed to process {file}: {e}")

        return combined_raw_df

    def fetch_rawdata_spark(self, files):
        spark = self.spark
        raw_dfs = []

        default_schema = StructType([
            StructField("col_0", StringType(), True),
            StructField("col_1", StringType(), True)
        ])

        for file in files:
            remote_path = f"{self.folder}/{file}"
            with self.sftp.open(remote_path, 'r') as remote_file:
                content = remote_file.read().decode("utf_8").strip()

            if not content:
                print(f"⚠️ Empty file: {file}")
                continue

            try:
                # Spark can't read from string, so we simulate a file with StringIO
                with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmp:
                    tmp.write(content)
                    tmp_path = tmp.name

                # Read using Spark
                temp_df = spark.read.csv(tmp_path, header=True, sep=",", inferSchema=True)
                raw_dfs.append(temp_df)

            except Exception as e:
                print(f"❌ Failed to process {file}: {e}")

        if raw_dfs:
            return raw_dfs[0].unionByName(*raw_dfs[1:]) if len(raw_dfs) > 1 else raw_dfs[0]
        else:
            return spark.createDataFrame([], schema=None)
    
    def fetch_blockdata(self, raw_df):
        """Split combined raw data into cleaned blocks based on data type keys."""
        self.data_blocks = {}

        if raw_df.empty:
            print("⚠️ No data to split.")
            return

        keys = ["SUM", "OTS", "CHR", "CMI", "CTD", "CDC", "MID", "KDS"]
        for key in keys:
            block = raw_df[raw_df.iloc[:, 1] == key].reset_index(drop=True)
            if not block.empty:
                split_cols = block.iloc[:, 0].str.split("|", expand=True)
                split_cols.columns = split_cols.iloc[0].astype(str)
                self.data_blocks[key] = split_cols[1:].reset_index(drop=True)
                for key in self.data_blocks:
                    df = self.data_blocks[key]
                    if key == "KDS":
                        self.data_blocks[key] = df[df['guestcheckid'] != 'guestcheckid'].reset_index(drop=True)
                    elif 'index' in df.columns:
                        self.data_blocks[key] = df[df['index'] != 'index'].reset_index(drop=True)

                     

    def fetch_blockdata_spark(self, raw_df):
        from pyspark.sql.functions import col, split

        self.data_blocks = {}

        keys = ["SUM", "OTS", "CHR", "CMI", "CTD", "CDC", "MID", "KDS"]
        for key in keys:
            block_df = raw_df.filter(col(raw_df.columns[1]) == key)

            if block_df.count() > 0:
                # Split the first column into multiple columns
                num_cols = block_df.select(split(col(block_df.columns[0]), "\\|")).first()[0]
                n = len(num_cols)

                # Apply split into separate columns
                split_cols = split(col(block_df.columns[0]), "\\|")
                for i in range(n):
                    block_df = block_df.withColumn(f"col_{i}", split_cols.getItem(i))

                self.data_blocks[key] = block_df.drop(block_df.columns[0])  # drop original string col


    def fetch_data(self):
        files = self.fetch_files()
        raw_data = self.fetch_rawdata(files)
        self.fetch_blockdata(raw_data)


    def get_data_block(self, key: str) -> pd.DataFrame:
        return self.data_blocks.get(key, pd.DataFrame())

    def close(self):
        if self.sftp:
            self.sftp.close()
        if self.ssh:
            self.ssh.close()
        print("🔒 SFTP connection closed.")
