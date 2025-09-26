# aws_fetcher/s3_data_fetcher.py

import os
import boto3
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

class S3DataFetcher:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        buckets: dict[str,int],
        prefixes: list[str],
        output_paths: dict[str,str],
        region_name: str = "ap-southeast-1"
    ):
        """
        buckets: mapping of bucket name → country code
        prefixes: list of file‐type prefixes (e.g. ["members-data", ...])
        output_paths: prefix → local parquet path
        """
        self.buckets = buckets
        self.prefixes = prefixes
        self.output_paths = output_paths
        self.region = region_name

        self.s3: boto3.client = None
        # for each prefix we’ll collect a list of DataFrames
        self.data_blocks: dict[str, list[pd.DataFrame]] = {
            p: [] for p in prefixes
        }

        # credentials
        self.aws_access_key_id     = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def connect(self):
        """Instantiate the S3 client."""
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id     = self.aws_access_key_id,
            aws_secret_access_key = self.aws_secret_access_key,
            region_name           = self.region
        )
        print("✅ Connected to S3.")

    def _list_keys(self, bucket: str, prefix: str, date: datetime) -> list[str]:
        """Return all object keys under bucket with prefix-date."""
        key_prefix = f"{prefix}-export-{date.strftime('%d-%m-%Y')}"
        resp = self.s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def _fetch_and_parse(self, bucket: str, key: str, prefix: str):
        """Download one zip file, extract CSVs and append to data_blocks[prefix]."""
        country = self.buckets[bucket]
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        with zipfile.ZipFile(BytesIO(obj["Body"].read())) as z:
            for fname in z.namelist():
                if not fname.endswith(".csv"):
                    continue
                with z.open(fname) as f:
                    df = pd.read_csv(f, dtype=str, low_memory=False)
                    df["Country"] = country
                    self.data_blocks[prefix].append(df)
                    print(f"📦 {bucket}/{key} → {fname} ({len(df)} rows)")

    def fetch_data(
        self,
        start_date: datetime,
        end_date:   datetime
    ) -> None:
        """
        Loop over each day in [start_date..end_date], each bucket, each prefix,
        download any matching zip, extract its CSVs & store in self.data_blocks.
        """
        self.data_blocks = { p: [] for p in self.prefixes}
        days = (end_date - start_date).days + 1
        for bucket, _ in self.buckets.items():
            print(f"\n🔄 Bucket: {bucket}")
            for prefix in self.prefixes:
                for offset in range(days):
                    date = start_date + timedelta(days=offset)
                    keys = self._list_keys(bucket, prefix, date)
                    if not keys:
                        continue
                    print(f"  🔍 {prefix} @ {date.strftime('%d-%m-%Y')}: {len(keys)} files")
                    for key in keys:
                        self._fetch_and_parse(bucket, key, prefix)

    def combine_dataframes(self) -> dict[str, pd.DataFrame]:
        """
        For each prefix, concat all collected DataFrames, drop duplicates,
        and store the combined DataFrame in self.data_blocks.
        Returns a dict mapping prefix -> combined DataFrame.
        """
        combined_blocks: dict[str, pd.DataFrame] = {}
        for prefix, dfs in self.data_blocks.items():
            dfs_to_concat = list(dfs)  # copy
            out_path = self.output_paths.get(prefix)
            if out_path and os.path.exists(out_path):
                existing = pd.read_parquet(out_path)
                dfs_to_concat.insert(0, existing)

            if not dfs_to_concat:
                print(f"⚠️ No data for prefix '{prefix}', skipping.")
                continue

            combined = pd.concat(dfs_to_concat, ignore_index=True).drop_duplicates()
            combined_blocks[prefix] = combined
            # also overwrite in-place if you want
            self.data_blocks[prefix] = combined

            print(f"✅ Combined {len(combined)} rows for '{prefix}'")

        return combined_blocks

    def close(self):
        """Nothing to close for boto3, but provided for symmetry."""
        print("🔒 S3DataFetcher done.")

