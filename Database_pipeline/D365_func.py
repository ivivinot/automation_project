import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
#start = time.time()
class D365ODataFetcher:
    def __init__(
        self,
        tenant_id: str,
        creds: dict[str, dict[str,str]],
        base_url: str,
        date_map: dict[str, str] | None = None,
        required_fields: list[str] | None = None,
        page_size: int = 100_000

    ):
        """
        tenant_id: your Azure AD tenant ID
        creds: mapping country -> {"client_id":…, "client_secret":…}
        base_url: the OData endpoint URL
        required_fields: list of OData fields to pull
        page_size: number of records per page
        """
        self.tenant_id       = tenant_id
        self.creds           = creds
        self.base_url        = base_url
        self.required_fields = required_fields
        self.page_size       = page_size
        self.date_map        = date_map

    def _get_date_field(self) -> str:
        if not self.date_map:
            return "TransDate"
        for key, field in self.date_map.items():
            if key in self.base_url:
                return field
        return "TransDate"


    def _get_token(self, client_id: str, client_secret: str) -> str:
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"
        payload = {
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "resource":      "https://th-prod.operations.dynamics.com"
        }
        r = requests.post(url, data=payload)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # print full error body for diagnostics
            print("❌ Token fetch failed:", r.status_code, r.text)
            raise
        return r.json()["access_token"]

    def _fetch_for_country_on_date(
        self, country: str, single_date: datetime, token: str
    ) -> pd.DataFrame:
        """
        Fetch OData just for one country on one date.
        Returns a DataFrame, with only self.required_fields if set.
        """
        date_str = single_date.strftime("%Y-%m-%d")
        headers  = {"Authorization": f"Bearer {token}"}
        date_field = self._get_date_field()
        #auth_fetch = time.time()
        #print(f"Auth fetch {auth_fetch - start}")
        
        
        flt      = (
            f"$filter={date_field} ge {date_str}T00:00:00Z"
            f" and {date_field} le {date_str}T23:59:59Z"
        ) 

        select_clause = f"&$select={','.join(self.required_fields)}" if self.required_fields else ""
        next_url = f"{self.base_url}?{flt}&$top={self.page_size}{select_clause}"
        #run_fetch = time.time()
        #print(f"run fetch {run_fetch - start}")
        all_records = []
        #n = 0
        while next_url:
            resp = requests.get(next_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            page_data = data.get("value", [])
            if self.required_fields:
                all_records.extend([{k: r.get(k) for k in self.required_fields} for r in page_data])
                #n += 1
            else:
                all_records.extend(page_data)
            # follow the OData continuation link
            next_url = data.get("@odata.nextLink")

        # build the DataFrame once all pages are fetched
        df = pd.DataFrame(all_records)
        df["Country"]   = country
        df["FetchDate"] = single_date
        #print(f"Records fetched: {n}")
        #fetch_end = time.time()
        #print(f"Fetch end {fetch_end - start}")
        return df

    def fetch_country_range(
        self,
        country: str,
        start: datetime,
        end:   datetime
    ) -> pd.DataFrame:
        """
        Fetch data for a single country over a date range [start..end], inclusive.
        Returns one concatenated DataFrame.
        """
        curr = start
        frames = []
        while curr <= end:
            print(f"→ Fetching {country} on {curr.date()}")
            df = self._fetch_for_country_on_date(country, curr)
            frames.append(df)
            curr += timedelta(days=1)

        if frames:
            result = pd.concat(frames, ignore_index=True)
            # drop duplicates by your unique key
            return result.drop_duplicates()
        else:
            return pd.DataFrame()

    def fetch_all_countries_on_date(
        self, single_date: datetime
    ) -> pd.DataFrame:
        """
        Authenticate once per country and fetch data for the given date.
        Fetch for all countries on a single date.
        """
        #before_token = time.time()
        #print(f"Before Auth {before_token - start}")
        country_token_map = {}
        for country, cred in self.creds.items():
            try:
                token = self._get_token(cred["client_id"], cred["client_secret"])
                country_token_map[country] = token
                #print("Authentication run")
            except Exception as e:
                print(f"❌ Skipping {country} for {single_date.date()} due to token error: {e}")
        #after_token = time.time()
        #print(f"After Auth {after_token - start}")
        
        frames = []
        for country, token in country_token_map.items():
            print(f"→ Fetching {country} on {single_date.date()}")
            try:
                df_country = self._fetch_for_country_on_date(country, single_date, token)
                #print("frame joining")

                if self.required_fields:
                    cols_to_keep = self.required_fields + ["Country", "FetchDate"]
                    # guard in case Country/FetchDate weren’t already in df_country
                    cols_to_keep = [c for c in cols_to_keep if c in df_country.columns]
                    df_country = df_country[cols_to_keep]

            
                frames.append(df_country)
            
            except Exception as e:
                print(f"❌ Skipping {country} for {single_date.date()} due to fetch error: {e}")
                
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def fetch_all_countries_range(
        self,
        start: datetime,
        end:   datetime
    ) -> pd.DataFrame:
        """
        Fetch for all countries over a date range.
        Returns one concatenated DataFrame.
        """
        curr = start
        frames = []
        while curr <= end:
            df_day = self.fetch_all_countries_on_date(curr)
            frames.append(df_day)
            curr += timedelta(days=1)
        if frames:
            df = pd.concat(frames, ignore_index=True)
            return df.drop_duplicates(subset=["InventTransId"])
        return pd.DataFrame()
