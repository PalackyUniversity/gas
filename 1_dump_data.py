from datetime import datetime, timedelta
from typing import List
from tqdm import tqdm
import requests
import time
import os

DATA_DIR = "data"
HISTORY = 5  # years

NOW = datetime.now()
DATE_FROM = (NOW - timedelta(365.25 * HISTORY)).strftime("%Y-%m-%d")
DATE_TO = NOW.strftime("%Y-%m-%d")

URL = f"https://transparency.entsog.eu/api/v1/operationalData.xlsx?" \
      f"forceDownload=True&dataset=1&directDownload=true&indicator=Physical%20Flow,Firm%20Available" \
      f"&from={DATE_FROM}&to={DATE_TO}&periodType=day&timezone=CET&limit=-1&pointDirection="


def get_cz_only(row: List[str]) -> List[str]:
    return [j for i in row for j in i.split(",") if "cz" in j]


def dump(ids: list, folder: str):
    """
    Dumps all links
    - cannot be done in parallel!
    :param ids: list of ids to dump into folder
    :param folder: folder name for dump
    :return: None
    """

    os.makedirs(os.path.join(DATA_DIR, folder), exist_ok=True)

    for i in tqdm(ids, desc=f"Downloading {folder}"):
        request = requests.get(URL + i, allow_redirects=True)
        with open(os.path.join(DATA_DIR, folder, i.replace(",", "_")) + ".xlsx", "wb") as f:
            f.write(request.content)

        time.sleep(3)


# From CZ to storage
STORAGE_IN = [
    # Prague
    "cz-tso-0001ugs-00004exit,cz-sso-0002ugs-00004entry",
    # Center
    "cz-tso-0001ugs-00446exit,cz-sso-0004ugs-00446entry,cz-sso-0001ugs-00446entry",
    # East
    "cz-tso-0001ugs-00003exit,cz-sso-0001ugs-00003entry"
]

# From storage to CZ
STORAGE_OUT = [
    # Prague
    "cz-sso-0002ugs-00004exit,cz-tso-0001ugs-00004entry",
    # Center
    "cz-sso-0004ugs-00446exit,cz-sso-0001ugs-00446exit,cz-tso-0001ugs-00446entry",
    # East
    "cz-sso-0001ugs-00003exit,cz-tso-0001ugs-00003entry"
]

# in -> CZ
FLOW_IN = get_cz_only([
    # From Poland
    "pl-tso-0002itp-00158exit,cz-tso-0001itp-00158entry",
    # From Slovakia
    "sk-tso-0001itp-00051exit,cz-tso-0001itp-00051entry",
    # From Germany - west (left)
    "de-tso-0009itp-00538exit,cz-tso-0001itp-00538entry",
    # From Germany - west (right)
    "de-tso-0004itp-00073exit,cz-tso-0001itp-00139entry",
    # From Germany - north (the lowest)
    "de-tso-0003itp-00535exit,de-tso-0001itp-00535exit,de-tso-0018itp-00535exit,de-tso-0005itp-00535exit,cz-tso-0001itp-00535entry",
    # From Germany - north (western)
    "de-tso-0001itp-00150exit,cz-tso-0001itp-00150entry",
    # From Germany - Stegal (eastern)
    "cz-tso-0001itp-00123entry",
    # From Germany - Hora svaté Kateřiny (norther)
    "de-tso-0003itp-00015exit,cz-tso-0001itp-00015entry",
    # From Germany - Barandov OPAL (the most norther)
    "de-tso-0020itp-00010exit,de-tso-0001itp-00452exit,de-tso-0016itp-00452exit,de-tso-0020itp-00451exit,cz-tso-0001itp-00010entry"
])

# CZ -> out
FLOW_OUT = get_cz_only([
    # To Poland
    "cz-tso-0001itp-00158exit,pl-tso-0002itp-00158entry",
    # To Slovakia
    "cz-tso-0001itp-00051exit,sk-tso-0001itp-00051entry",
    # To Germany - west (left)
    "cz-tso-0001itp-00538exit,de-tso-0009itp-00538entry",
    # To Germany - west (right)
    "cz-tso-0001itp-00139exit,de-tso-0009itp-00069entry,de-tso-0004itp-00073entry",
    # To Germany - north (the lowest)
    "cz-tso-0001itp-00535exit,de-tso-0018itp-00535entry",
    # To Germany - north (western)
    "cz-tso-0001itp-00150exit",
    # To Germany - Stegal (eastern)
    "cz-tso-0001itp-00123exit,de-tso-0001itp-00123entry",
    # To Germany - Hora svaté Kateřiny (norther)
    "cz-tso-0001itp-00015exit,de-tso-0003itp-00015entry",
    # To Germany - Barandov OPAL (the most norther)
    "cz-tso-0001itp-00010exit,de-tso-0001itp-00452entry,de-tso-0016itp-00452entry"
])

if __name__ == "__main__":
    dump(FLOW_IN, "flow_in")
    dump(FLOW_OUT, "flow_out")
    dump(STORAGE_IN, "storage_in")
    dump(STORAGE_OUT, "storage_out")
