"""
Shared test fixtures — sample CSV data & mock blob helpers.
"""

import io
import pytest


# ═════════════════════════════════════════════
# Sample CSV bytes — cold_extract (= separator, German decimals)
# ═════════════════════════════════════════════

COLD_EXTRACT_CSV = (
    "Entity=DocEntry=DocNum=DocDate=DocType=LineNum=CardCode=ItemCode="
    "Dscription=Quantity=Net Revenue=SlpCode=UpdateDate=\n"
    "GmbH=1001=5001=02.01.2023 00:00:00=I=0=10001=SKU-001="
    "Face Cream 50ml=1,000000=49,990000=7=02.01.2023 00:00:00=\n"
    "GmbH=1002=5002=15.03.2023 00:00:00=I=1=10002=SKU-002="
    "Eye Serum 15ml=2,500000=1.234,560000=12=15.03.2023 00:00:00=\n"
    "GmbH=1003=5003=31.12.2023 00:00:00=C=0=10003=SKU-003="
    "Body Lotion 200ml=-1,000000=-29,990000=7=31.12.2023 00:00:00=\n"
).encode("utf-8")


# ═════════════════════════════════════════════
# Sample CSV bytes — dim_customer (comma separator, quoted fields)
# ═════════════════════════════════════════════

DIM_CUSTOMER_CSV = (
    '"Entity","CardCode","CardName","BillToStreet","BillToCity","BillToZip",'
    '"BillToCountry","ShipToStreet","ShipToCity","ShipToZip","ShipToCountry",'
    '"GroupCode","Territory","SlpCode","CreateDate","UpdateDate","validFor"\n'
    '"GmbH","10001","Acme Corp","123 Main St","Berlin","10115",'
    '"DE","123 Main St","Berlin","10115","DE",'
    '"100","1","7","01.06.2022 00:00:00","15.01.2023 00:00:00","Y"\n'
    '"GmbH","10002","Schmidt, Mueller & Co.","Bahnhofstr. 5","München","80331",'
    '"DE","Bahnhofstr. 5","München","80331","DE",'
    '"100","1","12","10.03.2021 00:00:00","20.11.2023 00:00:00","N"\n'
    '"GmbH","10003","Test ""Quoted"" Name","Hauptstr. 1","Frankfurt, Main","60311",'
    '"DE","Hauptstr. 1","Frankfurt, Main","60311","DE",'
    '"200","2","7","25.12.2020 00:00:00","01.01.2024 00:00:00","Y"\n'
).encode("utf-8")


# ═════════════════════════════════════════════
# Sample CSV bytes — dim_product (comma separator, quoted fields)
# ═════════════════════════════════════════════

DIM_PRODUCT_CSV = (
    '"Entity","ItemCode","Description","ItemGroup","IsInventory","IsSalesItem",'
    '"IsActive","U_Guidanceline","U_Kontrollfeld","PriceListNum","PriceListName",'
    '"CreateDate","UpdateDate"\n'
    '"GmbH","SKU-001","Face Cream 50ml","100","Y","Y",'
    '"Y","Premium","DERM","1","Base Price",'
    '"01.01.2020 00:00:00","15.06.2023 00:00:00"\n'
    '"GmbH","SKU-002","Eye Serum 15ml","100","Y","Y",'
    '"Y","Standard","COSM","1","Base Price",'
    '"15.03.2021 00:00:00","20.11.2023 00:00:00"\n'
    '"GmbH","SKU-003","Body Lotion 200ml","200","N","/",'\
    '"Y","Economy","BODY","2","Retail",'
    '"10.06.2019 00:00:00","01.01.2024 00:00:00"\n'
).encode("utf-8")


# ═════════════════════════════════════════════
# Mock blob download helper
# ═════════════════════════════════════════════

class MockBlobDownload:
    """Mimics blob_client.download_blob().readall()."""

    def __init__(self, data: bytes):
        self._data = data

    def readall(self) -> bytes:
        return self._data


class MockContainerClient:
    """Minimal mock for azure.storage.blob.ContainerClient."""

    def __init__(self, blobs: dict[str, bytes] | None = None):
        self._blobs = blobs or {}

    def download_blob(self, name: str):
        if name not in self._blobs:
            raise FileNotFoundError(f"Blob not found: {name}")
        return MockBlobDownload(self._blobs[name])

    def upload_blob(self, name: str, data, overwrite: bool = True):
        if isinstance(data, (bytes, bytearray)):
            self._blobs[name] = bytes(data)
        else:
            self._blobs[name] = data.read() if hasattr(data, "read") else bytes(data)

    def list_blobs(self, name_starts_with: str = ""):
        return [
            type("Blob", (), {"name": k, "size": len(v), "last_modified": None})()
            for k, v in self._blobs.items()
            if k.startswith(name_starts_with)
        ]
