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
    '"GroupCode","GroupName","TerritoryID","SlpCode","CreateDate","UpdateDate","IsActive"\n'
    '"GmbH","10001","Acme Corp","123 Main St","Berlin","10115",'
    '"DE","123 Main St","Berlin","10115","DE",'
    '"100","Retail","1","7","2022-06-01","2023-01-15","Y"\n'
    '"GmbH","10002","Schmidt, Mueller & Co.","Bahnhofstr. 5","München","80331",'
    '"DE","Bahnhofstr. 5","München","80331","DE",'
    '"100","Retail","1","12","2021-03-10","2023-11-20","N"\n'
    '"GmbH","10003","Test ""Quoted"" Name","Hauptstr. 1","Frankfurt, Main","60311",'
    '"DE","Hauptstr. 1","Frankfurt, Main","60311","DE",'
    '"200","Wholesale","2","7","2020-12-25","2024-01-01","Y"\n'
).encode("utf-8")


# ═════════════════════════════════════════════
# Sample CSV bytes — dim_product (comma separator, quoted fields)
# ═════════════════════════════════════════════

DIM_PRODUCT_CSV = (
    '"Entity","ItemCode","Description","ItemGroup","IsActive","Webshop_Active",'
    '"WS_Active_Flag","Is_Prov","Status","Parent_Item","Weight_SU_kg",'
    '"Weight_Primary_g","Weight_Secondary_g","Content_ML","Content_GR",'
    '"ProductLine","Name_EN","Variant_Dim1","CreateDate"\n'
    '"GmbH","SKU-001","Face Cream 50ml","100","Y","Y",'
    '"Y","N","Active","","0.5",'
    '"50","200","50","",'
    '"Skincare","Face Cream 50ml EN","","2020-01-01"\n'
    '"GmbH","SKU-002","Eye Serum 15ml","100","Y","Y",'
    '"Y","N","Active","","0.2",'
    '"15","100","15","",'
    '"Skincare","Eye Serum 15ml EN","15ml","2021-03-15"\n'
    '"GmbH","SKU-003","Body Lotion 200ml","200","N","N",'
    '"N","N","Inactive","SKU-001","1.0",'
    '"200","500","","200",'
    '"Body","Body Lotion 200ml EN","","2019-06-10"\n'
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
