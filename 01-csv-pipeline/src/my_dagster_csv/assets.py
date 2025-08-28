from dagster import asset, Definitions
from pathlib import Path
import pandas as pd

# جذر المشروع = فولدر "CSV Pipeline" (أو أي اسم مشروعك)
ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)

@asset
def raw_csv_path() -> str:
    """مسار ملف الإدخال CSV."""
    return str(DATA / "raw.csv")

@asset
def loaded_df(raw_csv_path: str) -> pd.DataFrame:
    """قراءة CSV إلى DataFrame."""
    df = pd.read_csv(raw_csv_path)
    return df

@asset
def cleaned_df(loaded_df: pd.DataFrame) -> pd.DataFrame:
    """تنظيف بسيط للبيانات."""
    df = loaded_df.copy()

    # تنظيف الاسم
    if "name" in df.columns:
        df["name"] = df["name"].astype(str).str.strip()
        df = df[df["name"].str.len() > 0]          # حذف الفارغ
        df["name"] = df["name"].str.title()        # تنسيق

    # تحويل amount إلى رقم
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df = df.dropna(subset=["amount"]).reset_index(drop=True)

    # ترتيب أعمدة (اختياري)
    cols = [c for c in ["id", "name", "amount"] if c in df.columns] + \
           [c for c in df.columns if c not in ["id", "name", "amount"]]
    return df[cols]

@asset
def write_clean_csv(cleaned_df: pd.DataFrame) -> str:
    """حفظ الناتج إلى data/clean.csv وإرجاع المسار."""
    out_path = DATA / "clean.csv"
    cleaned_df.to_csv(out_path, index=False)
    return str(out_path)

# تعريف الأصول لـ Dagster
defs = Definitions(assets=[raw_csv_path, loaded_df, cleaned_df, write_clean_csv])
