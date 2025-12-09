"""
duplicate_checker.py

Công cụ Streamlit để kiểm tra trùng hồ sơ trước khi phê duyệt.
Dành cho dữ liệu ĐẤT Ở (Land) từ Excel iFast.

Các nhóm kiểm tra trùng:

A. Phê duyệt vs Hoàn thành
--------------------------
1) TRÙNG TỌA ĐỘ (ưu tiên, chắc chắn):
   - Tọa độ chuẩn hóa (coord_norm) trùng nhau
   - 'Thời điểm thu thập thông tin' của hồ sơ Phê duyệt > hồ sơ Hoàn thành
   → Luôn gắn nhãn: CẢNH BÁO TRÙNG (không xét Người tạo)
   → Cột hiển thị: TỌA ĐỘ

2) TRÙNG ĐỊA CHỈ (5 cột W,X,Y,Z,AE):
   - Trùng 5 thông tin:
        Tỉnh/Thành phố
        Quận/Huyện/Thị xã
        Xã/Phường
        Đường/Phố
        Số nhà
   - 'Thời điểm thu thập thông tin' của hồ sơ Phê duyệt > hồ sơ Hoàn thành
   - Nếu cùng Người tạo  → CẢNH BÁO TRÙNG
   - Nếu khác Người tạo → NGHI NGỜ TRÙNG
   → Cột hiển thị: ĐỊA CHỈ (nếu không có trùng tọa độ)

B. Hoàn thành vs Hoàn thành
---------------------------
- So sánh các hồ sơ đều ở trạng thái 'Hoàn thành' với nhau
- Chỉ xét các hồ sơ Hoàn thành có 'Thời điểm thu thập thông tin' nhỏ hơn hồ sơ đang xét
- Rule giống phần A:
    + Trùng tọa độ → CẢNH BÁO TRÙNG
    + Trùng địa chỉ:
        * Cùng Người tạo → CẢNH BÁO TRÙNG
        * Khác Người tạo → NGHI NGỜ TRÙNG

Output chung cho cả hai nhóm:
- ID                : ID của hồ sơ bị coi là trùng (hồ sơ về sau)
- Người tạo         : Người tạo của hồ sơ đó
- Lý do trùng       : Cảnh báo / Nghi ngờ + mô tả chi tiết
- Địa chỉ/Tọa độ trùng:
    + Nếu có trùng tọa độ → chỉ hiển thị tọa độ (cột AF)
    + Nếu chỉ trùng địa chỉ → hiển thị Địa chỉ: Số nhà – Đường – Xã – Quận – Tỉnh
- ID trùng          : các ID trước đó mà hồ sơ này trùng (ngăn cách '; ')
- Người tạo trùng   : Người tạo tương ứng các ID trùng
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Set
import io

import pandas as pd

try:
    import streamlit as st  # type: ignore
except ImportError:  # pragma: no cover
    st = None  # type: ignore


# ==========================
#  Constants
# ==========================

ADDR_COLS = [
    "Tỉnh/Thành phố",       # W
    "Quận/Huyện/Thị xã",    # X
    "Xã/Phường",            # Y
    "Đường/Phố",            # Z
    "Số nhà",               # AE
]

CREATOR_COL = "Người tạo"                     # cột E
TIME_COL = "Thời điểm thu thập thông tin"     # cột L
COORD_COL = "Tọa độ"                          # cột AF
STATUS_COL = "Giai đoạn hiện tại"             # cột H
ID_COL = "ID"


# ==========================
#  Helpers
# ==========================

def build_addr_key(row: pd.Series) -> str:
    parts = [str(row.get(col, "")).strip().lower() for col in ADDR_COLS]
    return "||".join(parts)


def normalize_coord(value: Any, max_len: int = 8) -> Optional[str]:
    """Chuẩn hóa tọa độ để bắt cả case thêm số lẻ phía sau."""
    if pd.isna(value):
        return None
    try:
        lat_raw, lon_raw = str(value).split(",")
        lat = lat_raw.strip()[:max_len]
        lon = lon_raw.strip()[:max_len]
        return f"{lat},{lon}"
    except Exception:
        return None


def format_address(row: pd.Series) -> str:
    """Số nhà – Đường/Phố – Xã/Phường – Quận/Huyện/Thị xã – Tỉnh/Thành phố."""
    parts = [
        str(row.get("Số nhà", "")).strip(),
        str(row.get("Đường/Phố", "")).strip(),
        str(row.get("Xã/Phường", "")).strip(),
        str(row.get("Quận/Huyện/Thị xã", "")).strip(),
        str(row.get("Tỉnh/Thành phố", "")).strip(),
    ]
    return " – ".join([p for p in parts if p])


# ==========================
#  Core Logic
# ==========================

def _prepare(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Chuẩn hóa chung: key địa chỉ, tọa độ, thời gian; tách nhóm trạng thái."""
    # check required columns
    required_cols = ADDR_COLS + [
        CREATOR_COL, TIME_COL, COORD_COL, STATUS_COL, ID_COL
    ]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Thiếu cột: {col}")

    df = df.copy()

    # chuẩn hóa
    df["addr_key"] = df.apply(build_addr_key, axis=1)
    df["coord_norm"] = df[COORD_COL].apply(normalize_coord)
    df["time_norm"] = pd.to_datetime(df[TIME_COL], dayfirst=True, errors="coerce")
    df["creator_norm"] = df[CREATOR_COL].astype(str).str.strip()

    hoan_thanh = df[df[STATUS_COL] == "Hoàn thành"].copy()
    phe_duyet = df[df[STATUS_COL] == "Phê duyệt"].copy()

    return {
        "all": df,
        "hoan_thanh": hoan_thanh,
        "phe_duyet": phe_duyet,
    }


def _build_groups(hoan_thanh: pd.DataFrame):
    """Tạo group cho Hoàn thành để tái sử dụng."""
    addr_groups = hoan_thanh.groupby("addr_key").groups
    coord_groups = hoan_thanh.groupby("coord_norm").groups
    return addr_groups, coord_groups


def _collect_result(
    row: pd.Series,
    duplicate_ids: Set[Any],
    duplicate_creators: Set[str],
    has_coord_dup: bool,
    has_addr_dup: bool,
    severity_label: str,
    reason_details: List[str],
) -> Dict[str, Any]:
    # ưu tiên hiển thị tọa độ nếu có trùng tọa độ
    if has_coord_dup:
        info = f"Tọa độ: {row.get(COORD_COL, '')}"
    elif has_addr_dup:
        info = f"Địa chỉ: {format_address(row)}"
    else:
        info = ""

    return {
        "ID": row.get(ID_COL),
        "Người tạo": row.get("creator_norm", ""),
        "Lý do trùng": f"{severity_label} – " + " ; ".join(reason_details),
        "Địa chỉ/Tọa độ trùng": info,
        "ID trùng": "; ".join(str(x) for x in sorted(duplicate_ids)),
        "Người tạo trùng": "; ".join(sorted(duplicate_creators)),
    }


def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trả về bảng trùng bao gồm:
    - Phê duyệt vs Hoàn thành
    - Hoàn thành vs Hoàn thành
    """
    prep = _prepare(df)
    hoan_thanh = prep["hoan_thanh"]
    phe_duyet = prep["phe_duyet"]

    addr_groups, coord_groups = _build_groups(hoan_thanh)

    results: List[Dict[str, Any]] = []

    # ==========================
    # A. Phê duyệt vs Hoàn thành
    # ==========================
    for _, row in phe_duyet.iterrows():
        duplicate_ids: Set[Any] = set()
        duplicate_creators: Set[str] = set()
        reason_details: List[str] = []
        severity: Optional[str] = None

        row_time = row["time_norm"]
        addr_key = row["addr_key"]
        coord_key = row["coord_norm"]
        creator = row["creator_norm"]

        has_addr_dup = False
        has_coord_dup = False

        # ---- Trùng địa chỉ
        addr_idx = addr_groups.get(addr_key)
        if addr_idx is not None and len(addr_idx) > 0:
            subset = hoan_thanh.loc[addr_idx]
            subset = subset[subset["time_norm"] < row_time]

            if not subset.empty:
                has_addr_dup = True
                same_creator = subset[subset["creator_norm"] == creator]
                diff_creator = subset[subset["creator_norm"] != creator]

                if not same_creator.empty:
                    severity = "Cảnh báo trùng"
                    duplicate_ids.update(same_creator[ID_COL])
                    duplicate_creators.update(same_creator["creator_norm"])
                    reason_details.append(
                        "Phê duyệt vs Hoàn thành – Cùng Người tạo và trùng 5 thông tin địa chỉ"
                    )

                if not diff_creator.empty:
                    if severity is None:
                        severity = "Nghi ngờ trùng"
                    duplicate_ids.update(diff_creator[ID_COL])
                    duplicate_creators.update(diff_creator["creator_norm"])
                    reason_details.append(
                        "Phê duyệt vs Hoàn thành – Khác Người tạo nhưng trùng 5 thông tin địa chỉ"
                    )

        # ---- Trùng tọa độ
        coord_idx = coord_groups.get(coord_key)
        if coord_idx is not None and len(coord_idx) > 0:
            subset = hoan_thanh.loc[coord_idx]
            subset = subset[subset["time_norm"] < row_time]

            if not subset.empty:
                has_coord_dup = True
                severity = "Cảnh báo trùng"
                duplicate_ids.update(subset[ID_COL])
                duplicate_creators.update(subset["creator_norm"])
                reason_details.append(
                    "Phê duyệt vs Hoàn thành – Trùng tọa độ (100% hoặc gần đúng)"
                )

        if duplicate_ids:
            if severity is None:
                severity = "Nghi ngờ trùng"  # fallback, về lý thuyết không xảy ra
            results.append(
                _collect_result(
                    row=row,
                    duplicate_ids=duplicate_ids,
                    duplicate_creators=duplicate_creators,
                    has_coord_dup=has_coord_dup,
                    has_addr_dup=has_addr_dup,
                    severity_label=severity,
                    reason_details=reason_details,
                )
            )

    # ==========================
    # B. Hoàn thành vs Hoàn thành
    # ==========================
    for _, row in hoan_thanh.iterrows():
        duplicate_ids: Set[Any] = set()
        duplicate_creators: Set[str] = set()
        reason_details: List[str] = []
        severity: Optional[str] = None

        row_time = row["time_norm"]
        addr_key = row["addr_key"]
        coord_key = row["coord_norm"]
        creator = row["creator_norm"]
        row_id = row[ID_COL]

        has_addr_dup = False
        has_coord_dup = False

        # ---- Trùng địa chỉ giữa Hoàn thành với nhau
        addr_idx = addr_groups.get(addr_key)
        if addr_idx is not None and len(addr_idx) > 0:
            subset = hoan_thanh.loc[addr_idx]
            subset = subset[(subset["time_norm"] < row_time) & (subset[ID_COL] != row_id)]

            if not subset.empty:
                has_addr_dup = True
                same_creator = subset[subset["creator_norm"] == creator]
                diff_creator = subset[subset["creator_norm"] != creator]

                if not same_creator.empty:
                    severity = "Cảnh báo trùng"
                    duplicate_ids.update(same_creator[ID_COL])
                    duplicate_creators.update(same_creator["creator_norm"])
                    reason_details.append(
                        "Hoàn thành vs Hoàn thành – Cùng Người tạo và trùng 5 thông tin địa chỉ"
                    )

                if not diff_creator.empty:
                    if severity is None:
                        severity = "Nghi ngờ trùng"
                    duplicate_ids.update(diff_creator[ID_COL])
                    duplicate_creators.update(diff_creator["creator_norm"])
                    reason_details.append(
                        "Hoàn thành vs Hoàn thành – Khác Người tạo nhưng trùng 5 thông tin địa chỉ"
                    )

        # ---- Trùng tọa độ giữa Hoàn thành với nhau
        coord_idx = coord_groups.get(coord_key)
        if coord_idx is not None and len(coord_idx) > 0:
            subset = hoan_thanh.loc[coord_idx]
            subset = subset[(subset["time_norm"] < row_time) & (subset[ID_COL] != row_id)]

            if not subset.empty:
                has_coord_dup = True
                severity = "Cảnh báo trùng"
                duplicate_ids.update(subset[ID_COL])
                duplicate_creators.update(subset["creator_norm"])
                reason_details.append(
                    "Hoàn thành vs Hoàn thành – Trùng tọa độ (100% hoặc gần đúng)"
                )

        if duplicate_ids:
            if severity is None:
                severity = "Nghi ngờ trùng"
            results.append(
                _collect_result(
                    row=row,
                    duplicate_ids=duplicate_ids,
                    duplicate_creators=duplicate_creators,
                    has_coord_dup=has_coord_dup,
                    has_addr_dup=has_addr_dup,
                    severity_label=severity,
                    reason_details=reason_details,
                )
            )

    return pd.DataFrame(results)


# ==========================
#  Streamlit App
# ==========================

def run_app() -> None:  # pragma: no cover
    if st is None:
        raise RuntimeError("Streamlit chưa được cài. Chạy: pip install streamlit")

    st.set_page_config(page_title="iFast Duplicate Checker", layout="wide")
    st.title("🧮 iFast – Công cụ kiểm tra trùng hồ sơ")

    st.markdown(
        """
        Công cụ kiểm tra trùng **hồ sơ Đất ở** trong iFast.

        **Nhóm kiểm tra:**
        - Phê duyệt vs Hoàn thành (hồ sơ đang trình so với hồ sơ đã hoàn thành)
        - Hoàn thành vs Hoàn thành (các hồ sơ đã hoàn thành trùng nhau)

        **Ưu tiên hiển thị:**
        - Nếu trùng tọa độ → chỉ hiển thị tọa độ
        - Nếu chỉ trùng địa chỉ → hiển thị địa chỉ
        """
    )

    uploaded = st.file_uploader("📥 Tải file Excel (.xlsx) xuất từ iFast", type=["xlsx"])
    if uploaded is None:
        st.info("Vui lòng tải lên file Excel để bắt đầu kiểm tra.")
        return

    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Lỗi đọc file Excel: {e}")
        return

    st.subheader("🔍 Xem trước dữ liệu")
    with st.expander("Xem 5 dòng đầu"):
        st.dataframe(df.head())

    st.subheader("📊 Kết quả kiểm tra trùng")

    try:
        dup_df = check_duplicates(df)
    except Exception as e:
        st.error(f"Lỗi khi kiểm tra trùng: {e}")
        return

    if dup_df.empty:
        st.success("✅ Không phát hiện hồ sơ trùng hoặc nghi ngờ trùng.")
    else:
        st.error(f"⚠ Phát hiện {len(dup_df)} hồ sơ trùng hoặc nghi ngờ trùng.")
        st.dataframe(dup_df, use_container_width=True)

        # ===== TẢI VỀ DƯỚI DẠNG EXCEL .XLSX =====
        output = io.BytesIO()
        # cần thư viện openpyxl trong requirements.txt
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            dup_df.to_excel(writer, index=False, sheet_name="Duplicates")
        output.seek(0)

        st.download_button(
            label="⬇️ Tải danh sách trùng (Excel)",
            data=output,
            file_name="detected_duplicates.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


if __name__ == "__main__":  # pragma: no cover
    if st is not None:
        run_app()
    else:
        print("Đây là module cho Streamlit. Chạy bằng:\n  streamlit run duplicate_checker.py")
