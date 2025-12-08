"""
duplicate_checker.py

Công cụ Streamlit để kiểm tra trùng hồ sơ trước khi phê duyệt.
- Tập trung vào dữ liệu ĐẤT Ở (Land) import từ file Excel iFast.
- So sánh các hồ sơ đang ở trạng thái "Phê duyệt" với các hồ sơ "Hoàn thành".

Rule trùng chính:

1) TRÙNG TỌA ĐỘ (ưu tiên, chắc chắn):
   - Tọa độ chuẩn hóa (coord_norm) trùng nhau
   - Và 'Thời điểm thu thập thông tin' của hồ sơ Phê duyệt > hồ sơ Hoàn thành
   → Luôn gắn nhãn: CẢNH BÁO TRÙNG
   (Không cần xét Người tạo)

2) TRÙNG ĐỊA CHỈ (5 cột W,X,Y,Z,AE):
   - Trùng 5 thông tin:
        Tỉnh/Thành phố
        Quận/Huyện/Thị xã
        Xã/Phường
        Đường/Phố
        Số nhà
   - Và 'Thời điểm thu thập thông tin' của hồ sơ Phê duyệt > hồ sơ Hoàn thành
   - Nếu cùng Người tạo  → CẢNH BÁO TRÙNG
   - Nếu khác Người tạo → NGHI NGỜ TRÙNG

Kết quả hiển thị:
- ID                : ID hồ sơ Phê duyệt
- Người tạo         : Người tạo hồ sơ Phê duyệt
- Lý do trùng       : Cảnh báo / Nghi ngờ + mô tả chi tiết
- Địa chỉ/Tọa độ trùng:
    + Nếu trùng địa chỉ → hiển thị đầy đủ địa chỉ: Số nhà – Đường – Xã – Quận – Tỉnh
    + Nếu trùng tọa độ → hiển thị tọa độ
- ID trùng          : các ID Hoàn thành trùng (ngăn cách '; ')
- Người tạo trùng   : Người tạo của các hồ sơ Hoàn thành trùng
"""

from __future__ import annotations

import io
from typing import Optional, List, Dict, Any

import pandas as pd

# Cho phép import module này ở môi trường không có streamlit (vd: test)
try:
    import streamlit as st  # type: ignore
except ImportError:  # pragma: no cover
    st = None  # type: ignore


# ==========================
#  Core checking logic
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


def build_addr_key(row: pd.Series) -> str:
    """Chuẩn hóa 5 thông tin địa chỉ thành 1 key để so sánh trùng."""
    parts: List[str] = []
    for col in ADDR_COLS:
        val = str(row.get(col, "")).strip().lower()
        parts.append(val)
    return "||".join(parts)


def normalize_coord(value: Any, max_len: int = 8) -> Optional[str]:
    """
    Chuẩn hóa tọa độ:
    - Tách lat, lon theo dấu ','
    - Cắt bớt độ dài mỗi phần để bắt được case nhập thêm số lẻ phía sau.
      Ví dụ:
        '12.670322,108.101062'
        '12.6703222,108.1010623'
      Sau chuẩn hóa đều thành:
        '12.670322,108.101062'
    """
    if pd.isna(value):
        return None

    try:
        text = str(value)
        lat_raw, lon_raw = text.split(",")
        lat = lat_raw.strip()
        lon = lon_raw.strip()
        lat = lat[:max_len]
        lon = lon[:max_len]
        return f"{lat},{lon}"
    except Exception:
        return None


def format_address(row: pd.Series) -> str:
    """
    Hiển thị địa chỉ theo thứ tự:
    Số nhà – Đường/Phố – Xã/Phường – Quận/Huyện/Thị xã – Tỉnh/Thành phố
    (tương ứng AE – Z – Y – X – W)
    """
    num = str(row.get("Số nhà", "")).strip()
    street = str(row.get("Đường/Phố", "")).strip()
    ward = str(row.get("Xã/Phường", "")).strip()
    district = str(row.get("Quận/Huyện/Thị xã", "")).strip()
    province = str(row.get("Tỉnh/Thành phố", "")).strip()

    parts = [p for p in [num, street, ward, district, province] if p]
    return " – ".join(parts)


def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kiểm tra trùng giữa:
    - Các hồ sơ 'Phê duyệt'  (đang trình)
    - Và hồ sơ 'Hoàn thành' (đã phê duyệt trước đó)

    Rule chi tiết:

    1) Trùng tọa độ:
       - coord_norm giống nhau
       - Thời điểm thu thập (time_norm) của Phê duyệt > Hoàn thành
       → luôn CẢNH BÁO TRÙNG

    2) Trùng địa chỉ (5 cột), thời điểm Phê duyệt > Hoàn thành:
       - Nếu cùng Người tạo → CẢNH BÁO TRÙNG
       - Nếu khác Người tạo → NGHI NGỜ TRÙNG

    Trả về DataFrame gồm:
    - ID                : ID hồ sơ Phê duyệt
    - Người tạo         : Người tạo hồ sơ Phê duyệt
    - Lý do trùng       : Cảnh báo / Nghi ngờ + mô tả
    - Địa chỉ/Tọa độ trùng
    - ID trùng          : các ID Hoàn thành trùng
    - Người tạo trùng   : người tạo tương ứng các bản Hoàn thành
    """

    # Kiểm tra các cột bắt buộc
    if "Giai đoạn hiện tại" not in df.columns:
        raise ValueError("Thiếu cột 'Giai đoạn hiện tại' trong file Excel.")
    if CREATOR_COL not in df.columns:
        raise ValueError(f"Thiếu cột '{CREATOR_COL}' trong file Excel.")
    if TIME_COL not in df.columns:
        raise ValueError(f"Thiếu cột '{TIME_COL}' trong file Excel.")
    if COORD_COL not in df.columns:
        raise ValueError(f"Thiếu cột '{COORD_COL}' trong file Excel.")
    for col in ADDR_COLS:
        if col not in df.columns:
            raise ValueError(f"Thiếu cột '{col}' trong file Excel.")

    # Tách 2 nhóm trạng thái
    hoan_thanh = df[df["Giai đoạn hiện tại"] == "Hoàn thành"].copy()
    phe_duyet = df[df["Giai đoạn hiện tại"] == "Phê duyệt"].copy()

    # Chuẩn hóa key địa chỉ
    hoan_thanh["addr_key"] = hoan_thanh.apply(build_addr_key, axis=1)
    phe_duyet["addr_key"] = phe_duyet.apply(build_addr_key, axis=1)

    # Chuẩn hóa tọa độ
    hoan_thanh["coord_norm"] = hoan_thanh[COORD_COL].apply(normalize_coord)
    phe_duyet["coord_norm"] = phe_duyet[COORD_COL].apply(normalize_coord)

    # Chuẩn hóa thời gian (dd/mm/yyyy → dayfirst=True)
    hoan_thanh["time_norm"] = pd.to_datetime(
        hoan_thanh[TIME_COL], dayfirst=True, errors="coerce"
    )
    phe_duyet["time_norm"] = pd.to_datetime(
        phe_duyet[TIME_COL], dayfirst=True, errors="coerce"
    )

    # Build group lookup cho Hoàn thành (groups trả về dict: key -> Index)
    addr_groups = hoan_thanh.groupby("addr_key").groups
    coord_groups = hoan_thanh.groupby("coord_norm").groups

    results: List[Dict[str, Any]] = []

    for _, row in phe_duyet.iterrows():
        duplicate_ids: set[Any] = set()
        duplicate_creators: set[str] = set()
        severity_levels: set[str] = set()  # {"Cảnh báo trùng", "Nghi ngờ trùng"}
        reason_details: List[str] = []

        addr_key = row.get("addr_key")
        coord_key = row.get("coord_norm")
        creator = str(row.get(CREATOR_COL, "")).strip()
        row_time = row.get("time_norm")

        # ==============
        # Rule 2: Trùng địa chỉ
        # ==============
        addr_indices = addr_groups.get(addr_key)
        if addr_indices is not None and len(addr_indices) > 0:
            candidates_addr = hoan_thanh.loc[addr_indices].copy()

            # Chỉ lấy các hồ sơ Hoàn thành có thời điểm < Phê duyệt
            if pd.notna(row_time):
                candidates_addr = candidates_addr[
                    (candidates_addr["time_norm"].notna())
                    & (candidates_addr["time_norm"] < row_time)
                ]

            if not candidates_addr.empty:
                same_creator_ids = candidates_addr[
                    candidates_addr[CREATOR_COL].astype(str).str.strip() == creator
                ]
                diff_creator_ids = candidates_addr[
                    candidates_addr[CREATOR_COL].astype(str).str.strip() != creator
                ]

                if not same_creator_ids.empty:
                    severity_levels.add("Cảnh báo trùng")
                    duplicate_ids.update(same_creator_ids["ID"].tolist())
                    duplicate_creators.update(
                        same_creator_ids[CREATOR_COL].astype(str).str.strip().tolist()
                    )
                    reason_details.append(
                        "Cùng Người tạo và trùng 5 thông tin địa chỉ "
                        "(Tỉnh/Thành phố, Quận/Huyện/Thị xã, Xã/Phường, Đường/Phố, Số nhà)"
                    )

                if not diff_creator_ids.empty:
                    severity_levels.add("Nghi ngờ trùng")
                    duplicate_ids.update(diff_creator_ids["ID"].tolist())
                    duplicate_creators.update(
                        diff_creator_ids[CREATOR_COL].astype(str).str.strip().tolist()
                    )
                    reason_details.append(
                        "Khác Người tạo nhưng trùng 5 thông tin địa chỉ "
                        "(Tỉnh/Thành phố, Quận/Huyện/Thị xã, Xã/Phường, Đường/Phố, Số nhà)"
                    )

        # ==============
        # Rule 1: Trùng tọa độ (luôn Cảnh báo)
        # ==============
        coord_indices = coord_groups.get(coord_key)
        if coord_indices is not None and len(coord_indices) > 0:
            candidates_coord = hoan_thanh.loc[coord_indices].copy()

            # Chỉ lấy Hoàn thành có thời điểm < Phê duyệt
            if pd.notna(row_time):
                candidates_coord = candidates_coord[
                    (candidates_coord["time_norm"].notna())
                    & (candidates_coord["time_norm"] < row_time)
                ]

            if not candidates_coord.empty:
                severity_levels.add("Cảnh báo trùng")
                duplicate_ids.update(candidates_coord["ID"].tolist())
                duplicate_creators.update(
                    candidates_coord[CREATOR_COL].astype(str).str.strip().tolist()
                )
                reason_details.append("Trùng tọa độ (Tọa độ trùng 100% hoặc gần đúng)")

        # Nếu có bất kỳ rule nào khớp → đây là bản trùng
        if duplicate_ids:
            # Xác định mức độ tổng hợp: nếu có Cảnh báo thì ưu tiên
            if "Cảnh báo trùng" in severity_levels:
                severity_label = "Cảnh báo trùng"
            else:
                severity_label = "Nghi ngờ trùng"

            # Thông tin trùng: địa chỉ +/hoặc tọa độ
            info_duplicated: List[str] = []
            addr_text = format_address(row)
            if addr_text:
                info_duplicated.append(f"Địa chỉ: {addr_text}")
            coord_text = str(row.get(COORD_COL, "")).strip()
            if coord_text:
                info_duplicated.append(f"Tọa độ: {coord_text}")

            results.append(
                {
                    "ID": row.get("ID"),
                    "Người tạo": creator,
                    "Lý do trùng": f"{severity_label} – " + " ; ".join(reason_details),
                    "Địa chỉ/Tọa độ trùng": " | ".join(info_duplicated),
                    "ID trùng": "; ".join(str(x) for x in sorted(duplicate_ids)),
                    "Người tạo trùng": "; ".join(sorted(duplicate_creators)),
                }
            )

    return pd.DataFrame(results)


# ==========================
#  Streamlit App
# ==========================

def run_app() -> None:  # pragma: no cover - chỉ chạy trên Streamlit
    if st is None:
        raise RuntimeError(
            "Streamlit chưa được cài. Hãy cài bằng:\n"
            "    pip install streamlit"
        )

    st.set_page_config(
        page_title="iFast Duplicate Checker",
        layout="wide",
    )

    st.title("🧮 iFast – Công cụ kiểm tra trùng hồ sơ")

    st.markdown(
        """
        Công cụ này giúp kiểm tra **hồ sơ Đất ở** đang ở trạng thái
        **“Phê duyệt”** xem có trùng với các hồ sơ **“Hoàn thành”** trước đó hay không.

        **Rule kiểm tra trùng (tóm tắt):**
        - Trùng 5 thông tin địa chỉ  
          *(Tỉnh/Thành phố, Quận/Huyện/Thị xã, Xã/Phường, Đường/Phố, Số nhà)*  
          + Cùng Người tạo → **Cảnh báo trùng**  
          + Khác Người tạo → **Nghi ngờ trùng**  

        - Trùng tọa độ (kể cả khi có thêm/bớt vài số thập phân phía sau),
          và thời điểm thu thập sau hồ sơ Hoàn thành → **Cảnh báo trùng**
        """
    )

    st.sidebar.header("⚙️ Cấu hình")
    asset_type = st.sidebar.selectbox(
        "Loại tài sản",
        ["Đất ở", "Căn hộ chung cư (chưa hỗ trợ)"],
        index=0,
    )

    uploaded = st.file_uploader(
        "📥 Tải lên file Excel xuất từ iFast (.xlsx)",
        type=["xlsx"],
    )

    if uploaded is None:
        st.info("Vui lòng tải lên file Excel để bắt đầu kiểm tra.")
        return

    # Đọc file Excel
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Lỗi đọc file Excel: {e}")
        return

    st.subheader("🔍 Thông tin tổng quan dữ liệu")
    with st.expander("Xem trước vài dòng đầu"):
        st.dataframe(df.head())

    if asset_type != "Đất ở":
        st.warning("Hiện tại mới hỗ trợ rule cho **Đất ở**. Các loại khác sẽ được bổ sung sau.")
        return

    # Thực hiện check trùng
    try:
        dup_df = check_duplicates(df)
    except Exception as e:
        st.error(f"Lỗi khi kiểm tra trùng: {e}")
        return

    st.subheader("📊 Kết quả kiểm tra trùng")

    if dup_df.empty:
        st.success("✅ Không phát hiện hồ sơ Phê duyệt nào trùng với Hoàn thành.")
    else:
        st.write(f"🔴 Phát hiện **{len(dup_df)}** hồ sơ Phê duyệt có dấu hiệu trùng.")
        st.dataframe(dup_df, use_container_width=True)

        # Nút download CSV
        buffer = io.StringIO()
        dup_df.to_csv(buffer, index=False)
        st.download_button(
            label="⬇️ Tải về danh sách trùng (CSV)",
            data=buffer.getvalue(),
            file_name="detected_duplicates.csv",
            mime="text/csv",
        )


# Khi chạy bằng `streamlit run duplicate_checker.py`
if __name__ == "__main__":  # pragma: no cover
    if st is not None:
        run_app()
    else:
        # Cho phép chạy python duplicate_checker.py để test nhanh không cần streamlit
        print(
            "Module loaded. Đây là file dành cho Streamlit.\n"
            "Để chạy app, dùng:\n"
            "    streamlit run duplicate_checker.py"
        )
