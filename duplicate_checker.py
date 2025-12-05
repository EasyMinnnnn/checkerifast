"""
duplicate_checker.py

Công cụ Streamlit để kiểm tra trùng hồ sơ trước khi phê duyệt.
- Tập trung vào dữ liệu ĐẤT Ở (Land) import từ file Excel iFast.
- So sánh các hồ sơ đang ở trạng thái "Phê duyệt" với các hồ sơ "Hoàn thành".
- Rule trùng:
    + Trùng 5 thông tin địa chỉ:
      (Tỉnh/Thành phố, Quận/Huyện/Thị xã, Xã/Phường, Đường/Phố, Số nhà)
    + Và/hoặc trùng tọa độ:
      - Tọa độ trùng 100%
      - Hoặc trùng gần đúng (cắt bớt vài số ở cuối để bắt case kiểu
        "12.670322,108.101062" và "12.6703222,108.1010623")

Kết quả trả về:
- ID hồ sơ Phê duyệt
- Địa chỉ tài sản
- Lý do trùng
- ID trùng với (các ID Hoàn thành liên quan)
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
    "Tỉnh/Thành phố",
    "Quận/Huyện/Thị xã",
    "Xã/Phường",
    "Đường/Phố",
    "Số nhà",
]


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


def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kiểm tra trùng giữa:
    - Các hồ sơ 'Phê duyệt'  (đang trình)
    - Và hồ sơ 'Hoàn thành' (đã phê duyệt trước đó)

    Trả về DataFrame gồm:
    - ID_phe_duyet : ID của dòng đang ở trạng thái Phê duyệt
    - Địa chỉ tài sản
    - Lý do trùng
    - ID trùng với : danh sách ID Hoàn thành trùng (ngăn cách bằng '; ')
    """

    if "Giai đoạn hiện tại" not in df.columns:
        raise ValueError("Thiếu cột 'Giai đoạn hiện tại' trong file Excel.")

    # Tách 2 nhóm trạng thái
    hoan_thanh = df[df["Giai đoạn hiện tại"] == "Hoàn thành"].copy()
    phe_duyet = df[df["Giai đoạn hiện tại"] == "Phê duyệt"].copy()

    # Chuẩn hóa key địa chỉ
    for sub_df in (hoan_thanh, phe_duyet):
        for col in ADDR_COLS:
            if col not in sub_df.columns:
                raise ValueError(f"Thiếu cột '{col}' trong file Excel.")
        sub_df["addr_key"] = sub_df.apply(build_addr_key, axis=1)

    # Chuẩn hóa tọa độ
    if "Tọa độ" not in df.columns:
        raise ValueError("Thiếu cột 'Tọa độ' trong file Excel.")

    hoan_thanh["coord_norm"] = hoan_thanh["Tọa độ"].apply(normalize_coord)
    phe_duyet["coord_norm"] = phe_duyet["Tọa độ"].apply(normalize_coord)

    # Build group lookup cho Hoàn thành
    addr_groups: Dict[str, List[Any]] = (
        hoan_thanh.groupby("addr_key")["ID"].apply(list).to_dict()
    )
    coord_groups: Dict[str, List[Any]] = (
        hoan_thanh.groupby("coord_norm")["ID"].apply(list).to_dict()
    )

    results: List[Dict[str, Any]] = []

    for _, row in phe_duyet.iterrows():
        duplicate_ids: set[Any] = set()
        reasons: List[str] = []

        addr_key = row.get("addr_key")
        coord_key = row.get("coord_norm")

        # Rule 1: Trùng 5 thông tin địa chỉ
        if addr_key in addr_groups and addr_key:
            duplicate_ids.update(addr_groups[addr_key])
            reasons.append(
                "Trùng 5 thông tin địa chỉ "
                "(Tỉnh/Thành phố, Quận/Huyện/Thị xã, Xã/Phường, Đường/Phố, Số nhà)"
            )

        # Rule 2: Trùng tọa độ (chính xác hoặc gần đúng)
        if coord_key in coord_groups and coord_key:
            duplicate_ids.update(coord_groups[coord_key])
            reasons.append("Tọa độ trùng 100% hoặc gần đúng")

        # Nếu có bất kỳ rule nào khớp → đây là bản trùng
        if duplicate_ids:
            results.append(
                {
                    "ID_phe_duyet": row.get("ID"),
                    "Địa chỉ tài sản": row.get("Địa chỉ tài sản"),
                    "Lý do trùng": ", ".join(reasons),
                    "ID trùng với": "; ".join(str(x) for x in sorted(duplicate_ids)),
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

        **Rule kiểm tra trùng:**
        - Trùng 5 thông tin địa chỉ  
          *(Tỉnh/Thành phố, Quận/Huyện/Thị xã, Xã/Phường, Đường/Phố, Số nhà)*  
        - Và/hoặc trùng tọa độ (kể cả khi có thêm/bớt vài số thập phân phía sau)
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
