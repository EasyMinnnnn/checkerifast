"""
duplicate_checker.py
====================

This module contains a Streamlit application for checking duplicate property
records prior to approval.  It supports two types of checks: land (“Đất ở”) and
apartment (“Căn hộ chung cư”).  The current implementation focuses on the land
dataset and demonstrates how to identify records in the “Phê duyệt” (submitted
for approval) stage that duplicate previously approved records.

Usage
-----
Run the script with Streamlit:

```
streamlit run duplicate_checker.py
```

Upload the exported Excel file.  The application will display a table of
entries flagged as duplicates along with the reasons (address match, exact
coordinate match or approximate coordinate match).

Notes
-----
This script is designed to be hosted on a platform such as GitHub and
integrated with Streamlit.  It does not perform any network operations.
"""

from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    # Streamlit is only required when running the application.  Wrapping in
    # try/except allows the module (and its functions) to be imported in
    # environments where Streamlit isn't installed (e.g., during unit tests).
    import streamlit as st  # type: ignore
except ImportError:
    st = None  # type: ignore


def standardize_address(row: pd.Series) -> str:
    """Construct a normalized key from the five address columns.

    The address fields are converted to uppercase strings and stripped of
    leading/trailing whitespace before concatenation.  Missing values are
    converted to the string ``"nan"``.

    Parameters
    ----------
    row: pd.Series
        A row from the DataFrame containing the address fields.

    Returns
    -------
    str
        A concatenated address key used for exact matching.
    """
    cols = [
        "Tỉnh/Thành phố",
        "Quận/Huyện/Thị xã",
        "Xã/Phường",
        "Đường/Phố",
        "Số nhà",
    ]
    values = []
    for col in cols:
        val = row.get(col, "")
        if pd.isna(val):
            val = "nan"
        val = str(val).strip().upper()
        values.append(val)
    return "||".join(values)


def parse_coords(coord: str) -> Optional[Tuple[float, float]]:
    """Parse a coordinate string into latitude and longitude floats.

    The input is expected to be in the form ``"lat,lon"`` with a comma
    separating the two values.  If parsing fails, ``None`` is returned.

    Parameters
    ----------
    coord: str
        A string representing a pair of latitude and longitude values.

    Returns
    -------
    Optional[Tuple[float, float]]
        A tuple ``(lat, lon)`` if parsing succeeds, otherwise ``None``.
    """
    if not coord or pd.isna(coord):
        return None
    coord = str(coord).strip()
    parts = coord.split(",")
    if len(parts) != 2:
        return None
    try:
        lat = float(parts[0])
        lon = float(parts[1])
        return lat, lon
    except ValueError:
        return None


def build_lookup(df: pd.DataFrame) -> Tuple[Dict[str, List[int]], Dict[Tuple[int, int], List[int]]]:
    """Build lookup dictionaries for address and truncated coordinates.

    For efficiency, this function constructs two dictionaries from the
    DataFrame of approved records (``"Hoàn thành"`` status):

    * ``addr_dict`` maps each normalized address key to a list of DataFrame
      indices containing that address.
    * ``coord_dict`` maps each coordinate rounded to 6 decimal places
      (represented as integers to avoid floating‐point key issues) to a list
      of DataFrame indices containing coordinates falling into that bucket.

    Parameters
    ----------
    df: pd.DataFrame
        A DataFrame containing the approved records.

    Returns
    -------
    Tuple[Dict[str, List[int]], Dict[Tuple[int, int], List[int]]]
        The address dictionary and coordinate dictionary, respectively.
    """
    addr_dict: Dict[str, List[int]] = {}
    coord_dict: Dict[Tuple[int, int], List[int]] = {}
    for idx, row in df.iterrows():
        # Build address key
        addr_key = standardize_address(row)
        addr_dict.setdefault(addr_key, []).append(idx)

        # Build coordinate key (rounded to 6 decimals, scaled to integers)
        coord = parse_coords(row.get("Tọa độ"))
        if coord:
            lat, lon = coord
            # Multiply by 1e6 and round to int to avoid float precision issues
            lat_key = int(round(lat * 1_000_000))
            lon_key = int(round(lon * 1_000_000))
            coord_dict.setdefault((lat_key, lon_key), []).append(idx)
    return addr_dict, coord_dict


def coordinate_match(
    pd_coord: str, ht_coords: List[str], threshold: float = 1e-6
) -> Optional[str]:
    """Determine whether a proposed coordinate matches any approved coordinate.

    Two matching strategies are considered:

    1. **Exact match**: both latitude and longitude values differ by no more
       than ``threshold``.
    2. **Prefix match**: either coordinate string is a prefix of the other
       (this covers cases where extra digits were appended to avoid detection).

    If a match is found, a descriptive reason string is returned.  If no
    match is found, ``None`` is returned.

    Parameters
    ----------
    pd_coord: str
        The coordinate string from the record under review ("Phê duyệt").
    ht_coords: List[str]
        A list of coordinate strings from previously approved records with
        matching truncated values.
    threshold: float, optional
        The maximum absolute difference between latitudes and longitudes to
        consider an exact match.  Defaults to ``1e-6``.

    Returns
    -------
    Optional[str]
        A reason string if a match is found, otherwise ``None``.
    """
    candidate = parse_coords(pd_coord)
    if not candidate:
        return None
    pd_lat, pd_lon = candidate
    for ht_coord_str in ht_coords:
        ht = parse_coords(ht_coord_str)
        if not ht:
            continue
        ht_lat, ht_lon = ht
        # Exact match within threshold
        if abs(ht_lat - pd_lat) <= threshold and abs(ht_lon - pd_lon) <= threshold:
            return "Tọa độ trùng 100%"
        # Prefix match: check if either string starts with the other
        if pd_coord and ht_coord_str:
            a = pd_coord.strip()
            b = ht_coord_str.strip()
            if a.startswith(b) or b.startswith(a):
                return "Tọa độ trùng gần chính xác (khớp theo tiền tố)"
    return None


def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Check for duplicate records between "Phê duyệt" and "Hoàn thành" entries.

    The function filters records in the "Phê duyệt" stage and compares them
    against records in the "Hoàn thành" stage.  A duplicate is flagged if
    either:

    * All five address fields (province/city, district/town, ward, street,
      house number) match exactly, or
    * The coordinates match exactly or approximately as defined in
      :func:`coordinate_match`.

    For each flagged record, the function includes the relevant address
    components, coordinates and the reasons for duplication.

    Parameters
    ----------
    df: pd.DataFrame
        A DataFrame containing the full exported dataset.

    Returns
    -------
    pd.DataFrame
        A DataFrame of flagged duplicate records with the following columns:

        * ``ID`` – the record identifier.
        * ``Tỉnh/Thành phố``, ``Quận/Huyện/Thị xã``, ``Xã/Phường``,
          ``Đường/Phố``, ``Số nhà`` – the address components.
        * ``Tọa độ`` – the coordinate string.
        * ``Lý do trùng`` – a comma‐separated list of reasons for flagging.
    """
    # Separate "Hoàn thành" and "Phê duyệt"
    ht_df = df[df["Giai đoạn hiện tại"] == "Hoàn thành"].copy()
    pd_df = df[df["Giai đoạn hiện tại"] == "Phê duyệt"].copy()

    # Build lookups from approved records
    addr_dict, coord_dict = build_lookup(ht_df)

    # Precompute coordinate strings for each index in the approved set
    ht_coord_strings: Dict[int, str] = {
        idx: str(ht_df.loc[idx, "Tọa độ"]) for idx in ht_df.index
    }

    flagged: List[Dict[str, object]] = []
    for idx, row in pd_df.iterrows():
        reasons: List[str] = []
        addr_key = standardize_address(row)
        # Address match
        if addr_key in addr_dict:
            reasons.append(
                "Trùng 5 thông tin địa chỉ (Tỉnh/Thành phố, Quận/Huyện/Thị xã, "
                "Xã/Phường, Đường/Phố, Số nhà)"
            )
        # Coordinate match
        coord = row.get("Tọa độ")
        coord_parsed = parse_coords(coord)
        if coord_parsed:
            # Round the candidate coordinates to build the key
            lat, lon = coord_parsed
            lat_key = int(round(lat * 1_000_000))
            lon_key = int(round(lon * 1_000_000))
            ht_indices = coord_dict.get((lat_key, lon_key), [])
            if ht_indices:
                ht_coords_list = [ht_coord_strings[i] for i in ht_indices]
                coord_reason = coordinate_match(str(coord), ht_coords_list)
                if coord_reason:
                    reasons.append(coord_reason)
        # If any reasons, record the duplicate
        if reasons:
            flagged.append(
                {
                    "ID": row.get("ID"),
                    "Tỉnh/Thành phố": row.get("Tỉnh/Thành phố"),
                    "Quận/Huyện/Thị xã": row.get("Quận/Huyện/Thị xã"),
                    "Xã/Phường": row.get("Xã/Phường"),
                    "Đường/Phố": row.get("Đường/Phố"),
                    "Số nhà": row.get("Số nhà"),
                    "Tọa độ": row.get("Tọa độ"),
                    "Lý do trùng": ", ".join(sorted(set(reasons))),
                }
            )
    return pd.DataFrame(flagged)


def main() -> None:
    """Entry point for the Streamlit application."""
    if st is None:
        raise RuntimeError(
            "Streamlit không được cài đặt. Vui lòng cài đặt streamlit để chạy ứng dụng."
        )

    st.set_page_config(
        page_title="Kiểm tra hồ sơ trùng", page_icon="🔍", layout="wide"
    )
    st.title("🔍 Công cụ kiểm tra hồ sơ trùng trước khi phê duyệt")
    st.markdown(
        """
        ### Hướng dẫn sử dụng

        1. Chọn loại kiểm tra (Đất ở hoặc Căn hộ chung cư).
        2. Tải lên file Excel chứa dữ liệu xuất theo mẫu hệ thống.
        3. Ứng dụng sẽ lọc các hồ sơ đang ở giai đoạn **Phê duyệt** và so sánh
           với các hồ sơ đã **Hoàn thành** để phát hiện trùng lặp theo quy tắc:
           * Trùng toàn bộ 5 thông tin địa chỉ: **Tỉnh/Thành phố**, **Quận/Huyện/Thị xã**, **Xã/Phường**, **Đường/Phố**, **Số nhà**.
           * Trùng tọa độ chính xác hoặc trùng gần chính xác (ví dụ: "12.670322,108.101062"
             và "12.6703222,108.1010623").
        4. Các hồ sơ nghi ngờ trùng sẽ được liệt kê kèm lý do để cán bộ kiểm soát xem xét.
        """
    )

    # Select the type of asset
    asset_type = st.radio(
        "Chọn loại kiểm tra:", ["Đất ở", "Căn hộ chung cư"], index=0
    )
    uploaded_file = st.file_uploader(
        "Tải lên file Excel xuất từ hệ thống", type=["xlsx"]
    )
    if uploaded_file is not None:
        try:
            # Read Excel file
            df = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"Không thể đọc file Excel: {e}")
            return
        # Filter by asset type (if necessary)
        # Currently we only implement logic for 'Đất ở'
        if asset_type == "Đất ở":
            with st.spinner("Đang kiểm tra hồ sơ trùng..."):
                result_df = check_duplicates(df)
            st.success(
                f"Đã phát hiện {len(result_df)} hồ sơ trùng trong tổng số "
                f"{len(df[df['Giai đoạn hiện tại'] == 'Phê duyệt'])} hồ sơ đang chờ phê duyệt."
            )
            if not result_df.empty:
                st.dataframe(result_df)
                # Offer download
                csv = result_df.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label="Tải danh sách trùng (CSV)",
                    data=csv,
                    file_name="ho_so_trung.csv",
                    mime="text/csv",
                )
            else:
                st.info(
                    "Không phát hiện hồ sơ trùng theo quy tắc hiện tại."
                )
        else:
            st.warning(
                "Chức năng kiểm tra Căn hộ chung cư đang được phát triển. Vui lòng chọn Đất ở."
            )


if __name__ == "__main__":
    main()
