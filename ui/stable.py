# -*- coding: utf-8 -*-
# Copyright 2024-2025 Streamlit Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import json
import redis
import pandas as pd
import streamlit as st
import altair as alt
import numpy as np
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

st.set_page_config(page_title="Feedback Analysis Dashboard", page_icon=":package:", layout="wide")

"""
# :material/storage: Feedback Analysis Dashboard

채널과 기간을 선택하면 Mnet Plus App에 대한 User 피드백을 실시간으로 저장하는 redis 서버에서 조회합니다.

"""

cols = st.columns([1, 3])

@st.cache_resource(show_spinner=False)
def get_client(host="localhost", port=6379, db=0, decode_responses=True):
    return redis.StrictRedis(host=host, port=port, db=db, decode_responses=decode_responses)

r = get_client()

DEFAULT_CHANNELS = ["google_play"]
DELIM = ":"
SUFFIX_ALL = "*"
SCAN_COUNT = 1000

def _get_qp_list(name: str, fallback: list[str]) -> list[str]:
    raw = st.query_params.get(name, ",".join(fallback))
    return [x for x in raw.split(",") if x]

def _get_qp_str(name: str, fallback: str) -> str:
    return st.query_params.get(name, fallback)

if "channels_input" not in st.session_state:
    st.session_state.channels_input = _get_qp_list("channel", DEFAULT_CHANNELS[:1])
if "horizon_input" not in st.session_state:
    st.session_state.horizon_input = _get_qp_str("horizon", "All")

def update_query_params():
    if st.session_state.channels_input:
        st.query_params["channel"] = ",".join(st.session_state.channels_input)
    else:
        st.query_params.pop("channel", None)
    st.query_params["horizon"] = st.session_state.horizon_input

def scan_all(client: redis.StrictRedis, match_pattern: str, count: int = SCAN_COUNT) -> list[str]:
    cursor = 0
    all_keys: list[str] = []
    while True:
        cursor, keys = client.scan(cursor=cursor, match=match_pattern, count=count)
        all_keys.extend(keys)
        if cursor == 0:
            break
    return all_keys

def read_value_by_type(client: redis.StrictRedis, key: str):
    t = client.type(key)
    if t == "string":
        val = client.get(key)
        try:
            return json.loads(val)
        except Exception:
            return val
    if t == "hash":
        data = client.hgetall(key)
        out = {}
        for k, v in data.items():
            try:
                out[k] = json.loads(v)
            except Exception:
                out[k] = v
        return out
    if t == "list":
        return client.lrange(key, 0, -1)
    if t == "set":
        return list(client.smembers(key))
    if t == "zset":
        return [{"member": m, "score": s} for m, s in client.zrange(key, 0, -1, withscores=True)]
    if t == "stream":
        entries = client.xrevrange(key, count=200)
        return [{"id": _id, **fields} for _id, fields in entries]
    return None

# ----------------------------
# 🔁 기간별 접두사 생성 로직
# ----------------------------
def _months_between(start_ym: str, end_ym: str) -> list[str]:
    """start_ym(YYYYMM)부터 end_ym(YYYYMM)까지 모든 YYYYMM 리스트"""
    sy, sm = int(start_ym[:4]), int(start_ym[4:])
    ey, em = int(end_ym[:4]), int(end_ym[4:])
    y, m = sy, sm
    out = []
    while (y < ey) or (y == ey and m <= em):
        out.append(f"{y:04d}{m:02d}")
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out

def prefixes_for_horizon(horizon: str, today: date):
    # All: 2022-10 ~ 오늘(YYYYMM)까지 월 접두사 생성 (YYYYMM*) → per_day=False
    if horizon == "All":
        start_ym = "202210"
        end_ym = today.strftime("%Y%m")
        yms = _months_between(start_ym, end_ym)
        return yms, False

    # 1 Year: 올해 1월 ~ 오늘(YYYYMM)까지 월 접두사 (연도 누적: YTD) → per_day=False
    if horizon == "1 Year":
        start_ym = f"{today.year}01"          # 예: 2025-01
        end_ym = today.strftime("%Y%m")       # 예: 2025-10
        yms = _months_between(start_ym, end_ym)
        return yms, False

    # 1 Month: '해당 월 1일~말일'을 일 단위(YYYYMMDD)로 생성 → per_day=True
    if horizon == "1 Month":
        month_start = today.replace(day=1)
        month_end = (month_start + relativedelta(months=1)) - timedelta(days=1)
        days = (month_end - month_start).days + 1
        ymds = [(month_start + timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]
        return ymds, True

    # 6 Months: 오늘 기준 '지난 6개월' 월 접두사 (오래된→최신, 예: 202505 ~ 202510) → per_day=False
    if horizon == "6 Months":
        yms = [(today - relativedelta(months=i)).strftime("%Y%m") for i in range(5, -1, -1)]
        return yms, False

    # (요청에 따라 '3 Months', '2 Weeks' 제거; '1 Week'만 유지)
    if horizon == "1 Week":
        ymds = [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(6, -1, -1)]
        return ymds, True

    return None, False

top_left_cell = cols[0].container(border=True, height="stretch", vertical_alignment="center")

with top_left_cell:
    channels = st.multiselect(
        "Channels",
        options=sorted(set(DEFAULT_CHANNELS) | set(st.session_state.channels_input)),
        default=st.session_state.channels_input,
        placeholder="조회할 채널을 선택/추가 (예: mnetplus)",
        accept_new_options=True,
        key="channels_input",
        on_change=update_query_params
    )

with top_left_cell:
    horizon_map = {
        "All": None,
        "1 Year": "1yr",
        "6 Months": "6mo",
        "1 Month": "1mo",
        "1 Week": "1w",
    }
    horizon = st.pills(
        "Time horizon",
        options=list(horizon_map.keys()),
        default=st.session_state.horizon_input,
        key="horizon_input",
        on_change=update_query_params
    )

if not channels:
    top_left_cell.info("조회할 채널을 하나 이상 선택하세요.", icon=":material/info:")
    st.stop()

right_cell = cols[1].container(border=True, height="stretch", vertical_alignment="center")

today = date.today()
prefixes, per_day = prefixes_for_horizon(horizon, today)

def build_patterns(channels: list[str], prefixes, per_day: bool, horizon: str | None = None):
    patterns = []
    if prefixes is None:
        for ch in channels:
            patterns.append(f"review:{ch}{DELIM}{SUFFIX_ALL}")
        return patterns
    for ch in channels:
        for p in prefixes:
            patterns.append(f"review:{ch}{DELIM}{p}{SUFFIX_ALL}")
    return patterns

patterns = build_patterns(channels, prefixes, per_day, horizon=horizon)

@st.cache_data(show_spinner=False)
def run_query(patterns: list[str]):
    client = get_client()
    all_keys = []
    for p in patterns:
        print("query : ", p)
        all_keys.extend(scan_all(client, p, count=SCAN_COUNT))
    all_keys = sorted(set(all_keys))

    rows, type_counter, error_count = [], {}, 0
    for k in all_keys:
        try:
            t = client.type(k)
            v = read_value_by_type(client, k)
            rows.append({"key": k, "type": t, "value": v})
            type_counter[t] = type_counter.get(t, 0) + 1
        except Exception as e:
            rows.append({"key": k, "type": "error", "value": f"⚠️ {e}"})
            error_count += 1
    df = pd.DataFrame(rows)
    return all_keys, df, type_counter, error_count

# ----------------------------
# 📈 그래프 헬퍼 (All 월 집계용)
# ----------------------------
def _channel_from_key(key: str) -> str | None:
    parts = key.split(DELIM, 2)  # ["review", "{channel}", "{rest}"]
    return parts[1] if len(parts) >= 2 else None

def _to_yyyymm_from_value(v) -> str | None:
    """value.review_created_at → YYYYMM으로 변환"""
    if not isinstance(v, dict):
        return None
    raw = v.get("review_created_at")
    if raw is None:
        return None
    try:
        # epoch 숫자(s/ms) 처리
        if isinstance(raw, (int, float)) or (isinstance(raw, str) and raw.isdigit()):
            num = int(raw)
            unit = "ms" if num >= 10**12 else "s"
            ts = pd.to_datetime(num, unit=unit, errors="coerce")
            if pd.isna(ts):
                return None
            return ts.strftime("%Y%m")
    except Exception:
        pass
    # 문자열에서 숫자만 추출 후 앞 8자리 → YYYYMM
    digits = "".join(re.findall(r"\d", str(raw)))
    if len(digits) >= 8:
        return digits[:6]
    return None

def build_month_series_from_values_for_all(df: pd.DataFrame, channels: list[str]) -> pd.DataFrame:
    rows = []
    if df is None or df.empty:
        return pd.DataFrame(columns=["Prefix", "Channel", "Count"])
    for _, row in df.iterrows():
        ch = _channel_from_key(row.get("key", ""))
        if not ch or ch not in channels:
            continue
        ym = _to_yyyymm_from_value(row.get("value"))
        if not ym:
            continue
        rows.append({"Prefix": ym, "Channel": ch})
    if not rows:
        return pd.DataFrame(columns=["Prefix", "Channel", "Count"])
    return (
        pd.DataFrame(rows)
        .groupby(["Prefix", "Channel"])
        .size()
        .reset_index(name="Count")
        .sort_values(["Prefix", "Channel"])
        .reset_index(drop=True)
    )

# ---- 접두사(YYYYMM / YYYYMMDD)별 집계 (per_day에 맞춤) ----
def build_prefix_series(keys: list[str], per_day: bool, prefixes: list[str] | None) -> pd.DataFrame:
    """
    반환: Prefix(str), Channel(str), Count(int)
    - per_day=True  -> YYYYMMDD 단위
    - per_day=False -> YYYYMM 단위
    """
    if not keys:
        return pd.DataFrame(columns=["Prefix", "Channel", "Count"])

    pat = re.compile(r"^(\d{8})" if per_day else r"^(\d{6})")
    rows = []
    for k in keys:
        parts = k.split(DELIM, 2)  # ["review", "{channel}", "{rest}"]
        if len(parts) < 3:
            continue
        ch, rest = parts[1], parts[2]

        m = pat.match(rest)
        if not m:
            continue

        prefix = m.group(1)  # YYYYMMDD or YYYYMM
        rows.append({"Prefix": prefix, "Channel": ch})

    if not rows:
        return pd.DataFrame(columns=["Prefix", "Channel", "Count"])

    dfp = pd.DataFrame(rows).groupby(["Prefix", "Channel"]).size().reset_index(name="Count")
    return dfp

# ---- 표시용 안전 문자열화 ----
INT64_MIN = -(2**63)
INT64_MAX = (2**63) - 1
def _coerce_big_int(v):
    try:
        iv = int(v)
    except Exception:
        return v
    if iv < INT64_MIN or iv > INT64_MAX:
        return str(iv)
    return iv
def _stringify_for_grid(v):
    if isinstance(v, (dict, list, set, tuple)):
        try:
            return json.dumps(v, ensure_ascii=False, default=str)
        except Exception:
            return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        try:
            return bytes(v).decode("utf-8")
        except Exception:
            return repr(v)
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return _coerce_big_int(v)
    try:
        import numpy as np
        if isinstance(v, (np.integer,)):
            return _coerce_big_int(int(v))
    except Exception:
        pass
    return v
def make_display_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "key" in out.columns:
        out["key"] = out["key"].astype(str)
    if "type" in out.columns:
        out["type"] = out["type"].astype(str)
    if "value" in out.columns:
        out["value"] = out["value"].apply(_stringify_for_grid)
    return out

# ---- 데이터 로드 & 그래프 ----
with right_cell:
    # 캡션: All이면 요약 형태로 표시
    # if horizon == "All" and prefixes:
    #     st.caption(f"⏱ 기간: **All** – 사용 패턴: `{prefixes[0]}*` … `{prefixes[-1]}*` (총 {len(prefixes)}개)")
    # else:
    #     st.caption("🔍 사용 패턴: " + ", ".join(f"`{p}`" for p in patterns))

    with st.spinner("Redis에서 데이터를 가져오는 중..."):
        keys, df, type_counter, error_count = run_query(patterns)

    if not keys:
        st.warning("해당 조건에 맞는 키가 없습니다.")
        st.stop()

    # === 그래프 ===
    series_df = build_prefix_series(keys, per_day=per_day, prefixes=prefixes)

    # All일 때는 '기간에 해당하는 접두사가 없습니다' 문구 출력하지 않음
    if horizon != "All" and not prefixes:
        st.info("기간에 해당하는 접두사가 없습니다.", icon=":material/info:")
    else:
        # 선택된 접두사 기준으로 누락 구간 0으로 채움
        all_rows = []
        # prefixes가 None일 가능성 방지 (All에서는 리스트, 그 외에도 리스트)
        fill_prefixes = prefixes or []
        for ch in channels:
            for p in fill_prefixes:
                cnt = 0
                row = series_df[(series_df["Prefix"] == p) & (series_df["Channel"] == ch)]
                if not row.empty:
                    cnt = int(row["Count"].values[0])
                all_rows.append({"Prefix": p, "Channel": ch, "Count": cnt})

        chart_df = pd.DataFrame(all_rows) if all_rows else series_df.copy()

        # 라벨 포맷
        def format_label(p: str) -> str:
            try:
                if per_day and len(p) == 8:
                    dt = pd.to_datetime(p, format="%Y%m%d")
                    return dt.strftime("%Y %b %d")
                elif len(p) == 6:
                    dt = pd.to_datetime(p + "01", format="%Y%m%d")
                    return dt.strftime("%Y %b")
            except Exception:
                pass
            return p

        if not chart_df.empty:
            chart_df["DisplayPrefix"] = chart_df["Prefix"].apply(format_label)
            sort_order = [format_label(p) for p in fill_prefixes] if fill_prefixes else list(chart_df["DisplayPrefix"])

            chart = (
                alt.Chart(chart_df)
                .mark_line(point=True)
                .encode(
                    alt.X("DisplayPrefix:N", title="Month" if not per_day else "Date",
                          sort=sort_order, axis=alt.Axis(labelAngle=0)),
                    alt.Y("Count:Q", title="Count").scale(zero=False),
                    alt.Color("Channel:N", title="Channel"),
                    tooltip=[
                        alt.Tooltip("DisplayPrefix:N", title="Month" if not per_day else "Date"),
                        alt.Tooltip("Channel:N", title="Channel"),
                        alt.Tooltip("Count:Q", title="Count"),
                    ],
                )
                .properties(height=380)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("표시할 데이터가 없습니다.", icon=":material/info:")
            
            
# ---- 좌하단 메트릭 ----
bottom_left_cell = cols[0].container(border=True, height="stretch", vertical_alignment="center")
with bottom_left_cell:
    c = st.columns(2)
    c[0].metric("Matched keys", f"{len(keys):,}")
    c[1].metric("Errors", f"{error_count}")

# ---- Raw values ----            
"""
## Raw values 
"""
def _reorder_columns(df: pd.DataFrame, preferred: list[str], sort_remaining: bool = True) -> pd.DataFrame:
    """
    preferred 순서대로 앞에 배치하고, 나머지 컬럼은 뒤에 붙임.
    sort_remaining=True면 나머지를 알파벳 정렬해서 부착.
    """
    preferred = [c for c in preferred if c in df.columns]
    remaining = [c for c in df.columns if c not in preferred]
    if sort_remaining:
        remaining = sorted(remaining)
    cols = preferred + remaining
    return df[cols]

def _safe_stringify(obj):
    """dict/list 내부까지 순회하며 표시 안전한 값으로 변환(초대형 정수 → 문자열 등)."""
    if isinstance(obj, dict):
        return {k: _safe_stringify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_stringify(x) for x in obj]
    # 스칼라 타입은 기존 유틸 사용
    return _stringify_for_grid(obj)

records = []  # dict 또는 list[dict]를 표의 레코드로 사용
others  = []  # dict 정규화가 어려운 값은 단일 컬럼으로 표시

for v in df["value"].tolist():
    v_safe = _safe_stringify(v)

    if isinstance(v_safe, dict):
        records.append(v_safe)
    elif isinstance(v_safe, list) and v_safe and all(isinstance(x, dict) for x in v_safe):
        # [{...}, {...}] 형태면 각 원소를 행으로 펼침
        records.extend(v_safe)
    else:
        # 그 외는 단일 value 컬럼에 그대로 표시
        others.append({"value": v_safe if not isinstance(v_safe, (dict, list)) else json.dumps(v_safe, ensure_ascii=False)})

if records:
    # dict 기반 value들을 열로 정규화
    values_df = pd.json_normalize(records, sep=".")
    # 강제 형변환 방지: 전 컬럼 object 유지
    for col in values_df.columns:
        values_df[col] = values_df[col].astype("object")

    # === 열 순서 UI: Drag & Drop 전용 ===
        default_priority = [
            "channel_name", "original_id", "original_content", "original_created_at",
            "review_created_at", "reviewer_name", "review_content",
            "rating", "like", "views", "review_id", "inserted_at"
        ]
        options = list(values_df.columns)

        # 기본 초기 순서: default_priority 먼저, 나머지 뒤
        initial = [c for c in default_priority if c in options] + [c for c in options if c not in default_priority]

        # 이전에 정해둔 순서가 있으면 그걸 초기값으로 사용
        initial = st.session_state.get("col_order_drag", initial)

        preferred = initial[:]  # 기본값
        try:
            from streamlit_sortables import sort_items  # pip install streamlit-sortables

        except Exception:
            st.info("드래그 UI 컴포넌트를 사용할 수 없어 현재 컬럼 순서를 유지합니다.\n"
                    "패키지 설치: `pip install streamlit-sortables`", icon=":material/info:")

    # Drag & Drop 결과 순서로 정확히 재정렬 (뒤에 남는 컬럼 없음)
    values_df = values_df[[c for c in preferred if c in values_df.columns] +
                        [c for c in values_df.columns if c not in preferred]]

    st.dataframe(values_df, use_container_width=True)

    # 다운로드(열 순서 반영)
    json_values = json.dumps(values_df.to_dict(orient="records"), ensure_ascii=False, indent=2)
    horizon_tag = "all" if prefixes is None else horizon.replace(" ", "").lower()
    st.download_button(
        label="💾 JSON으로 저장하기 (values only - 열 순서 반영)",
        data=json_values,
        file_name=f"redis_{'-'.join(channels)}_{horizon_tag}_values.json",
        mime="application/json",
    )

elif others:
    values_df = pd.DataFrame(others)
    for col in values_df.columns:
        values_df[col] = values_df[col].astype("object")

    with st.expander("열 순서 설정", expanded=False):
        options = list(values_df.columns)
        default_selected = ["value"] if "value" in options else []

        preferred_cols = st.multiselect(
            "우선 표시할 열(위에서부터 적용, 선택 순서 유지)",
            options=options,
            default=default_selected,
            placeholder="예: value",
            accept_new_options=True,
            key="others_col_order"
        )
        sort_remaining = st.checkbox("나머지 열 알파벳 정렬", value=True, key="others_sort_remaining")

    preferred = [c for c in preferred_cols if c in values_df.columns]
    values_df = _reorder_columns(values_df, preferred, sort_remaining=sort_remaining)

    st.dataframe(values_df, use_container_width=True)

    json_values = json.dumps(values_df.to_dict(orient="records"), ensure_ascii=False, indent=2)
    horizon_tag = "all" if prefixes is None else horizon.replace(" ", "").lower()
    st.download_button(
        label="💾 JSON으로 저장하기 (values only - 열 순서 반영)",
        data=json_values,
        file_name=f"redis_{'-'.join(channels)}_{horizon_tag}_values.json",
        mime="application/json",
    )
else:
    st.info("표시할 value가 없습니다.", icon=":material/info:")
