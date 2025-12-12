import datetime as dt
import time
from collections import defaultdict

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import altair as alt

try:
    import streamlit as st
except ImportError:
    st = None

# 모니터링할 브랜드 목록
BRANDS = ["피자헛", "도미노", "파파존스", "피자스쿨"]

BASE_URL = "https://gall.dcinside.com/board/lists"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://gall.dcinside.com/board/lists?id=pizza",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}
REQUEST_TIMEOUT = (5, 20)
RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.mount("https://", HTTPAdapter(max_retries=RETRY_STRATEGY))
SESSION.mount("http://", HTTPAdapter(max_retries=RETRY_STRATEGY))


def normalize_date(text: str, today: dt.date) -> dt.date | None:
    text = text.strip()
    if " " in text:
        text = text.split(" ")[0]

    if text in ("오늘",):
        return today
    if text in ("어제",):
        return today - dt.timedelta(days=1)

    if ":" in text:
        return today

    for fmt in ("%Y.%m.%d", "%y.%m.%d", "%m.%d", "%m/%d", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(text, fmt)
            if fmt in ("%m.%d", "%m/%d"):
                parsed = parsed.replace(year=today.year)
            return parsed.date()
        except ValueError:
            continue
    return None


def get_counts_in_range(
    keyword,
    start_date: dt.date,
    end_date: dt.date,
    max_page=50,
    timeout=REQUEST_TIMEOUT,
    progress_cb=None,
):
    """키워드별 지정 기간의 게시글 수를 일자별로 카운트."""
    daily_count = defaultdict(int)
    today = dt.date.today()

    for page in range(1, max_page + 1):
        msg = f"[{keyword}] 페이지 {page} 수집 중..."
        print(msg)
        if progress_cb:
            progress_cb(msg)

        params = {
            "id": "pizza",
            "s_type": "search_subject_memo",
            "s_keyword": keyword,
            "page": page,
        }

        try:
            res = SESSION.get(BASE_URL, params=params, timeout=timeout)
        except requests.exceptions.ReadTimeout:
            err_msg = f"[{keyword}] page {page} request timed out (read {timeout[1]}s), skipped"
            print(err_msg)
            if progress_cb:
                progress_cb(err_msg)
            continue
        except requests.exceptions.RequestException as exc:
            err_msg = f"[{keyword}] page {page} request failed: {exc}"
            print(err_msg)
            if progress_cb:
                progress_cb(err_msg)
            time.sleep(1.0)
            continue
        if res.status_code != 200:
            err_msg = f"[{keyword}] request failed: status {res.status_code}"
            print(err_msg)
            if progress_cb:
                progress_cb(err_msg)
            time.sleep(1.0)
            continue
        soup = BeautifulSoup(res.text, "html.parser")
        rows = soup.select("tr.ub-content")
        if not rows:
            print(f"[{keyword}] 행을 찾지 못했습니다 (page {page}). HTML 구조 변경 가능성.")
            break

        parsed_dates = []

        for row in rows:
            date_tag = row.select_one(".gall_date")
            if not date_tag:
                continue

            parsed = normalize_date(date_tag.text, today)
            if not parsed:
                continue

            parsed_dates.append(parsed)

            if parsed < start_date:
                continue  # 조회 시작 이전
            if parsed > end_date:
                continue  # 조회 종료 이후

            daily_count[parsed.isoformat()] += 1

        # 페이지 내 가장 오래된 날짜가 시작일보다 이전이면 종료
        if parsed_dates and min(parsed_dates) < start_date:
            break

        time.sleep(0.5)  # 차단 방지용 딜레이

    return daily_count


def fetch_counts(
    brands,
    start_date: dt.date,
    end_date: dt.date,
    max_page=50,
    timeout=REQUEST_TIMEOUT,
    progress_cb=None,
) -> pd.DataFrame:
    all_data = []
    for brand in brands:
        if progress_cb:
            progress_cb(f"[{brand}] 수집 시작")
        result = get_counts_in_range(
            brand,
            start_date=start_date,
            end_date=end_date,
            max_page=max_page,
            timeout=timeout,
            progress_cb=progress_cb,
        )
        for day, count in result.items():
            all_data.append({"brand": brand, "date": day, "count": count})

    df = pd.DataFrame(all_data)
    if df.empty:
        return df
    return df.sort_values(by=["date", "brand"])


def run_streamlit():
    if st is None:
        raise ImportError("streamlit이 설치되어 있지 않습니다. `pip install streamlit` 후 실행하세요.")

    today = dt.date.today()
    default_start = today - dt.timedelta(days=6)

    st.title("DCInside 피자 브랜드 추이")
    st.write("브랜드 키워드로 검색한 게시글 수를 일자별로 집계합니다.")

    col1, col2 = st.columns([2, 1])
    with col1:
        brands = st.multiselect("브랜드 선택", options=BRANDS, default=BRANDS)
    with col2:
        max_page = st.slider("최대 페이지(검색 깊이)", min_value=5, max_value=80, value=50, step=5)
    read_timeout = st.slider("Read timeout (sec)", min_value=5, max_value=60, value=REQUEST_TIMEOUT[1], step=5)
    timeout = (REQUEST_TIMEOUT[0], read_timeout)

    date_range = st.date_input(
        "조회 기간 (시작일, 종료일)",
        value=(default_start, today),
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        st.warning("시작일과 종료일을 모두 선택하세요.")
        return

    if start_date > end_date:
        st.warning("시작일이 종료일보다 늦습니다. 기간을 다시 선택하세요.")
        return

    if not brands:
        st.info("브랜드를 최소 1개 선택하세요.")
        return

    if st.button("데이터 불러오기", type="primary"):
        progress_area = st.empty()

        def progress_cb(msg: str):
            progress_area.write(msg)

        with st.spinner("수집 중..."):
            df = fetch_counts(
                brands=brands,
                start_date=start_date,
                end_date=end_date,
                max_page=max_page,
                timeout=timeout,
                progress_cb=progress_cb,
            )

        progress_area.write("수집 완료")
        progress_area.empty()

        if df.empty:
            st.warning("수집된 데이터가 없습니다. 검색 키워드나 페이지 수를 늘려보세요.")
            return

        pivot = (
            df.pivot_table(index="date", columns="brand", values="count", fill_value=0)
            .sort_index()
        )

        st.subheader("일자별 브랜드별 게시글 수")
        st.dataframe(pivot, use_container_width=True)

        st.subheader("추이 그래프")
        chart_data = pivot.reset_index().melt("date", var_name="brand", value_name="count")
        chart_data["date_str"] = pd.to_datetime(chart_data["date"]).dt.strftime("%m/%d")

        chart = (
            alt.Chart(chart_data)
            .mark_line(point=True)
            .encode(
                x=alt.X("date_str:N", axis=alt.Axis(labelAngle=0, title="날짜")),
                y=alt.Y("count:Q", axis=alt.Axis(title="게시글 수")),
                color=alt.Color(
                    "brand:N",
                    title="브랜드",
                    scale=alt.Scale(
                        domain=["피자헛", "도미노", "피자스쿨", "파파존스"],
                        range=["#d62728", "#1f77b4", "#ffbf00", "#2ca02c"],  # 빨강, 파랑, 노랑, 초록
                    ),
                ),
            )
            .properties(width="container")
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("브랜드, 기간, 페이지를 선택한 뒤 '데이터 불러오기'를 클릭하세요.")


if __name__ == "__main__":
    run_streamlit()
