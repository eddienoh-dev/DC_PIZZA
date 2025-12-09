import datetime as dt
import time
from collections import defaultdict

import pandas as pd
import requests
from bs4 import BeautifulSoup

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


def normalize_date(text: str, today: dt.date) -> dt.date | None:
    text = text.strip()
    # 시간까지 포함되면 첫 토큰(날짜)만 사용
    if " " in text:
        text = text.split(" ")[0]

    # 상대 표현 처리
    if text in ("오늘",):
        return today
    if text in ("어제",):
        return today - dt.timedelta(days=1)

    # 당일 게시글은 시간만 표시되는 경우가 있어 ":" 여부로 판별
    if ":" in text:
        return today

    # DCInside에서 쓰이는 여러 날짜 포맷 시도
    for fmt in ("%Y.%m.%d", "%y.%m.%d", "%m.%d", "%m/%d", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(text, fmt)

            # 연도가 없는 경우 올해로 간주
            if fmt in ("%m.%d", "%m/%d"):
                parsed = parsed.replace(year=today.year)

            return parsed.date()
        except ValueError:
            continue

    return None


def get_recent_7days_count(keyword, max_page=50):
    """키워드별 최근 7일 게시글 수를 일자별로 카운트."""
    daily_count = defaultdict(int)
    today = dt.date.today()
    cutoff = today - dt.timedelta(days=6)  # 오늘 포함 최근 7일

    for page in range(1, max_page + 1):
        print(f"[{keyword}] 페이지 {page} 수집 중...")

        params = {
            "id": "pizza",
            "s_type": "search_subject_memo",
            "s_keyword": keyword,
            "page": page,
        }

        res = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=10)
        if res.status_code != 200:
            print(f"[{keyword}] 요청 실패: status {res.status_code}")
            break
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

            if parsed < cutoff:
                continue  # 7일 이전 글 제외

            daily_count[parsed.isoformat()] += 1

        # 이번 페이지에서 가장 오래된 날짜가 컷오프보다 이전이면 더 이상 탐색하지 않음
        if parsed_dates and min(parsed_dates) < cutoff:
            break

        time.sleep(0.5)  # 차단 방지용 딜레이

    return daily_count


def fetch_recent_counts(brands=BRANDS, max_page=50) -> pd.DataFrame:
    """브랜드 리스트를 받아 최근 7일 데이터프레임 반환."""
    all_data = []

    for brand in brands:
        result = get_recent_7days_count(brand, max_page=max_page)
        for day, count in result.items():
            all_data.append({"brand": brand, "date": day, "count": count})

    df = pd.DataFrame(all_data)
    if df.empty:
        return df

    return df.sort_values(by=["date", "brand"])


def run_streamlit():
    if st is None:
        raise ImportError("streamlit이 설치되어 있지 않습니다. `pip install streamlit` 후 실행하세요.")

    @st.cache_data(show_spinner=True, ttl=600)
    def load_data(selected_brands, max_page: int):
        df = fetch_recent_counts(brands=selected_brands, max_page=max_page)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df

    st.title("DCInside 피자 브랜드 추이 (최근 7일)")
    st.write("DCInside 피자 갤러리에서 브랜드 키워드로 검색한 최근 7일치 게시글 수를 일자별로 집계합니다.")

    col1, col2 = st.columns([2, 1])
    with col1:
        brands = st.multiselect("브랜드 선택", options=BRANDS, default=BRANDS)
    with col2:
        max_page = st.slider("최대 페이지(검색 깊이)", min_value=5, max_value=80, value=50, step=5)

    if not brands:
        st.info("브랜드를 최소 1개 선택하세요.")
        return

    if st.button("데이터 불러오기", type="primary"):
        with st.spinner("수집 중..."):
            df = load_data(brands, max_page)

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
        st.line_chart(pivot)
    else:
        st.info("브랜드와 페이지를 선택한 뒤 '데이터 불러오기'를 클릭하세요.")


if __name__ == "__main__":
    run_streamlit()
