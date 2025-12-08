ご要望（重複除外、Commentsシートの日時ソート）を完璧に盛り込んだ構成で作成しました。

今回はファイルが2つになります。

comment_scraper.py （新規作成：コメント取得・書き込み・ソートのロジック）

main.py （既存改修：最後に上記を呼び出す）

以下、それぞれの全文です。

1. comment_scraper.py (新規作成)

このファイルを main.py と同じ場所に作成してください。
コメント取得、10件ごとの結合、重複チェック、そして最後に日時順（新しい順）へのソートを行います。

code
Python
download
content_copy
expand_less
import time
import re
import requests
from bs4 import BeautifulSoup
import gspread

# 設定
COMMENTS_SHEET_NAME = "Comments"
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}

def ensure_comments_sheet(sh: gspread.Spreadsheet):
    """ Commentsシートがなければ作成し、ヘッダーを設定する """
    try:
        ws = sh.worksheet(COMMENTS_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        # 列数は多めに確保
        ws = sh.add_worksheet(title=COMMENTS_SHEET_NAME, rows="1000", cols="300")
        
        # ヘッダー作成
        headers = ["URL", "タイトル", "投稿日時", "ソース"]
        # コメント列：1-10 ... 2491-2500
        for i in range(0, 250):
            start = i * 10 + 1
            end = (i + 1) * 10
            headers.append(f"コメント：{start} - {end}")
            
        ws.update(range_name='A1', values=[headers])
        
    return ws

def fetch_comments_from_url(article_url: str) -> list[str]:
    """ 記事URLから全コメントを取得し、10件ごとに結合したリストを返す """
    
    # URL調整 (/commentsエンドポイントを作成)
    base_url = article_url.split('?')[0]
    if not base_url.endswith('/comments'):
        if '/comments' in base_url:
             base_url = base_url.split('/comments')[0] + '/comments'
        else:
             base_url = f"{base_url}/comments"

    all_comments_data = [] 
    page = 1
    
    print(f"    - コメント取得開始: {base_url}")

    while True:
        target_url = f"{base_url}?page={page}"
        try:
            res = requests.get(target_url, headers=REQ_HEADERS, timeout=10)
            if res.status_code == 404:
                break 
            res.raise_for_status()
        except Exception as e:
            print(f"      ! 通信エラー(p{page}): {e}")
            break

        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 記事タグ構造から取得（クラス名非依存）
        articles = soup.find_all('article')
        
        if not articles:
            break 

        comments_in_page = 0
        for art in articles:
            # ユーザー名 (h2直下のテキストなど)
            user_tag = art.find('h2')
            user_name = user_tag.get_text(strip=True) if user_tag else "匿名"
            
            # 本文 (最も長いpタグを採用)
            p_tags = art.find_all('p')
            comment_body = ""
            if p_tags:
                comment_body = max([p.get_text(strip=True) for p in p_tags], key=len)
            
            if comment_body:
                full_text = f"【投稿者: {user_name}】\n{comment_body}"
                all_comments_data.append(full_text)
                comments_in_page += 1
        
        if comments_in_page == 0:
            break 

        page += 1
        time.sleep(1) 

    # 10件ごとに結合
    merged_columns = []
    chunk_size = 10
    for i in range(0, len(all_comments_data), chunk_size):
        chunk = all_comments_data[i : i + chunk_size]
        merged_text = "\n\n".join(chunk)
        merged_columns.append(merged_text)
        
    print(f"    - 取得完了: 全{len(all_comments_data)}件")
    return merged_columns

def set_row_height(ws, pixels):
    try:
        requests = [{
           "updateDimensionProperties": {
                 "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
                 "properties": {"pixelSize": pixels}, "fields": "pixelSize"
            }
        }]
        ws.spreadsheet.batch_update({"requests": requests})
    except: pass

def run_comment_collection(gc: gspread.Client, source_sheet_id: str, source_sheet_name: str):
    print("\n=====   ステップ⑤ 条件付きコメント収集・保存 =====")
    
    sh = gc.open_by_key(source_sheet_id)
    try:
        source_ws = sh.worksheet(source_sheet_name)
    except:
        print("  ! Sourceシートが見つかりません。")
        return

    dest_ws = ensure_comments_sheet(sh)
    
    # 既存データの読み込み（重複チェック用）
    dest_rows = dest_ws.get_all_values()
    existing_urls = set()
    if len(dest_rows) > 1:
        existing_urls = set(row[0] for row in dest_rows[1:] if row)

    # ソースデータの読み込み
    source_rows = source_ws.get_all_values()
    if len(source_rows) < 2: return
    data_rows = source_rows[1:]
    
    process_count = 0

    for i, row in enumerate(data_rows):
        if len(row) < 11: continue
        
        url = row[0]
        title = row[1]
        post_date = row[2]
        source = row[3]
        comment_count_str = row[5]
        target_company = row[6]
        sentiment = row[8]
        
        # 重複チェック（既にCommentsシートにあるURLはスキップ）
        if url in existing_urls:
            continue

        # --- 条件判定 ---
        is_target = False
        
        # 条件1: コメント数 > 100
        try:
            cnt = int(re.sub(r'\D', '', comment_count_str))
            if cnt > 100: is_target = True
        except: pass
            
        # 条件2: 対象企業が日産(開始) かつ ネガティブ
        if not is_target:
            if target_company.startswith("日産") or "日産" in target_company:
                 if "ネガティブ" in sentiment:
                     is_target = True
        
        if is_target:
            print(f"  - 対象記事発見(行{i+2}): {title[:20]}...")
            comment_columns = fetch_comments_from_url(url)
            
            if comment_columns:
                row_data = [url, title, post_date, source] + comment_columns
                dest_ws.append_rows([row_data], value_input_option='USER_ENTERED')
                process_count += 1
                time.sleep(2)

    # --- 最後にソート処理 ---
    if process_count > 0:
        print("  - Commentsシートを投稿日時順（新しい順）に並び替えます...")
        try:
            last_row = len(dest_ws.col_values(1))
            if last_row > 1:
                # C列(3列目)の日付で降順ソート
                dest_ws.sort((3, 'des'), range=f'A2:Z{last_row}') # Z列は適当な十分な範囲
        except Exception as e:
            print(f"  ! ソートエラー: {e}")
            
        set_row_height(dest_ws, 21)

    print(f" ? コメント収集完了: 新たに {process_count} 件の記事からコメントを保存しました。")
2. main.py (改修版全文)

前回からの変更点は、冒頭で import comment_scraper を追加し、main() 関数の最後でそれを呼び出す処理を追加した点のみです。

code
Python
download
content_copy
expand_less
import os
import json
import time
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set, Dict, Any
import sys
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- Gemini API 関連 ---
from google import genai
from google.genai import types
from google.api_core.exceptions import ResourceExhausted

# --- コメント収集用モジュールのインポート ---
import comment_scraper 
# ------------------------------------

# ====== 設定 ======
SHARED_SPREADSHEET_ID = os.environ.get("SPREADSHEET_KEY")
if not SHARED_SPREADSHEET_ID:
    print("エラー: 環境変数 'SPREADSHEET_KEY' が設定されていません。")
    sys.exit(1)

KEYWORD_FILE = "keywords.txt"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
MAX_SHEET_ROWS_FOR_REPLACE = 10000
MAX_PAGES = 10 

YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", "対象企業", "カテゴリ分類", "ポジネガ分類", "日産関連文", "日産ネガ文"]
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

ALL_PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_target_company.txt",
    "prompt_category.txt",
    "prompt_posinega.txt",
    "prompt_nissan_mention.txt",
    "prompt_nissan_sentiment.txt"
]

GEMINI_REQUEST_COUNT = 0
USING_SECOND_KEY = False

try:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("警告: GOOGLE_API_KEY 未設定。Gemini分析はスキップ。")
        GEMINI_CLIENT = None
    else:
        GEMINI_CLIENT = genai.Client(api_key=api_key)
except Exception as e:
    print(f"警告: Gemini初期化失敗: {e}")
    GEMINI_CLIENT = None

GEMINI_PROMPT_TEMPLATE = None

# ====== ヘルパー関数群 ======

def gspread_util_col_to_letter(col_index: int) -> str:
    if col_index < 1: raise ValueError("Column index must be >= 1")
    return re.sub(r'\d+', '', gspread.utils.rowcol_to_a1(1, col_index))

def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def format_datetime(dt_obj) -> str:
    return dt_obj.strftime("%Y/%m/%d %H:%M:%S")

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        s = re.sub(r"\([月火水木金土日]\)$", "", s).strip()
        s = s.replace('配信', '').strip()
        for fmt in ("%Y/%m/%d %H:%M:%S", "%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M": dt = dt.replace(year=today_jst.year)
                if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31): dt = dt.replace(year=dt.year - 1)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError: pass
        return None

def build_gspread_client() -> gspread.Client:
    try:
        creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        if creds_str:
            info = json.loads(creds_str)
            return gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(info, scope))
        else:
            return gspread.service_account(filename='credentials.json')
    except Exception as e:
        raise RuntimeError(f"Google認証失敗: {e}")

def load_keywords(filename: str) -> List[str]:
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except Exception: return []

def load_merged_prompt() -> str:
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE: return GEMINI_PROMPT_TEMPLATE
    combined = []
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for fname in ALL_PROMPT_FILES:
            with open(os.path.join(script_dir, fname), 'r', encoding='utf-8') as f:
                combined.append(f.read().strip())
        
        base = combined[0] + "\n" + "\n".join(combined[1:])
        base += "\n\n【重要】\n該当する情報（特に日産への言及やネガティブ要素）がない場合は、説明文や翻訳を一切書かず、必ず単語で『なし』とだけ出力してください。"
        base += "\n\n記事本文:\n{TEXT_TO_ANALYZE}"
        GEMINI_PROMPT_TEMPLATE = base
        print(" プロンプト統合ロード完了。")
        return base
    except Exception as e:
        print(f"プロンプト読込エラー: {e}")
        return ""

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            if res.status_code == 404: return None
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1: time.sleep(2 + random.random())
            else: return None
    return None

def set_row_height(ws: gspread.Worksheet, row_height_pixels: int):
    try:
        requests = [{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
            "properties": {"pixelSize": row_height_pixels}, "fields": "pixelSize"}}]
        ws.spreadsheet.batch_update({"requests": requests})
    except: pass

def update_sheet_with_retry(ws, range_name, values, max_retries=3):
    for attempt in range(max_retries):
        try:
            ws.update(range_name=range_name, values=values, value_input_option='USER_ENTERED')
            return
        except gspread.exceptions.APIError as e:
            if any(c in str(e) for c in ['500', '502', '503']):
                time.sleep(30 * (attempt + 1))
            else: raise e
        except Exception:
            time.sleep(10 * (attempt + 1))
    print(f"  !! 更新失敗: {range_name}")

# ====== Gemini 分析関数 ======
def analyze_article_full(text_to_analyze: str) -> Dict[str, str]:
    global GEMINI_CLIENT, GEMINI_REQUEST_COUNT, USING_SECOND_KEY
    default = {"company_info": "N/A", "category": "N/A", "sentiment": "N/A", "nissan_related": "なし", "nissan_negative": "なし"}
    
    if not GEMINI_CLIENT or not text_to_analyze.strip(): return default

    if GEMINI_REQUEST_COUNT >= 240 and not USING_SECOND_KEY:
        print("  ! API回数240回到達。Key2へ切替。")
        try:
            k2 = os.environ.get("GOOGLE_API_KEY_2")
            if k2: 
                GEMINI_CLIENT = genai.Client(api_key=k2)
                USING_SECOND_KEY = True
            else: USING_SECOND_KEY = True
        except: USING_SECOND_KEY = True

    tmpl = load_merged_prompt()
    if not tmpl: return default

    for attempt in range(3):
        try:
            GEMINI_REQUEST_COUNT += 1
            prompt = tmpl.replace("{TEXT_TO_ANALYZE}", text_to_analyze[:15000])
            response = GEMINI_CLIENT.models.generate_content(
                model='gemini-2.5-flash', contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema={"type": "object", "properties": {
                        "company_info": {"type": "string"}, "category": {"type": "string"},
                        "sentiment": {"type": "string"}, "nissan_related": {"type": "string"},
                        "nissan_negative": {"type": "string"}
                    }}
                ),
            )
            res = json.loads(response.text.strip())
            return {
                "company_info": res.get("company_info", "N/A"),
                "category": res.get("category", "N/A"),
                "sentiment": res.get("sentiment", "N/A"),
                "nissan_related": res.get("nissan_related", "なし"),
                "nissan_negative": res.get("nissan_negative", "なし")
            }
        except ResourceExhausted:
            print("    Gemini API クォータ制限(429)。停止。")
            sys.exit(1)
        except Exception:
            if attempt < 2: time.sleep(2)
            else: return default
    return default

# ====== 記事取得関連 ======
def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    print(f"  Yahoo!ニュース検索: {keyword}")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    except: return []
    
    driver.get(f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local")
    try: WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']")))
    except: pass
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    data = []
    today = jst_now()
    for art in soup.find_all("li", class_=re.compile("sc-1u4589e-0")):
        try:
            title = art.find("div", class_=re.compile("sc-3ls169-0")).text.strip()
            link = art.find("a", href=True)["href"]
            if not link.startswith("https://news.yahoo.co.jp/articles/"): continue
            
            date_str = art.find("time").text.strip() if art.find("time") else ""
            src_div = art.find("div", class_=re.compile("sc-n3vj8g-0"))
            source = ""
            if src_div:
                sub = src_div.find("div", class_=re.compile("sc-110wjhy-8"))
                if sub:
                    cands = [s.text.strip() for s in sub.find_all("span") if not s.find("svg") and not re.match(r'\d{1,2}/\d{1,2}.*\d{2}:\d{2}', s.text.strip())]
                    if cands: source = max(cands, key=len)
            
            fmt_date = date_str
            try:
                dt = parse_post_date(date_str, today)
                if dt: fmt_date = format_datetime(dt)
                else: fmt_date = re.sub(r"\([月火水木金土日]\)$", "", date_str).strip()
            except: pass
            
            data.append({"URL": link, "タイトル": title, "投稿日時": fmt_date, "ソース": source})
        except: continue
    
    print(f"  取得件数: {len(data)}")
    return data

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    aid = re.search(r'/articles/([a-f0-9]+)', base_url)
    if not aid: return "本文取得不可", -1, None
    
    clean_url = base_url.split('?')[0]
    full_body = []
    cmt_cnt = -1
    ext_date = None
    
    for page in range(1, MAX_PAGES + 1):
        res = request_with_retry(f"{clean_url}?page={page}")
        if not res: break
        if page > 1 and f"page={page}" not in res.url: break
        
        soup = BeautifulSoup(res.text, 'html.parser')
        
        if page == 1:
            btn = soup.find(["button", "a"], attrs={"data-cl-params": re.compile(r"cmtmod")})
            if btn:
                m = re.search(r'(\d+)', btn.get_text(strip=True).replace(",", ""))
                if m: cmt_cnt = int(m.group(1))
            
            art_div = soup.find('article') or soup.find('div', class_=re.compile(r'article_body|article_detail'))
            if art_div:
                m = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', art_div.get_text()[:500])
                if m: ext_date = f"{m.group(1)} {m.group(3)}"

        content = soup.find('article') or soup.find('div', class_=re.compile(r'article_detail|article_body'))
        p_texts = []
        if content:
            for n in content.find_all(['button', 'a', 'div'], class_=re.compile(r'reaction|rect|module|link|footer|comment')): n.decompose()
            ps = content.find_all('p', class_=re.compile(r'sc-\w+-0\s+\w+.*highLightSearchTarget')) or content.find_all('p')
            for p in ps:
                txt = p.get_text(strip=True)
                if txt and txt not in ["そう思う", "そう思わない", "学びがある", "わかりやすい", "新しい視点", "私もそう思います"]:
                    p_texts.append(txt)
        
        if not p_texts: 
            if page > 1: break
        
        page_txt = "\n".join(p_texts)
        if page > 1 and len(full_body) > 0 and page_txt == full_body[0].split('ーーーー\n')[-1]: break
        
        full_body.append(f"\n{page}ページ目{'ー'*30}\n{page_txt}")
        time.sleep(1)

    return "".join(full_body).strip() or "本文取得不可", cmt_cnt, ext_date

# ====== メイン処理フロー ======

def ensure_source_sheet(gc):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try: ws = sh.worksheet(SOURCE_SHEET_NAME)
    except: ws = sh.add_worksheet(SOURCE_SHEET_NAME, MAX_SHEET_ROWS_FOR_REPLACE, len(YAHOO_SHEET_HEADERS))
    if ws.row_values(1) != YAHOO_SHEET_HEADERS:
        ws.update(range_name=f'A1:{gspread_util_col_to_letter(len(YAHOO_SHEET_HEADERS))}1', values=[YAHOO_SHEET_HEADERS])
    return ws

def main():
    print("--- 統合スクリプト開始 ---")
    keys = load_keywords(KEYWORD_FILE)
    if not keys: sys.exit(0)
    
    try: gc = build_gspread_client()
    except Exception as e: print(f"致命的エラー: {e}"); sys.exit(1)
    
    # ① ニュース取得
    for k in keys:
        print(f"\n===== ① 取得: {k} =====")
        data = get_yahoo_news_with_selenium(k)
        ws = ensure_source_sheet(gc)
        exist = set(str(r[0]) for r in ws.get_all_values()[1:] if len(r)>0 and str(r[0]).startswith("http"))
        new = [[d['URL'], d['タイトル'], d['投稿日時'], d['ソース']] for d in data if d['URL'] not in exist]
        if new: ws.append_rows(new, value_input_option='USER_ENTERED')
        time.sleep(2)

    # ② 詳細取得
    print("\n===== ② 詳細取得 =====")
    fetch_details_and_update_sheet(gc)

    # ③ ソート
    print("\n===== ③ ソート・整形 =====")
    sort_yahoo_sheet(gc)
    
    # ④ Gemini分析
    print("\n===== ④ Gemini分析 =====")
    analyze_with_gemini_and_update_sheet(gc)
    
    # ⑤ コメント収集 (New!)
    # analyze_with_gemini_and_update_sheet(gc) # 念のため既存行を再度確認する場合などはここ
    # 新機能呼び出し
    print("\n===== ⑤ コメント取得開始 =====")
    comment_scraper.run_comment_collection(gc, SHARED_SPREADSHEET_ID, SOURCE_SHEET_NAME)
    
    print("\n--- 統合スクリプト完了 ---")

if __name__ == '__main__':
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    main()
