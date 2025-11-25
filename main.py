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

# --- Gemini API 関連のインポート ---
from google import genai
from google.genai import types
from google.api_core.exceptions import ResourceExhausted
# ------------------------------------

# ====== 設定 ======
# 【改修①】スプレッドシートIDをGithub Secretsの環境変数 "SPREADSHEET_KEY" から取得
SHARED_SPREADSHEET_ID = os.environ.get("SPREADSHEET_KEY")
if not SHARED_SPREADSHEET_ID:
    print("エラー: 環境変数 'SPREADSHEET_KEY' が設定されていません。処理を中断します。")
    sys.exit(1)

KEYWORD_FILE = "keywords.txt"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
MAX_SHEET_ROWS_FOR_REPLACE = 10000

# 【改修】最大取得ページ数を10に設定
MAX_PAGES = 10 

# 【改修③】ヘッダーにJ列（日産関連文）、K列（日産ネガ文）を追加
YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", "対象企業", "カテゴリ分類", "ポジネガ分類", "日産関連文", "日産ネガ文"]
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_target_company.txt"
]

NISSAN_PROMPT_FILES = [
    "prompt_nissan_mention.txt",
    "prompt_nissan_sentiment.txt"
]

# 【改修②】Gemini API Keyを環境変数 "GOOGLE_API_KEY" から読み込む
try:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("警告: 環境変数 'GOOGLE_API_KEY' が設定されていません。Gemini分析はスキップされます。")
        GEMINI_CLIENT = None
    else:
        GEMINI_CLIENT = genai.Client(api_key=api_key)
except Exception as e:
    print(f"警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
    GEMINI_CLIENT = None

GEMINI_PROMPT_TEMPLATE = None
NISSAN_PROMPT_TEMPLATE = None

# ====== ヘルパー関数群 ======

def gspread_util_col_to_letter(col_index: int) -> str:
    """ gspreadの古いバージョン対策: 列番号をアルファベットに変換 """
    if col_index < 1:
        raise ValueError("Column index must be 1 or greater")
    a1_notation = gspread.utils.rowcol_to_a1(1, col_index)
    return re.sub(r'\d+', '', a1_notation)

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
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31):
                    dt = dt.replace(year=dt.year - 1)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None

def build_gspread_client() -> gspread.Client:
    try:
        creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(credentials)
        else:
            try:
                return gspread.service_account(filename='credentials.json')
            except FileNotFoundError:
                raise RuntimeError("Google認証情報 (GCP_SERVICE_ACCOUNT_KEY)が環境変数、または 'credentials.json' ファイルに見つかりません。")

    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")

def load_keywords(filename: str) -> List[str]:
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        if not keywords:
            raise ValueError("キーワードファイルに有効なキーワードが含まれていません。")
        return keywords
    except FileNotFoundError:
        print(f"致命的エラー: キーワードファイル '{filename}' が見つかりません。")
        return []
    except Exception as e:
        print(f"キーワードファイルの読み込みエラー: {e}")
        return []

def load_gemini_prompt() -> str:
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE is not None:
        return GEMINI_PROMPT_TEMPLATE
        
    combined_instructions = []
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        role_instruction = ""
        role_file = PROMPT_FILES[0]
        file_path = os.path.join(script_dir, role_file)
        with open(file_path, 'r', encoding='utf-8') as f:
            role_instruction = f.read().strip()
        
        for filename in PROMPT_FILES[1:]:
            file_path = os.path.join(script_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            if content:
                combined_instructions.append(content)
                        
        base_prompt = role_instruction + "\n" + "\n".join(combined_instructions)
        base_prompt += "\n\n記事本文:\n{TEXT_TO_ANALYZE}"

        GEMINI_PROMPT_TEMPLATE = base_prompt
        return base_prompt
    except Exception as e:
        print(f"致命的エラー: プロンプトファイルの読み込み中にエラー: {e}")
        return ""

def load_nissan_prompt() -> str:
    global NISSAN_PROMPT_TEMPLATE
    if NISSAN_PROMPT_TEMPLATE is not None:
        return NISSAN_PROMPT_TEMPLATE
        
    combined_instructions = []
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        role_instruction = ""
        role_file = PROMPT_FILES[0]
        file_path = os.path.join(script_dir, role_file)
        with open(file_path, 'r', encoding='utf-8') as f:
            role_instruction = f.read().strip()
        
        for filename in NISSAN_PROMPT_FILES:
            file_path = os.path.join(script_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            if content:
                combined_instructions.append(content)
                        
        base_prompt = role_instruction + "\n" + "\n".join(combined_instructions)
        base_prompt += "\n\n記事本文:\n{TEXT_TO_ANALYZE}"

        NISSAN_PROMPT_TEMPLATE = base_prompt
        return base_prompt
    except Exception as e:
        print(f"エラー: 日産用プロンプトファイルの読み込み中にエラー: {e}")
        return ""

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            if res.status_code == 404:
                # ページが存在しない場合はNoneを返す
                return None
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.random()
                time.sleep(wait_time)
            else:
                return None
    return None

# ====== Gemini 分析関数 ======
def analyze_with_gemini(text_to_analyze: str) -> Tuple[str, str, str, bool]:
    if not GEMINI_CLIENT:
        return "N/A", "N/A", "N/A", False
    if not text_to_analyze.strip():
        return "N/A", "N/A", "N/A", False

    prompt_template = load_gemini_prompt()
    if not prompt_template:
        return "ERROR(Prompt Missing)", "ERROR", "ERROR", False

    MAX_RETRIES = 3
    MAX_CHARACTERS = 15000 # トークン制限対策
    
    for attempt in range(MAX_RETRIES):
        try:
            text_for_prompt = text_to_analyze[:MAX_CHARACTERS]
            prompt = prompt_template.replace("{TEXT_TO_ANALYZE}", text_for_prompt)
            
            response = GEMINI_CLIENT.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema={"type": "object", "properties": {
                        "company_info": {"type": "string", "description": "記事の主題企業名と（）内に共同開発企業名を記載した結果"},
                        "category": {"type": "string", "description": "企業、モデル、技術などの分類結果"},
                        "sentiment": {"type": "string", "description": "ポジティブ、ニュートラル、ネガティブのいずれか"}
                    }}
                ),
            )
            analysis = json.loads(response.text.strip())
            return analysis.get("company_info", "N/A"), analysis.get("category", "N/A"), analysis.get("sentiment", "N/A"), False

        except ResourceExhausted:
            print("    Gemini API クォータ制限エラー (429)。停止します。")
            sys.exit(1)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return "ERROR", "ERROR", "ERROR", False
    return "ERROR", "ERROR", "ERROR", False

def analyze_nissan_context(text_to_analyze: str) -> Tuple[str, str]:
    """ 日産以外の記事に対して、日産への言及とネガティブな文脈を抽出する """
    if not GEMINI_CLIENT:
        return "N/A", "N/A"
    
    prompt_template = load_nissan_prompt()
    if not prompt_template:
        return "ERROR(Prompt Missing)", "ERROR"

    MAX_RETRIES = 3
    MAX_CHARACTERS = 15000
    
    for attempt in range(MAX_RETRIES):
        try:
            text_for_prompt = text_to_analyze[:MAX_CHARACTERS]
            prompt = prompt_template.replace("{TEXT_TO_ANALYZE}", text_for_prompt)
            
            response = GEMINI_CLIENT.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema={"type": "object", "properties": {
                        "nissan_related": {"type": "string", "description": "記事内の日産に関連する言及の要約または抜粋"},
                        "nissan_negative": {"type": "string", "description": "日産に対するネガティブな文脈の要約または抜粋"}
                    }}
                ),
            )
            analysis = json.loads(response.text.strip())
            return analysis.get("nissan_related", "なし"), analysis.get("nissan_negative", "なし")

        except ResourceExhausted:
            print("    Gemini API クォータ制限エラー (429)。停止します。")
            sys.exit(1)
        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return "ERROR", "ERROR"
    return "ERROR", "ERROR"

# ====== データ取得関数 (Selenium) ======

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    try:
        driver_path = ChromeDriverManager().install()
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f" WebDriverの初期化に失敗しました: {e}")
        return []
        
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(search_url)
    
    try:
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']"))
        )
        time.sleep(3)
    except Exception as e:
        print(f"  ?? 検索結果ページロードでタイムアウト: {e}")
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    articles_data = []
    today_jst = jst_now()
    
    for article in articles:
        try:
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            
            link_tag = article.find("a", href=True)
            url = link_tag["href"] if link_tag and link_tag["href"].startswith("https://news.yahoo.co.jp/articles/") else ""
            
            date_str = ""
            time_tag = article.find("time")
            if time_tag:
                date_str = time_tag.text.strip()
            
            # ソース抽出
            source_text = ""
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            if source_container:
                time_and_comments = source_container.find("div", class_=re.compile("sc-110wjhy-8"))
                if time_and_comments:
                    source_candidates = [
                        span.text.strip() for span in time_and_comments.find_all("span")
                        if not span.find("svg") and not re.match(r'\d{1,2}/\d{1,2}.*\d{2}:\d{2}', span.text.strip())
                    ]
                    if source_candidates:
                        source_text = max(source_candidates, key=len)
                    if not source_text:
                        for content in time_and_comments.contents:
                            if content.name is None and content.strip() and not re.match(r'\d{1,2}/\d{1,2}.*\d{2}:\d{2}', content.strip()):
                                source_text = content.strip()
                                break
                    
            if title and url:
                formatted_date = date_str
                try:
                    dt_obj = parse_post_date(date_str, today_jst)
                    if dt_obj:
                        formatted_date = format_datetime(dt_obj)
                    else:
                        formatted_date = re.sub(r"\([月火水木金土日]\)$", "", date_str).strip()
                except:
                    pass

                articles_data.append({
                    "URL": url,
                    "タイトル": title,
                    "投稿日時": formatted_date if formatted_date else "取得不可",
                    "ソース": source_text if source_text else "取得不可"
                })
        except Exception:
            continue
            
    print(f"  Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

# ====== 詳細取得関数 (ページネーション対応 & ノイズ除去版) ======

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    """
    記事の本文（最大10ページ）とコメント数、詳細な日付を取得する。
    - ページネーション対応: ?page=1〜10 を巡回
    - ノイズ除去: 「私もそう思う」などのボタンテキストを除外
    """
    comment_count = -1
    extracted_date_str = None
    
    # 記事ID抽出
    article_id_match = re.search(r'/articles/([a-f0-9]+)', base_url)
    if not article_id_match:
        return "本文取得不可", -1, None
    
    # クエリパラメータを除去したベースURLを作成
    base_url_clean = base_url.split('?')[0]
    
    full_body_parts = []
    
    # --- ページネーションループ (page=1 to MAX_PAGES) ---
    for page_num in range(1, MAX_PAGES + 1):
        target_url = f"{base_url_clean}?page={page_num}"
        
        # リクエスト実行
        response = request_with_retry(target_url)
        if not response:
            # 404やエラーならループ終了
            break
            
        # 【重要】リダイレクト判定
        # Yahooニュースは存在しないページ番号(例: page=3)を指定すると、1ページ目(page=1 or paramなし)にリダイレクトすることがある
        # リダイレクトされたURLが、リクエストしたURLと明らかに異なる（ページ番号が消えている等）場合は終了
        current_resp_url = response.url
        if page_num > 1:
            # page=N をリクエストしたのに、URLに page=N が含まれていない、または page=1 になっている場合
            if f"page={page_num}" not in current_resp_url:
                break

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # --- 初回ページのみ: コメント数と日付の取得 ---
        if page_num == 1:
            # コメント数
            cmt_btn = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")}) or \
                      soup.find("a", attrs={"data-cl-params": re.compile(r"cmtmod")})
            if cmt_btn:
                txt = cmt_btn.get_text(strip=True).replace(",", "")
                m = re.search(r'(\d+)', txt)
                if m: comment_count = int(m.group(1))
            
            # 日付（本文冒頭付近の日時文字列を探す）
            # まずはarticle要素を探す
            article_temp = soup.find('article') or soup.find('div', class_=re.compile(r'article_body|article_detail'))
            if article_temp:
                temp_text = article_temp.get_text()[:500] # 先頭500文字程度
                m = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', temp_text)
                if m:
                    extracted_date_str = f"{m.group(1)} {m.group(3)}"

        # --- 本文抽出 (ノイズ除去ロジック強化) ---
        article_content = soup.find('article') or soup.find('div', class_='article_body') or soup.find('div', class_=re.compile(r'article_detail|article_body'))
        
        page_text_blocks = []
        if article_content:
            # ノイズ要素を事前に除去 (ボタン、リアクション、フッター、関連記事モジュール)
            for noise in article_content.find_all(['button', 'a', 'div'], class_=re.compile(r'reaction|rect|module|link|footer|comment')):
                noise.decompose()
            
            # 本文パラグラフの取得
            # sc-xxx highLightSearchTarget のようなクラスを優先
            paragraphs = article_content.find_all('p', class_=re.compile(r'sc-\w+-0\s+\w+.*highLightSearchTarget'))
            if not paragraphs:
                # 汎用pタグ取得（ただし、明らかに本文でないものは除外したい）
                paragraphs = article_content.find_all('p')
            
            for p in paragraphs:
                # テキストのみ取得
                text = p.get_text(strip=True)
                # ノイズフィルタ: 「私もそう思う」「学びがある」などが紛れ込んでいないかチェック
                if text and text not in ["そう思う", "そう思わない", "学びがある", "わかりやすい", "新しい視点", "私もそう思います"]:
                    page_text_blocks.append(text)
        
        if not page_text_blocks:
            # このページに有効なテキストがなければ終了
            if page_num > 1: break
        
        page_body = "\n".join(page_text_blocks)
        
        # 重複チェック（リダイレクトが検知しきれず、1ページ目と同じ内容が返ってきた場合）
        if page_num > 1 and len(full_body_parts) > 0:
            # 直前のページと同じ内容なら終了（1ページ目と比較する簡易ロジックでも可）
            prev_content = full_body_parts[0].split('ーーーー\n')[-1] # 1ページ目の本文部分
            if page_body == prev_content:
                break

        # 区切り線を入れて格納
        separator = f"\n{page_num}ページ目{'ー'*30}\n"
        full_body_parts.append(separator + page_body)
        
        time.sleep(1) # ページ遷移間の待機

    # 全ページの結合
    final_body_text = "".join(full_body_parts).strip()
    
    return final_body_text if final_body_text else "本文取得不可", comment_count, extracted_date_str

# ====== スプレッドシート操作関数 ======

def ensure_source_sheet_headers(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SOURCE_SHEET_NAME, rows=str(MAX_SHEET_ROWS_FOR_REPLACE), cols=str(len(YAHOO_SHEET_HEADERS)))
        
    current_headers = ws.row_values(1)
    if current_headers != YAHOO_SHEET_HEADERS:
        ws.update(range_name=f'A1:{gspread.utils.rowcol_to_a1(1, len(YAHOO_SHEET_HEADERS))}', values=[YAHOO_SHEET_HEADERS])
    return ws

def write_news_list_to_source(gc: gspread.Client, articles: list[dict]):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    worksheet = ensure_source_sheet_headers(sh)
            
    existing_data = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    existing_urls = set(str(row[0]) for row in existing_data[1:] if len(row) > 0 and str(row[0]).startswith("http"))
    
    new_data = [[a['URL'], a['タイトル'], a['投稿日時'], a['ソース']] for a in articles if a['URL'] not in existing_urls]
    
    if new_data:
        worksheet.append_rows(new_data, value_input_option='USER_ENTERED')
        print(f"  SOURCEシートに {len(new_data)} 件追記しました。")
    else:
        print("  SOURCEシートに追記すべき新しいデータはありません。")

def sort_yahoo_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        return

    last_row = len(worksheet.col_values(1))
    if last_row <= 1: return

    try:
        requests = []
        days_of_week = ["月", "火", "水", "木", "金", "土", "日"]
        for day in days_of_week:
            requests.append({
                "findReplace": {
                    "range": {"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE, "startColumnIndex": 2, "endColumnIndex": 3},
                    "find": rf"\({day}\)", "replacement": "", "searchByRegex": True,
                }
            })
        requests.append({
            "findReplace": {
                "range": {"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE, "startColumnIndex": 2, "endColumnIndex": 3},
                "find": r"\s{2,}", "replacement": " ", "searchByRegex": True,
            }
        })
        requests.append({
            "findReplace": {
                "range": {"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE, "startColumnIndex": 2, "endColumnIndex": 3},
                "find": r"^\s+|\s+$", "replacement": "", "searchByRegex": True,
            }
        })
        worksheet.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f" ?? 置換エラー: {e}")

    try:
        format_requests = [{
            "repeatCell": {
                "range": {"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": last_row, "startColumnIndex": 2, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy/mm/dd hh:mm:ss"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        }]
        worksheet.spreadsheet.batch_update({"requests": format_requests})
        time.sleep(2)
    except Exception as e:
        print(f" ?? 書式設定エラー: {e}") 

    try:
        last_col_index = len(YAHOO_SHEET_HEADERS)
        last_col_a1 = gspread_util_col_to_letter(last_col_index)
        sort_range = f'A2:{last_col_a1}{last_row}'
        worksheet.sort((3, 'desc'), range=sort_range)
        print(" ? SOURCEシートを投稿日時順に並び替えました。")
    except Exception as e:
        print(f" ?? ソートエラー: {e}")

# ====== 本文・コメント数の取得と即時更新 ======

def fetch_details_and_update_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        return
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1: return
        
    data_rows = all_values[1:]
    update_count = 0
    
    print("\n=====   ステップ② 記事本文とコメント数の取得・即時反映 (E, F列) =====")

    now_jst = jst_now()
    three_days_ago = (now_jst - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)

    for idx, data_row in enumerate(data_rows):
        if len(data_row) < len(YAHOO_SHEET_HEADERS):
            data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
            
        row_num = idx + 2
        
        url = str(data_row[0])
        title = str(data_row[1])
        post_date_raw = str(data_row[2])
        source = str(data_row[3])
        body = str(data_row[4])
        comment_count_str = str(data_row[5])
        
        if not url.strip() or not url.startswith('http'):
            continue

        is_content_fetched = (body.strip() and body != "本文取得不可")
        needs_body_fetch = not is_content_fetched
        
        post_date_dt = parse_post_date(post_date_raw, now_jst)
        is_within_three_days = (post_date_dt and post_date_dt >= three_days_ago)
        
        if is_content_fetched and not is_within_three_days:
            continue
            
        is_comment_only_update = is_content_fetched and is_within_three_days
        needs_full_fetch = needs_body_fetch
        
        if not (needs_full_fetch or is_comment_only_update):
            continue

        if needs_full_fetch:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): **本文(1-10p)/コメント数取得中...**")
        elif is_comment_only_update:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): **コメント数更新中...**")
            
        fetched_body, fetched_comment_count, extracted_date = fetch_article_body_and_comments(url)

        new_body = body
        new_comment_count = comment_count_str
        new_post_date = post_date_raw
        
        needs_update_to_sheet = False

        if needs_full_fetch:
            if fetched_body != "本文取得不可":
                if new_body != fetched_body:
                    new_body = fetched_body
                    needs_update_to_sheet = True
            elif body != "本文取得不可":
                 new_body = "本文取得不可"
                 needs_update_to_sheet = True
        elif is_comment_only_update and fetched_body == "本文取得不可":
             if body != "本文取得不可":
                 new_body = "本文取得不可"
                 needs_update_to_sheet = True
            
        if needs_full_fetch and ("取得不可" in post_date_raw or not post_date_raw.strip()) and extracted_date:
            dt_obj = parse_post_date(extracted_date, now_jst)
            if dt_obj:
                formatted_dt = format_datetime(dt_obj)
                if formatted_dt != post_date_raw:
                    new_post_date = formatted_dt
                    needs_update_to_sheet = True
            else:
                raw_date = re.sub(r"\([月火水木金土日]\)$", "", extracted_date).strip()
                if raw_date != post_date_raw:
                    new_post_date = raw_date
                    needs_update_to_sheet = True
            
        if fetched_comment_count != -1:
            if needs_full_fetch or is_comment_only_update:
                if str(fetched_comment_count) != comment_count_str:
                    new_comment_count = str(fetched_comment_count)
                    needs_update_to_sheet = True

        if needs_update_to_sheet:
            ws.update(
                range_name=f'C{row_num}:F{row_num}',
                values=[[new_post_date, source, new_body, new_comment_count]],
                value_input_option='USER_ENTERED'
            )
            update_count += 1
            time.sleep(1 + random.random() * 0.5)

    print(f" ? {update_count} 行の詳細情報を更新しました。")


# ====== 【改修③対応】Gemini分析の実行 (G〜K列) ======

def analyze_with_gemini_and_update_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        return
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1: return
        
    data_rows = all_values[1:]
    update_count = 0
    
    print("\n=====   ステップ④ Gemini分析の実行・即時反映 (G〜K列) =====")

    for idx, data_row in enumerate(data_rows):
        if len(data_row) < len(YAHOO_SHEET_HEADERS):
            data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
            
        row_num = idx + 2
        url = str(data_row[0])
        title = str(data_row[1])
        body = str(data_row[4])
        
        current_company = str(data_row[6]) # G列
        current_category = str(data_row[7])
        current_sentiment = str(data_row[8])
        current_nissan_rel = str(data_row[9]) # J列
        current_nissan_neg = str(data_row[10]) # K列

        needs_basic = not current_company.strip() or not current_category.strip() or not current_sentiment.strip()
        needs_nissan = not current_nissan_rel.strip() or not current_nissan_neg.strip()

        if not needs_basic and not needs_nissan:
            continue
            
        if not body.strip() or body == "本文取得不可":
            ws.update(range_name=f'G{row_num}:K{row_num}', values=[['N/A(No Body)', 'N/A', 'N/A', 'N/A', 'N/A']], value_input_option='USER_ENTERED')
            update_count += 1
            time.sleep(1)
            continue
            
        if not url.strip(): continue

        print(f"  - 行 {row_num} (記事: {title[:20]}...): Gemini分析を実行中...")

        final_company = current_company
        final_category = current_category
        final_sentiment = current_sentiment

        if needs_basic:
            final_company, final_category, final_sentiment, _ = analyze_with_gemini(body)
            time.sleep(1 + random.random() * 0.5)

        final_nissan_rel = current_nissan_rel
        final_nissan_neg = current_nissan_neg

        if needs_nissan:
            # 企業名判定 (日産 or NISSANが含まれていれば追加分析スキップ)
            if "日産" in final_company or "NISSAN" in final_company.upper():
                final_nissan_rel = "－ (対象が日産)"
                final_nissan_neg = "－"
            else:
                print(f"    -> 対象企業が日産以外 ({final_company}) のため、日産関連文脈を分析中...")
                final_nissan_rel, final_nissan_neg = analyze_nissan_context(body)
                time.sleep(1 + random.random() * 0.5)
        
        ws.update(
            range_name=f'G{row_num}:K{row_num}',
            values=[[final_company, final_category, final_sentiment, final_nissan_rel, final_nissan_neg]],
            value_input_option='USER_ENTERED'
        )
        update_count += 1

    print(f" ? Gemini分析を {update_count} 行について実行しました。")


def main():
    print("--- 統合スクリプト開始 ---")
    
    keywords = load_keywords(KEYWORD_FILE)
    if not keywords:
        sys.exit(0)

    try:
        gc = build_gspread_client()
    except RuntimeError as e:
        print(f"致命的エラー: {e}")
        sys.exit(1)
    
    # ステップ① ニュース取得
    for current_keyword in keywords:
        print(f"\n=====   ステップ① ニュースリスト取得: {current_keyword} =====")
        yahoo_news_articles = get_yahoo_news_with_selenium(current_keyword)
        write_news_list_to_source(gc, yahoo_news_articles)
        time.sleep(2)

    # ステップ② 本文・コメント数の取得 (ページネーション対応済)
    fetch_details_and_update_sheet(gc)

    # ステップ③ ソートと整形
    print("\n=====   ステップ③ 記事データのソートと整形 =====")
    sort_yahoo_sheet(gc)
    
    # ステップ④ Gemini分析 (基本 + 日産追加)
    analyze_with_gemini_and_update_sheet(gc)
    
    print("\n--- 統合スクリプト完了 ---")

if __name__ == '__main__':
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
    main()
