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
# 【改修①】スプレッドシートIDを環境変数 SPREADSHEET_KEY から取得
SHARED_SPREADSHEET_ID = os.environ.get("SPREADSHEET_KEY")
if not SHARED_SPREADSHEET_ID:
    print("エラー: 環境変数 'SPREADSHEET_KEY' が設定されていません。")
    # ローカルテスト用などでIDがない場合、これ以降でエラーになるため終了するか、適宜対応が必要
    # ここでは続行不可として終了します
    sys.exit(1)

KEYWORD_FILE = "keywords.txt"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
MAX_SHEET_ROWS_FOR_REPLACE = 10000
MAX_PAGES = 10 

# 【改修③】ヘッダーにJ列（日産関連文）、K列（日産ネガ文）を追加
YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", "対象企業", "カテゴリ分類", "ポジネガ分類", "日産関連文", "日産ネガ文"]
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

# 通常分析用プロンプトファイル
PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_target_company.txt"
]

# 【改修③】日産追加分析用プロンプトファイル
NISSAN_PROMPT_FILES = [
    "prompt_nissan_mention.txt",
    "prompt_nissan_sentiment.txt"
]

# 【改修②】Gemini API Keyを環境変数 GOOGLE_API_KEY から読み込む
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
NISSAN_PROMPT_TEMPLATE = None # 日産用テンプレートキャッシュ

# ====== ヘルパー関数群 ======

def gspread_util_col_to_letter(col_index: int) -> str:
    """ gspreadの古いバージョンで col_to_letter がない場合の代替関数 (1-indexed) """
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

# 通常分析用プロンプト読み込み
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
                        
        if not role_instruction or not combined_instructions:
            print("致命的エラー: プロンプトファイルの内容が不完全または空です。")
            return ""

        base_prompt = role_instruction + "\n" + "\n".join(combined_instructions)
        base_prompt += "\n\n記事本文:\n{TEXT_TO_ANALYZE}"

        GEMINI_PROMPT_TEMPLATE = base_prompt
        print(f" Geminiプロンプトテンプレートを {PROMPT_FILES} から読み込み、結合しました。")
        return base_prompt
    except Exception as e:
        print(f"致命的エラー: プロンプトファイルの読み込み中にエラーが発生しました: {e}")
        return ""

# 【改修③】日産分析用プロンプト読み込み
def load_nissan_prompt() -> str:
    global NISSAN_PROMPT_TEMPLATE
    if NISSAN_PROMPT_TEMPLATE is not None:
        return NISSAN_PROMPT_TEMPLATE
        
    combined_instructions = []
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 役割（Role）は共通のものを使用する
        role_instruction = ""
        role_file = PROMPT_FILES[0] # prompt_gemini_role.txt
        file_path = os.path.join(script_dir, role_file)
        with open(file_path, 'r', encoding='utf-8') as f:
            role_instruction = f.read().strip()
        
        # 日産用の指示ファイルを結合
        for filename in NISSAN_PROMPT_FILES:
            file_path = os.path.join(script_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            if content:
                combined_instructions.append(content)
                        
        base_prompt = role_instruction + "\n" + "\n".join(combined_instructions)
        base_prompt += "\n\n記事本文:\n{TEXT_TO_ANALYZE}"

        NISSAN_PROMPT_TEMPLATE = base_prompt
        print(f" 日産用プロンプトテンプレートを {NISSAN_PROMPT_FILES} から読み込み、結合しました。")
        return base_prompt
    except Exception as e:
        print(f"エラー: 日産用プロンプトファイルの読み込み中にエラーが発生しました: {e}")
        return ""

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            if res.status_code == 404:
                print(f"  ? ページなし (404 Client Error): {url}")
                return None
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ?? 接続エラー、リトライ中... ({attempt + 1}/{max_retries})。待機: {wait_time:.2f}秒")
                time.sleep(wait_time)
            else:
                print(f"  ? 最終リトライ失敗: {e}")
                return None
    return None

# ====== Gemini 分析関数 (通常) ======
def analyze_with_gemini(text_to_analyze: str) -> Tuple[str, str, str, bool]:
    if not GEMINI_CLIENT:
        return "N/A", "N/A", "N/A", False
    if not text_to_analyze.strip():
        return "N/A", "N/A", "N/A", False

    prompt_template = load_gemini_prompt()
    if not prompt_template:
        return "ERROR(Prompt Missing)", "ERROR", "ERROR", False

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
                        "company_info": {"type": "string", "description": "記事の主題企業名と（）内に共同開発企業名を記載した結果"},
                        "category": {"type": "string", "description": "企業、モデル、技術などの分類結果"},
                        "sentiment": {"type": "string", "description": "ポジティブ、ニュートラル、ネガティブのいずれか"}
                    }}
                ),
            )
            analysis = json.loads(response.text.strip())
            return analysis.get("company_info", "N/A"), analysis.get("category", "N/A"), analysis.get("sentiment", "N/A"), False

        except ResourceExhausted as e:
            print(f"    Gemini API クォータ制限エラー (429): {e}")
            sys.exit(1)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ?? Gemini API エラー (通常分析)。リトライします...: {e}")
                time.sleep(wait_time)
                continue
            else:
                print(f"Gemini分析エラー: {e}")
                return "ERROR", "ERROR", "ERROR", False
    return "ERROR", "ERROR", "ERROR", False

# ====== 【改修③】Gemini 分析関数 (日産追加分析) ======
def analyze_nissan_context(text_to_analyze: str) -> Tuple[str, str]:
    """
    日産以外の記事に対して、日産への言及とネガティブな文脈を抽出する
    戻り値: (nissan_related_text, nissan_negative_text)
    """
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

        except ResourceExhausted as e:
            print(f"    Gemini API クォータ制限エラー (429): {e}")
            sys.exit(1)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ?? Gemini API エラー (日産分析)。リトライします...: {e}")
                time.sleep(wait_time)
                continue
            else:
                print(f"Gemini日産分析エラー: {e}")
                return "ERROR", "ERROR"
    return "ERROR", "ERROR"

# ====== データ取得関数 (ソース抽出ロジック修正) ======

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
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
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
        print(f"  ?? ページロードまたは要素検索でタイムアウト。エラー: {e}")
        time.sleep(5)
    
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
            
            source_text = ""
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            
            if source_container:
                time_and_comments = source_container.find("div", class_=re.compile("sc-110wjhy-8"))
                if time_and_comments:
                    source_candidates = [
                        span.text.strip() for span in time_and_comments.find_all("span")
                        if not span.find("svg")
                        and not re.match(r'\d{1,2}/\d{1,2}\([月火水木金土日]\)\d{1,2}:\d{2}', span.text.strip())
                    ]
                    if source_candidates:
                        source_text = max(source_candidates, key=len)
                    if not source_text:
                        for content in time_and_comments.contents:
                            if content.name is None and content.strip() and not re.match(r'\d{1,2}/\d{1,2}\([月火水木金土日]\)\d{1,2}:\d{2}', content.strip()):
                                source_text = content.strip()
                                break
                    
            if title and url:
                formatted_date = ""
                if date_str:
                    try:
                        dt_obj = parse_post_date(date_str, today_jst)
                        if dt_obj:
                            formatted_date = format_datetime(dt_obj)
                        else:
                            formatted_date = re.sub(r"\([月火水木金土日]\)$", "", date_str).strip()
                    except:
                        formatted_date = date_str

                articles_data.append({
                    "URL": url,
                    "タイトル": title,
                    "投稿日時": formatted_date if formatted_date else "取得不可",
                    "ソース": source_text if source_text else "取得不可"
                })
        except Exception as e:
            continue
            
    print(f"  Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

# ====== 詳細取得関数 ======
def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    comment_count = -1
    extracted_date_str = None
    
    article_id_match = re.search(r'/articles/([a-f0-9]+)', base_url)
    if not article_id_match:
        print(f"  ? URLから記事IDが抽出できませんでした: {base_url}")
        return "本文取得不可", -1, None
        
    current_url = base_url.split('?')[0]
    response = request_with_retry(current_url)
    
    if not response:
        print(f"  ? 記事本文の取得に失敗したため、本文取得不可を返します。: {current_url}")
        return "本文取得不可", -1, None
        
    soup = BeautifulSoup(response.text, 'html.parser')

    article_content = soup.find('article') or soup.find('div', class_='article_body') or soup.find('div', class_=re.compile(r'article_detail|article_body'))

    current_body = []
    if article_content:
        paragraphs = article_content.find_all('p', class_=re.compile(r'sc-\w+-0\s+\w+.*highLightSearchTarget'))
        if not paragraphs:
            paragraphs = article_content.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text:
                current_body.append(text)
    
    body_text = "\n".join(current_body)
    
    comment_button = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")}) or \
                         soup.find("a", attrs={"data-cl-params": re.compile(r"cmtmod")})
    if comment_button:
        text = comment_button.get_text(strip=True).replace(",", "")
        match = re.search(r'(\d+)', text)
        if match:
            comment_count = int(match.group(1))

    if body_text:
        body_text_partial = "\n".join(body_text.split('\n')[:3])
        match = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', body_text_partial)
        if match:
            month_day = match.group(1)
            time_str = match.group(3)
            extracted_date_str = f"{month_day} {time_str}"
            
    return body_text if body_text else "本文取得不可", comment_count, extracted_date_str


# ====== スプレッドシート操作関数 ======

def set_row_height(ws: gspread.Worksheet, row_height_pixels: int):
    try:
        requests = []
        requests.append({
           "updateDimensionProperties": {
                 "range": {
                     "sheetId": ws.id,
                     "dimension": "ROWS",
                     "startIndex": 1,
                     "endIndex": ws.row_count
                 },
                 "properties": {
                     "pixelSize": row_height_pixels
                 },
                 "fields": "pixelSize"
            }
        })
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f" ?? 行高設定エラー: {e}")

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
        print("ソートスキップ: Yahooシートが見つかりません。")
        return

    last_row = len(worksheet.col_values(1))
    
    if last_row <= 1:
        return

    try:
        requests = []
        days_of_week = ["月", "火", "水", "木", "金", "土", "日"]
        for day in days_of_week:
            requests.append({
                "findReplace": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1,
                        "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE,
                        "startColumnIndex": 2,
                        "endColumnIndex": 3
                    },
                    "find": rf"\({day}\)",
                    "replacement": "",
                    "searchByRegex": True,
                }
            })
        requests.append({
            "findReplace": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "find": r"\s{2,}",
                "replacement": " ",
                "searchByRegex": True,
            }
        })
        requests.append({
            "findReplace": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "find": r"^\s+|\s+$",
                "replacement": "",
                "searchByRegex": True,
            }
        })
        worksheet.spreadsheet.batch_update({"requests": requests})
        print(" スプレッドシート上でC列の**曜日記載を個別に削除し、体裁を整えました**。")
    except Exception as e:
        print(f" ?? スプレッドシート上の置換エラー: {e}")

    try:
        format_requests = []
        format_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": last_row,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "DATE_TIME",
                            "pattern": "yyyy/mm/dd hh:mm:ss"
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })
        worksheet.spreadsheet.batch_update({"requests": format_requests})
        print(f" ? C列(2行目〜{last_row}行) の表示形式を 'yyyy/mm/dd hh:mm:ss' に設定しました。")
        time.sleep(2)
    except Exception as e:
        print(f" ?? C列の表示形式設定エラー: {e}") 

    try:
        last_col_index = len(YAHOO_SHEET_HEADERS)
        last_col_a1 = gspread_util_col_to_letter(last_col_index)
        sort_range = f'A2:{last_col_a1}{last_row}'
        worksheet.sort((3, 'desc'), range=sort_range)
        print(" ? SOURCEシートを投稿日時の**新しい順**にスプレッドシート上で並び替えました。")
    except Exception as e:
        print(f" ?? スプレッドシート上のソートエラー: {e}")

# ====== 本文・コメント数の取得と即時更新 (E, F列) ======

def fetch_details_and_update_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("詳細取得スキップ: Yahooシートが見つかりません。")
        return
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print(" Yahooシートにデータがないため、詳細取得をスキップします。")
        return
        
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
        needs_detail_fetch = is_comment_only_update or needs_full_fetch

        if not needs_detail_fetch:
            continue

        if needs_full_fetch:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): **本文/コメント数/日時補完を取得中... (完全取得)**")
        elif is_comment_only_update:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): **コメント数を更新中... (軽量更新)**")
            
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

    print(f" ? 本文/コメント数取得と日時補完を {update_count} 行について実行し、即時反映しました。")


# ====== 【改修③対応】Gemini分析の実行と強制中断 (G, H, I, J, K列) ======

def analyze_with_gemini_and_update_sheet(gc: gspread.Client):
    """ G列〜I列の分析に加え、条件に応じてJ列・K列の分析を行い、即時更新する """
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("Gemini分析スキップ: Yahooシートが見つかりません。")
        return
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print(" Yahooシートにデータがないため、Gemini分析をスキップします。")
        return
        
    data_rows = all_values[1:]
    update_count = 0
    
    print("\n=====   ステップ④ Gemini分析の実行・即時反映 (G〜K列) =====")

    for idx, data_row in enumerate(data_rows):
        if len(data_row) < len(YAHOO_SHEET_HEADERS):
            data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
            
        row_num = idx + 2
        
        url = str(data_row[0])
        title = str(data_row[1])
        body = str(data_row[4])       # E列
        
        # 既存分析値
        current_company_info = str(data_row[6]) # G列
        current_category = str(data_row[7])     # H列
        current_sentiment = str(data_row[8])    # I列
        current_nissan_rel = str(data_row[9])   # J列
        current_nissan_neg = str(data_row[10])  # K列

        # G〜I列の分析が必要かどうか
        needs_basic_analysis = not current_company_info.strip() or not current_category.strip() or not current_sentiment.strip()
        
        # J〜K列の分析が必要かどうか (初期値)
        # ※基本分析が終わってから、対象企業の結果を見て判断するため、ここでは「すでに埋まっているか」だけ見る
        needs_nissan_analysis = not current_nissan_rel.strip() or not current_nissan_neg.strip()

        if not needs_basic_analysis and not needs_nissan_analysis:
            continue
            
        if not body.strip() or body == "本文取得不可":
            print(f"  - 行 {row_num}: 本文がないため分析をスキップし、N/Aを設定。")
            ws.update(
                range_name=f'G{row_num}:K{row_num}',
                values=[['N/A(No Body)', 'N/A', 'N/A', 'N/A', 'N/A']],
                value_input_option='USER_ENTERED'
            )
            update_count += 1
            time.sleep(1)
            continue
            
        if not url.strip():
            continue

        print(f"  - 行 {row_num} (記事: {title[:20]}...): Gemini分析を実行中...")

        # --- 1. 基本分析 (G, H, I列) ---
        final_company_info = current_company_info
        final_category = current_category
        final_sentiment = current_sentiment

        if needs_basic_analysis:
            final_company_info, final_category, final_sentiment, _ = analyze_with_gemini(body)
            # API負荷軽減のための待機
            time.sleep(1 + random.random() * 0.5)

        # --- 2. 日産追加分析 (J, K列) ---
        final_nissan_rel = current_nissan_rel
        final_nissan_neg = current_nissan_neg

        # 基本分析の結果、対象企業が「日産」または「NISSAN」を含まない場合に追加分析
        # かつ、まだJ,Kが埋まっていない場合
        if needs_nissan_analysis:
            # 企業名判定: 大文字小文字を区別せず、文字列が含まれるかチェック
            if "日産" in final_company_info or "NISSAN" in final_company_info.upper():
                # 対象がすでに日産の場合は追加分析不要
                final_nissan_rel = "－ (対象が日産)"
                final_nissan_neg = "－"
            else:
                # 対象が日産以外 -> 追加分析実行
                print(f"    -> 対象企業が日産以外 ({final_company_info}) のため、日産関連文脈を分析中...")
                final_nissan_rel, final_nissan_neg = analyze_nissan_context(body)
                time.sleep(1 + random.random() * 0.5)
        
        # --- スプレッドシート更新 (G〜K列まとめて) ---
        ws.update(
            range_name=f'G{row_num}:K{row_num}',
            values=[[final_company_info, final_category, final_sentiment, final_nissan_rel, final_nissan_neg]],
            value_input_option='USER_ENTERED'
        )
        update_count += 1

    print(f" ? Gemini分析を {update_count} 行について実行し、即時反映しました。")


# ====== メイン処理 ======

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
    
    # ① ステップ① ニュース取得
    for current_keyword in keywords:
        print(f"\n=====   ステップ① ニュースリスト取得: {current_keyword} =====")
        yahoo_news_articles = get_yahoo_news_with_selenium(current_keyword)
        write_news_list_to_source(gc, yahoo_news_articles)
        time.sleep(2)

    # ② ステップ② 本文・コメント数の取得
    fetch_details_and_update_sheet(gc)

    # ③ ステップ③ ソートと整形
    print("\n=====   ステップ③ 記事データのソートと整形 =====")
    sort_yahoo_sheet(gc)
    
    # ④ ステップ④ Gemini分析 (基本 + 日産追加)
    analyze_with_gemini_and_update_sheet(gc)
    
    print("\n--- 統合スクリプト完了 ---")

if __name__ == '__main__':
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
    main()
