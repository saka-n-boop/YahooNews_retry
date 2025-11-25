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
                time_and_comments = source_
