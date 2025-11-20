import os
import re
import time
import json
import gspread
import requests
import traceback
import google.generativeai as genai
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import GoogleAPIError
from gspread.exceptions import APIError as GSpreadAPIError

# --- グローバル変数 ---
# Googleスプレッドシートのスコープと認証情報
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# 環境変数からスプレッドシートキーを取得
SPREADSHEET_KEY = os.environ.get("SPREADSHEET_KEY")
if not SPREADSHEET_KEY:
    # ログ出力（致命的エラー）
    print("❌ 環境変数 'SPREADSHEET_KEY' が設定されていません。", flush=True)
    # スクリプトの実行を停止
    exit(1) # exit(1) はエラーによる終了を示す

# Geminiモデルのグローバルインスタンス
gemini_model = None

# 検索キーワード
SEARCH_KEYWORDS = [
    "トヨタ", "日産", "ホンダ", "三菱自動車",
    "マツダ", "スバル", "ダイハツ", "スズキ"
]

# プロンプトのファイルパス
PROMPT_FILES = {
    "role": "prompt_gemini_role.txt",
    "sentiment": "prompt_posinega.txt",
    "category": "prompt_category.txt",
    "company_info": "prompt_target_company.txt",
    "nissan_mention": "prompt_nissan_mention.txt",
    "nissan_sentiment": "prompt_nissan_sentiment.txt",
}

# 読み込んだプロンプトを格納する辞書
PROMPTS = {}


def setup_gspread():
    """
    Google スプレッドシート API への認証を行う。
    環境変数 GCP_SERVICE_ACCOUNT_KEY から認証情報を読み込む。
    """
    try:
        # 環境変数からサービスアカウントキーのJSON文字列を取得
        creds_json_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        if not creds_json_str:
            print("❌ 環境変数 'GCP_SERVICE_ACCOUNT_KEY' が設定されていません。")
            return None

        # JSON文字列を辞書に変換
        creds_dict = json.loads(creds_json_str)

        # 辞書から認証情報オブジェクトを作成
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        
        # gspread クライアントを認証
        gc = gspread.authorize(credentials)
        
        # スプレッドシートが開けるかテスト
        gc.open_by_key(SPREADSHEET_KEY)
        
        print("✅ Googleスプレッドシートへの認証に成功しました。")
        return gc

    except json.JSONDecodeError:
        print("❌ 'GCP_SERVICE_ACCOUNT_KEY' のJSON形式が正しくありません。")
        return None
    except Exception as e:
        print(f"❌ Googleスプレッドシートへの認証に失敗しました: {e}")
        return None


def get_worksheet(gc, sheet_name):
    """
    gspread クライアントとシート名を受け取り、ワークシートオブジェクトを返す。
    """
    if not gc:
        print(f"  ❌ ワークシート '{sheet_name}' を取得できません (gspreadクライアント未初期化)。")
        return None
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
        worksheet = spreadsheet.worksheet(sheet_name)
        return worksheet
    except GSpreadAPIError as e:
        print(f"  ❌ ワークシート '{sheet_name}' が見つからないか、アクセス権限がありません: {e}")
        return None
    except Exception as e:
        print(f"  ❌ ワークシート '{sheet_name}' の取得中に予期せぬエラー: {e}")
        return None


def load_existing_urls(ws):
    """
    SOURCE ワークシートから B 列（URL）のデータを読み込み、
    重複チェック用のセットとして返す。
    """
    try:
        # B列の全ての値を取得
        urls = ws.col_values(2) # B列は 2
        # 1行目（ヘッダー）を除く
        return set(urls[1:])
    except Exception as e:
        print(f"  ❌ 既存URLの読み込みに失敗しました: {e}")
        # 空のセットを返して処理を続行
        return set()


# (修正済) Yahoo!ニュースのHTML構造変更（一覧ページ）に対応
def get_yahoo_news_search_results(keyword):
    """
    指定されたキーワードで Yahoo!ニュースを検索し、
    記事のタイトル、URL、発行元、投稿時間のリストを返す。
    """
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(search_url, headers=headers)
        response.raise_for_status() # HTTPエラーをチェック
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # --- コンテナを探す ---
        # (新) <ol class="newsFeed_list"> を探す
        search_results_container = soup.find("ol", class_="newsFeed_list")
        # (新) <div class="newsFeed"> (小文字) を探す
        if not search_results_container:
            search_results_container = soup.find("div", class_="newsFeed")
        # (旧) <div class="NewsFeed"> (大文字) を探す
        if not search_results_container:
             search_results_container = soup.find("div", class_="NewsFeed")
        # (旧) <div class...="Search__ResultList"> を探す
        if not search_results_container:
            search_results_container = soup.find("div", class_=re.compile(r"Search__ResultList"))

        if not search_results_container:
            print(f"  - 検索結果のコンテナが見つかりません (ol.newsFeed_list, div.newsFeed, div.NewsFeed, Search__ResultList のいずれか)。")
            return []

        # --- 記事要素 (li) を探す ---
        articles = search_results_container.find_all("li")
        if not articles:
            articles = search_results_container.find_all("div", class_="newsFeed_item")

        if not articles:
            print("  - 記事要素 (li or div.newsFeed_item) が見つかりません。")
            return []

        results = []
        for article in articles:
            try:
                # --- 記事の「本文」領域のクラスをアンカーにする ---
                body_tag = article.find("div", class_="newsFeed_item_body")
                
                # body がない (広告liなど) 場合はスキップ
                if not body_tag:
                    continue

                # body から親の <a> タグを探して URL を取得
                title_tag = body_tag.find_parent("a")
                
                if not title_tag or "href" not in title_tag.attrs:
                    continue 

                url = title_tag["href"]
                
                # 記事URL以外は除外
                if not url.startswith("https://news.yahoo.co.jp/articles/"):
                    continue

                # --- タイトル、発行元、時間を取得 ---
                title = "（タイトル取得失敗）"
                source = "発行元不明"
                post_time_str = "時間不明"

                # time タグを探す
                time_tag = body_tag.find("time")
                if time_tag:
                    post_time_str = time_tag.text.strip()
                    
                    # time タグの親から span (発行元) を探す
                    meta_container = time_tag.find_parent("div")
                    if meta_container:
                        source_tag = meta_container.find("span")
                        if source_tag:
                            source = source_tag.text.strip()

                # タイトルを探す (動的クラス名 `sc-` に依存しない方法)
                # 'newsFeed_item_body' の中にある 'a' タグの 'div' でクラス名が 'sc-' で始まるものを探す
                title_text_tag = body_tag.find("div", class_=re.compile(r"^sc-3ls169-0")) # 暫定的な目印
                
                if not title_text_tag:
                    # 'sc-' で始まるクラスを持つ div を全て探し、その中のテキストを結合する (堅牢性を高める)
                    title_divs = body_tag.select("div[class*='sc-']")
                    if title_divs:
                        # 最初の 'sc-' クラスの div をタイトルとする
                        title = title_divs[0].get_text(strip=True)

                if title_text_tag and title == "（タイトル取得失敗）":
                        title = title_text_tag.get_text(strip=True)

                # <em> タグ内のテキストも取得（キーワードがハイライトされている場合）
                if title == "（タイトル取得失敗）" and title_tag.find("div", class_=re.compile(r"newsFeed_item_title")):
                     title = title_tag.find("div", class_=re.compile(r"newsFeed_item_title")).get_text(strip=True)
                
                if title == "（タイトル取得失敗）":
                    # 最終手段
                    title = title_tag.get_text(strip=True).split("\n")[0]


                results.append({
                    "title": title,
                    "url": url,
                    "source": source,
                    "post_time_str": post_time_str,
                    "keyword": keyword
                })

            except Exception as e:
                print(f"  - 記事パースエラー: {e}")
                continue
                
        print(f"  Yahoo!ニュース件数: {len(results)} 件取得")
        return results

    except requests.exceptions.RequestException as e:
        print(f"  ❌ Yahoo!ニュース検索リクエスト失敗: {e}")
        return []
    except Exception as e:
        print(f"  ❌ Yahoo!ニュース検索処理エラー: {e}")
        traceback.print_exc()
        return []


def parse_relative_time(time_str):
    """
    Yahoo!ニュースの相対時間（例: '1時間前', '11/11(月) 10:00'）を
    datetime オブジェクトに変換する。
    """
    now = datetime.now()
    
    # 1. '11/11(月) 10:00' 形式 (今年)
    match = re.search(r"(\d{1,2})/(\d{1,2})\(.\) (\d{1,2}):(\d{1,2})", time_str)
    if match:
        month, day, hour, minute = map(int, match.groups())
        try:
            return now.replace(month=month, day=day, hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
             return now.replace(year=now.year - 1, month=month, day=day, hour=hour, minute=minute, second=0, microsecond=0)

    # 2. '○分前' 形式
    match = re.search(r"(\d+)分前", time_str)
    if match:
        minutes = int(match.group(1))
        return now - timedelta(minutes=minutes)

    # 3. '○時間前' 形式
    match = re.search(r"(\d+)時間前", time_str)
    if match:
        hours = int(match.group(1))
        return now - timedelta(hours=hours)

    # 4. '昨日' 形式
    if "昨日" in time_str:
        match = re.search(r"(\d{1,2}):(\d{1,2})", time_str)
        day_delta = 1
        if match:
            hour, minute = map(int, match.groups())
            return (now - timedelta(days=day_delta)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        else:
            return now - timedelta(days=day_delta)

    # 5. '○日前' 形式
    match = re.search(r"(\d+)日前", time_str)
    if match:
        days = int(match.group(1))
        return now - timedelta(days=days)

    # 不明な形式
    return None


# --- (修正箇所) ---
# 記事本文ページのHTML構造変更に対応
def get_article_details(article_url):
    """
    記事URLから本文（最大10ページ）、コメント数、正確な投稿日時を取得する。
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    article_body_parts = []
    comment_count = "0" # デフォルト
    full_post_time = None # デフォルト

    try:
        # --- 1ページ目の取得 (コメント数と日時もここから取る) ---
        response = requests.get(article_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # コメント数 (動的クラス名対応)
        comment_count_tag = soup.find("a", class_=re.compile(r"CommentCount__CommentCountButton"), href=re.compile(r"/comments/"))
        if not comment_count_tag:
            # (フォールバック) sc-1n9vtw0-1 (コメントボタン)
            comment_count_tag = soup.find("button", class_=re.compile(r"sc-1n9vtw0-1"))
        
        if comment_count_tag:
            match = re.search(r"(\d+)", comment_count_tag.text)
            if match:
                comment_count = match.group(1)

        # 正確な投稿日時
        time_tag = soup.find("time")
        if time_tag and time_tag.has_attr("datetime"):
            try:
                full_post_time = datetime.fromisoformat(time_tag["datetime"].replace("Z", "+00:00"))
            except ValueError:
                print(f"  - 日時パース失敗: {time_tag['datetime']}")
                full_post_time = None

        # --- (修正) 記事本文 (1ページ目) ---
        # 旧: class_=re.compile(r"ArticleBody")
        # 新: class="article_body"
        body_container = soup.find("div", class_="article_body")
        
        if body_container:
            # 本文テキスト
            body_text = body_container.get_text(separator="\n", strip=True)
            article_body_parts.append(body_text)
        else:
            print(f"  - 記事本文(P1)が見つかりません (URL: {article_url})")
            article_body_parts.append("（本文取得失敗）")


        # --- 2ページ目以降の取得 (最大10ページ) ---
        for page_num in range(2, 11): # 2〜10ページ
            next_page_url = f"{article_url}?page={page_num}"
            try:
                response_page = requests.get(next_page_url, headers=headers)
                
                if response_page.status_code != 200:
                    print(f"  - 記事本文 ページ {page_num} は存在しませんでした。本文取得を完了します。")
                    break 
                
                soup_page = BeautifulSoup(response_page.text, "html.parser")
                # --- (修正) 2ページ目以降の本文 ---
                body_container_page = soup_page.find("div", class_="article_body")
                
                if body_container_page:
                    body_text_page = body_container_page.get_text(separator="\n", strip=True)
                    if body_text_page == article_body_parts[0]:
                         print(f"  - 記事本文 ページ {page_num} は1ページ目と同じ内容のため終了します。")
                         break
                    
                    print(f"  - 記事本文 ページ {page_num} を取得しました。")
                    article_body_parts.append(body_text_page)
                else:
                    print(f"  - 記事本文 ページ {page_num} が見つかりませんでした。")
                    break
                
                time.sleep(1) 

            except requests.exceptions.RequestException as re_e:
                if "404" in str(re_e):
                    print(f"  ❌ ページなし (404 Client Error): {next_page_url}")
                    print(f"  - 記事本文 ページ {page_num} は存在しませんでした。本文取得を完了します。")
                else:
                    print(f"  ❌ ページ {page_num} 取得エラー: {re_e}")
                break
            except Exception as e_page:
                print(f"  ❌ ページ {page_num} 処理エラー: {e_page}")
                break

    except requests.exceptions.RequestException as re_e:
        print(f"  ❌ 記事詳細ページ取得エラー (URL: {article_url}): {re_e}")
        return ["（本文取得失敗）"] * 10, "0", None
    except Exception as e:
        print(f"  ❌ 記事詳細処理エラー (URL: {article_url}): {e}")
        traceback.print_exc()
        return ["（本文取得失敗）"] * 10, "0", None

    if len(article_body_parts) < 10:
        article_body_parts.extend(["-"] * (10 - len(article_body_parts)))
    
    return article_body_parts[:10], comment_count, full_post_time


def load_prompts():
    """
    プロンプトファイルを読み込む。
    """
    global PROMPTS
    print("  プロンプトファイルを読み込んでいます...")
    try:
        for key, file_path in PROMPT_FILES.items():
            if not os.path.exists(file_path):
                print(f"  ❌ 警告: プロンプトファイル '{file_path}' が見つかりません。")
                continue
                
            with open(file_path, "r", encoding="utf-8") as f:
                PROMPTS[key] = f.read()
        
        if not PROMPTS:
             print("  ❌ エラー: 読み込めたプロンプトが1つもありません。")
             return False
             
        print("  ✅ プロンプトの読み込みが完了しました。")
        return True

    except Exception as e:
        print(f"  ❌ プロンプトファイルの読み込み中にエラー: {e}")
        return False


def initialize_gemini():
    """
    Gemini API を初期化する。
    """
    global gemini_model
    try:
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            print("  ❌ 警告: 環境変数 'GOOGLE_API_KEY' が設定されていません。")
            return

        if hasattr(genai, "configure"):
             genai.configure(api_key=api_key)
        else:
             print("  ⚠️ 警告: genai.configure が見つかりません。APIキーの手動設定を試みます。")
             pass 
        
        model = genai.GenerativeModel('gemini-pro')
        
        if not hasattr(genai, "configure"):
            model = genai.GenerativeModel('gemini-pro', api_key=api_key)

        gemini_model = model
        print("✅ Geminiクライアントの初期化に成功しました。 (model: gemini-pro)")

    except Exception as e:
        print(f"  ❌ 警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
        traceback.print_exc()
        gemini_model = None


def analyze_article_with_gemini(article_body):
    """
    記事本文を受け取り、Gemini API で分析する。
    """
    if not gemini_model:
        return {
            "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
            "nissan_mention": "N/A", "nissan_sentiment": "N/A"
        }

    max_length = 10000
    if len(article_body) > max_length:
        article_body = article_body[:max_length]

    full_prompt = f"""
{PROMPTS.get("role", "あなたは業界アナリストです。")}

【記事本文】
{article_body}
【記事本文ここまで】

---
【タスク】
記事本文を分析し、以下のタスクを実行してください。
結果は必ず指定されたJSONフォーマットで、キー「sentiment」「category」「company_info」「nissan_mention」「nissan_sentiment」を持つ単一のJSONオブジェクトとして出力してください。

1. **sentimentの判定**:
{PROMPTS.get("sentiment", "（sentimentルール）")}

2. **categoryの判定**:
{PROMPTS.get("category", "（categoryルール）")}

3. **company_infoの判定**:
{PROMPTS.get("company_info", "（company_infoルール）")}

4. **nissan_mentionの判定**:
(注: company_infoが「日産」*以外*の場合のみ、本文中の「日産」への言及を確認せよ)
{PROMPTS.get("nissan_mention", "（nissan_mentionルール）")}

5. **nissan_sentimentの判定**:
(注: nissan_mentionが「-」*以外*の場合のみ、その言及が日産にとってポジティブ/ネガティブ/ニュートラルか判定せよ)
{PROMPTS.get("nissan_sentiment", "（nissan_sentimentルール）")}

---
【出力フォーマット (JSON)】
{{
  "sentiment": "（1の判定結果）",
  "category": "（2の判定結果）",
  "company_info": "（3の判定結果）",
  "nissan_mention": "（4の判定結果）",
  "nissan_sentiment": "（5の判定結果）"
}}
"""

    try:
        response = gemini_model.generate_content(full_prompt)
        
        json_match = re.search(r"\{.*\}", response.text, re.DOTALL)
        
        if not json_match:
            print("  ❌ Gemini応答からJSONを抽出できませんでした。")
            print(f"  応答: {response.text}")
            return {
                "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
                "nissan_mention": "N/A", "nissan_sentiment": "N/A"
            }

        json_str = json_match.group(0)
        result = json.loads(json_str)
        
        required_keys = ["sentiment", "category", "company_info", "nissan_mention", "nissan_sentiment"]
        if not all(key in result for key in required_keys):
             print(f"  ❌ Gemini応答JSONに必要なキーが不足しています。 {result.keys()}")
             for key in required_keys:
                 if key not in result:
                     result[key] = "N/A (キー欠損)"

        return result

    except json.JSONDecodeError as e:
        print(f"  ❌ Gemini応答のJSONパースに失敗しました: {e}")
        print(f"  応答テキスト (JSON抽出後): {json_str}")
        return {
            "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
            "nissan_mention": "N/A", "nissan_sentiment": "N/A"
        }
    except GoogleAPIError as e:
        print(f"  ❌ Gemini API エラー: {e}")
        return {
            "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
            "nissan_mention": "N/A", "nissan_sentiment": "N/A"
        }
    except Exception as e:
        print(f"  ❌ Gemini分析中に予期せぬエラー: {e}")
        traceback.print_exc()
        return {
            "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
            "nissan_mention": "N/A", "nissan_sentiment": "N/A"
        }


# --- (修正箇所) ---
# コメント欄のHTML構造変更（動的クラス名）に対応
def get_yahoo_news_comments(article_id, article_url):
    """
    記事IDと記事URLを受け取り、コメントページの1〜3ページ目までをスクレイピングする。
    (動的な `sc-` クラス名に対応)
    """
    print(f"    - コメント本文 (S列～AC列) を取得中...")
    comments_data = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        base_comments_url = f"{article_url}/comments"
        
        for page_num in range(1, 4): # 1ページから3ページまで
            if page_num == 1:
                comments_url = base_comments_url
            else:
                comments_url = f"{base_comments_url}?page={page_num}"

            response = requests.get(comments_url, headers=headers)
            
            if response.status_code != 200:
                print(f"    ❌ コメント ページ {page_num} ( {comments_url} ) が存在しないか取得失敗。ステータス: {response.status_code}")
                break 

            soup = BeautifulSoup(response.text, "html.parser")

            # --- (修正) 動的クラス名対応 ---
            # 1. コメント欄のメインコンテナを探す
            comment_main = soup.find("article", id="comment-main")
            if not comment_main:
                 print(f"    - コメント ページ {page_num} に 'comment-main' コンテナが見つかりません。")
                 break

            # 2. コンテナ内の全 <article> タグ (これが各コメント) を探す
            #    (専門家コメント `sc-z8tf0-1`、一般コメント `sc-169yn8p-3` に対応)
            comments = comment_main.find_all("article", class_=re.compile(r"sc-"))
            
            if not comments:
                # print(f"    - コメント ページ {page_num} にコメントが見つかりませんでした。")
                break 

            for comment in comments:
                user_name = "ユーザー名不明"
                comment_text = "コメント本文なし"
                
                # 3. ユーザー名 (h2 タグ) を探す
                user_name_tag = comment.find("h2")
                if user_name_tag:
                    user_name = user_name_tag.get_text(strip=True)

                # 4. コメント本文 (p タグ) を探す
                #    (専門家 `sc-z8tf0-11`、一般 `sc-169yn8p-10` に対応する p タグ)
                comment_text_tag = comment.find("p", class_=re.compile(r"sc-.*-\d{1,2}$"))
                
                if comment_text_tag:
                    comment_text = comment_text_tag.get_text(strip=True)

                comments_data.append(f"【{user_name}】{comment_text}")

                if len(comments_data) >= 10: # 10件取得したら終了
                    break
            
            if len(comments_data) >= 10:
                break
            
            time.sleep(1) 

        if not comments_data:
            print(f"    - コメントが1件も見つかりませんでした（またはコメント欄閉鎖）。")
            return ["取得不可"] * 10

        if len(comments_data) < 10:
            comments_data.extend(["-"] * (10 - len(comments_data)))

        print(f"    ✅ コメント {len(comments_data)} 件を取得しました。")
        return comments_data[:10]

    except Exception as e:
        print(f"    ❌ コメント取得エラー: {e}")
        traceback.print_exc()
        return ["取得不可"] * 10
# --- (修正ここまで) ---


def update_source_sheet(ws, new_articles, existing_urls):
    """
    SOURCE ワークシートを更新する。
    1. 新しい記事をフィルタリング
    2. 新しい記事をシートに追加 (A-E列)
    3. analysis_flag が "TRUE" かつ 本文が空の記事 (F-AC列) を更新
    """
    
    # --- 1. 新しい記事をフィルタリング ---
    articles_to_add = []
    for article in new_articles:
        if article["url"] not in existing_urls:
            
            post_time = parse_relative_time(article["post_time_str"])
            if post_time:
                post_time_formatted = post_time.strftime("%Y/%m/%d %H:%M:%S")
            else:
                post_time_formatted = article["post_time_str"] 

            row_data = [
                article["keyword"],
                article["url"],
                post_time_formatted,
                article["source"],
                article["title"],
                "TRUE" # F列: analysis_flag
            ]
            articles_to_add.append(row_data)
            existing_urls.add(article["url"])

    # --- 2. 新しい記事をシートに追加 ---
    if articles_to_add:
        try:
            ws.append_rows(articles_to_add, value_input_option="USER_ENTERED")
            print(f"  ✅ {len(articles_to_add)} 件の新しい記事を SOURCEシート に追加しました。")
        except Exception as e:
            print(f"  ❌ 新規記事のスプレッドシートへの書き込みに失敗しました: {e}")
    else:
        print("  SOURCEシートに追記すべき新しいデータはありません。")


    # --- 3. 本文・コメント等が未取得の記事を更新 ---
    try:
        print("  ... 本文・コメント未取得のデータをスプレッドシートから読み込み中 ...")
        all_data = ws.get_all_values()
        if len(all_data) <= 1:
            print("  - データがありません。")
            return 

        headers = all_data[0]
        data_rows = all_data[1:]
        
        # 列インデックスの特定 (0始まり)
        try:
            url_col = headers.index("URL") # B列
            title_col = headers.index("title") # E列
            flag_col = headers.index("analysis_flag") # F列
            body_p1_col = headers.index("body_p1") # G列
            comment_1_col = headers.index("comment_1") # S列
        except ValueError as e:
            print(f"  ❌ 必要な列が見つかりません: {e}。本文取得をスキップします。")
            return

        batch_update_data = []

        # 2行目から (インデックス 0 = 2行目)
        for i, row in enumerate(data_rows):
            row_index = i + 2 # 実際のシート上の行番号
            
            if len(row) <= max(flag_col, body_p1_col, url_col):
                continue
            
            analysis_flag = row[flag_col]
            body_p1 = row[body_p1_col]
            
            if (analysis_flag.upper() == "TRUE" or analysis_flag == "1") and \
               (not body_p1 or body_p1 == "（本文取得失敗）"):
                
                title = row[title_col][:30] if len(row) > title_col else "（タイトル不明）"
                print(f"  - 行 {row_index} (記事: {title}...): 本文(P1-P10)/コメント数/日時補完/コメント本文 を取得中... (完全取得)")
                
                article_url = row[url_col]
                article_id_match = re.search(r"/articles/([a-f0-9]+)", article_url)
                if not article_id_match:
                    print(f"    - URLから記事IDが抽出できませんでした: {article_url}")
                    continue
                
                article_id = article_id_match.group(1)

                article_body_parts, comment_count, full_post_time = get_article_details(article_url)
                
                # (修正済) get_yahoo_news_comments に article_url を渡す
                comments_data = get_yahoo_news_comments(article_id, article_url)
                
                update_row_data = []
                update_row_data.extend(article_body_parts) # G-P列 (10列)
                update_row_data.append(comment_count) # Q列
                
                if full_post_time:
                    jst = full_post_time.astimezone(timedelta(hours=9))
                    update_row_data.append(jst.strftime("%Y/%m/%d %H:%M:%S"))
                else:
                    update_row_data.append("-") # R列

                update_row_data.extend(comments_data) # S-AC列 (10列)
                
                # 更新範囲 (G列 から AC列 まで)
                start_col_letter = gspread.utils.rowcol_to_a1(row_index, body_p1_col + 1)[0]
                end_col_letter = gspread.utils.rowcol_to_a1(row_index, comment_1_col + 9)
                end_col_letter = ''.join([c for c in end_col_letter if not c.isdigit()])

                range_to_update = f"{start_col_letter}{row_index}:{end_col_letter}{row_index}"
                
                batch_update_data.append({
                    'range': range_to_update,
                    'values': [update_row_data]
                })

                time.sleep(3)
        
        if batch_update_data:
            print(f"  ... {len(batch_update_data)} 件の本文/コメントデータをスプレッドシートに一括書き込み中 ...")
            ws.batch_update(batch_update_data, value_input_option="USER_ENTERED")
            print("  ✅ 本文/コメントデータの一括書き込みが完了しました。")

    except Exception as e:
        print(f"  ❌ 本文・コメント取得・書き込み処理中にエラー: {e}")
        traceback.print_exc()


def sort_and_format_sheet(gc):
    """
    SOURCE ワークシートの C列 (投稿日時) の書式を整え、
    シート全体を C列 の降順 (新しい順) でソートする。
    """
    print("\n===== 📑 ステップ③ 記事データのソートと整形 =====")
    ws = get_worksheet(gc, "SOURCE")
    if not ws:
        return

    try:
        # シートが空でないか確認 (行が1行=ヘッダーのみ、または0行の場合ソート不要)
        if ws.row_count <= 1:
            print("  - シートにデータがないため、ソートをスキップします。")
            return

        print(" スプレッドシート上でC列の書式設定とソートを実行します。")
        
        # C列全体の書式設定リクエスト (C2からC列最後まで)
        format_request = {
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,  # 2行目から (0-indexed)
                    "endRowIndex": ws.row_count,
                    "startColumnIndex": 2, # C列 (0-indexed)
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
        }

        # ソートリクエスト (C列=列インデックス2 で降順ソート)
        sort_request = {
            "sortRange": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1, # 2行目から (ヘッダー除く)
                    "endRowIndex": ws.row_count,
                    "startColumnIndex": 0, # A列から
                    "endColumnIndex": ws.col_count
                },
                "sortSpecs": [
                    {
                        "dimensionIndex": 2, # C列 (0-indexed)
                        "sortOrder": "DESCENDING"
                    }
                ]
            }
        }
        
        ws.spreadsheet.batch_update({
            "requests": [format_request, sort_request]
        })
        
        print(f" ✅ C列(2行目〜{ws.row_count}行) の表示形式を 'yyyy/mm/dd hh:mm:ss' に設定しました。")
        print(" ✅ SOURCEシートを投稿日時の新しい順にスプレッドシート上で並び替えました。")

    except Exception as e:
        print(f"  ❌ ソート・書式設定中にエラー: {e}")
        traceback.print_exc()


def analyze_with_gemini_and_update_sheet(gc):
    """
    スプレッドシートの「分析フラグ」が立っている記事（最大30件）をGeminiで分析し、
    結果をP-R列 (sentiment, category, company_info) と
    AD-AE列 (nissan_mention, nissan_sentiment) に一括で書き込む。
    (修正済：API 429 エラー対策のバッチ処理化)
    """
    try:
        if not gemini_model:
            print("\n===== 🧠 ステップ④ (スキップ) =====")
            print("  Geminiモデルが初期化されていないため、分析をスキップします。")
            return

        print("\n===== 🧠 ステップ④ Gemini分析の実行・即時反映 (P-R, AD-AE列) [最大30件] =====")
        ws = get_worksheet(gc, "SOURCE")
        if not ws:
            return

        print("  ... 分析対象データをスプレッドシートから読み込み中 ...")
        all_data = ws.get_all_values()
        if len(all_data) <= 1:
            print("  分析対象データがありません。")
            return

        headers = all_data[0]
        data_rows = all_data[1:]

        # ヘッダー行を取得して、列インデックスを動的に見つける
        try:
            # 必要な列のインデックス（0始まり）を取得
            title_col_idx = headers.index("title") + 1 # E列
            analysis_flag_col_idx = headers.index("analysis_flag") + 1 # F列
            body_col_idx = headers.index("body_p1") + 1 # G列
            sentiment_col_idx = headers.index("sentiment") + 1 # P列
            category_col_idx = headers.index("category") + 1 # Q列
            company_info_col_idx = headers.index("company_info") + 1 # R列
            nissan_mention_col_idx = headers.index("nissan_mention") + 1 # AD列
            nissan_sentiment_col_idx = headers.index("nissan_sentiment") + 1 # AE列

        except ValueError as e:
            print(f"  ❌ 必要な列が見つかりません: {e}。分析を中断します。")
            print(f"  (取得したヘッダー: {headers})")
            return
        
        batch_updates = []
        count = 0
        max_analyze = 30 # 最大分析件数

        # 2行目から (インデックス0 = 2行目)
        for i, row in enumerate(data_rows):
            row_index = i + 2 # 実際のシート上の行番号
            
            if len(row) <= max(analysis_flag_col_idx-1, sentiment_col_idx-1, body_col_idx-1):
                continue

            try:
                analysis_flag = row[analysis_flag_col_idx - 1]
                sentiment = row[sentiment_col_idx - 1]
                
                if (analysis_flag.upper() == "TRUE" or analysis_flag == "1") and (not sentiment or sentiment == "N/A"):
                    
                    if count >= max_analyze:
                        print(f"  分析件数が{max_analyze}件に達したため、残りは次回に回します。")
                        break
                    
                    count += 1
                    title = row[title_col_idx - 1][:30] # タイトル列
                    print(f"  - 行 {row_index} (記事: {title}...): Gemini分析を実行中... ({count}/{max_analyze}件目)")

                    # 本文 (G列からP列の直前まで)
                    body_p1_to_p10 = row[body_col_idx - 1 : body_col_idx + 9]
                    article_body = " ".join([text for text in body_p1_to_p10 if text and text != "-"])
                    
                    if len(article_body.strip()) < 50: 
                        print(f"    ...本文が短すぎるためスキップ (本文: {article_body[:50]}...)")
                        analysis_result = {
                            "sentiment": "N/A (本文短)", "category": "N/A", "company_info": "N/A",
                            "nissan_mention": "-", "nissan_sentiment": "-"
                        }
                    else:
                        analysis_result = analyze_article_with_gemini(article_body)
                    
                    sentiment = analysis_result.get("sentiment", "N/A")
                    category = analysis_result.get("category", "N/A")
                    company_info = analysis_result.get("company_info", "N/A")
                    nissan_mention = analysis_result.get("nissan_mention", "N/A")
                    nissan_sentiment = analysis_result.get("nissan_sentiment", "N/A")

                    # メインの分析結果 (P列〜R列)
                    batch_updates.append({
                        'range': f"{gspread.utils.rowcol_to_a1(row_index, sentiment_col_idx)}:{gspread.utils.rowcol_to_a1(row_index, company_info_col_idx)}",
                        'values': [[sentiment, category, company_info]]
                    })
                    
                    # 日産関連の分析結果 (AD列〜AE列)
                    batch_updates.append({
                        'range': f"{gspread.utils.rowcol_to_a1(row_index, nissan_mention_col_idx)}:{gspread.utils.rowcol_to_a1(row_index, nissan_sentiment_col_idx)}",
                        'values': [[nissan_mention, nissan_sentiment]]
                    })
                    
                    time.sleep(1) 

            except Exception as e:
                print(f"  ❌ 行 {row_index} の処理中にエラー: {e}")
                traceback.print_exc()

        if batch_updates:
            print(f"  ... {len(batch_updates) // 2} 件の分析結果をスプレッドシートに一括書き込み中 ...")
            try:
                ws.batch_update(batch_updates, value_input_option="USER_ENTERED")
                print("  ✅ 分析結果の一括書き込みが完了しました。")
            except Exception as e:
                print(f"  ❌ スプレッドシートへの一括書き込みに失敗しました: {e}")
                traceback.print_exc()
        elif count == 0:
            print("  分析対象（分析フラグがTRUEで未分析）の記事はありませんでした。")

    except Exception as e:
        print(f"  ❌ Gemini分析ステップ全体でエラー: {e}")
        traceback.print_exc()


# (修正) ヘッダー自動設定機能
def check_and_set_headers(ws):
    """
    ワークシートの1行目（ヘッダー）を確認し、
    存在しない場合や不整合がある場合に自動で設定する。
    """
    print("  ヘッダー行（1行目）の整合性を確認中...")
    
    # プログラムが期待するヘッダーの完全なリスト
    expected_headers = [
        'keyword', 'URL', 'post_time_str', 'source', 'title', 'analysis_flag', 
        'body_p1', 'body_p2', 'body_p3', 'body_p4', 'body_p5', 'body_p6', 
        'body_p7', 'body_p8', 'body_p9', 'body_p10', 
        'sentiment', 'category', 'company_info', 
        'comment_count', 'full_post_time', 
        'comment_1', 'comment_2', 'comment_3', 'comment_4', 'comment_5', 
        'comment_6', 'comment_7', 'comment_8', 'comment_9', 'comment_10', 
        'nissan_mention', 'nissan_sentiment'
    ]
    
    try:
        current_headers = ws.row_values(1)
    except GSpreadAPIError as e:
        print(f"  シートが空のようです (エラー: {e})。")
        current_headers = []
    except Exception as e:
        print(f"  ヘッダー行の読み取りに失敗: {e}")
        current_headers = []

    if current_headers != expected_headers:
        print("  ヘッダー行が不足または不整合です。1行目にヘッダーを自動設定します...")
        try:
            ws.update('A1', [expected_headers], value_input_option='RAW')
            print("  ✅ ヘッダー行を更新しました。")
            return True
        except Exception as e:
            print(f"  ❌ ヘッダー行の設定に失敗しました: {e}")
            traceback.print_exc()
            return False
    else:
        print("  ✅ ヘッダー行は正常です。")
        return True


def main():
    """
    メイン処理
    """
    print("--- 統合スクリプト開始 ---")
    start_time = time.time()
    
    # --- セットアップ ---
    gc = setup_gspread()
    if not gc:
        print("スプレッドシート認証に失敗。処理を終了します。")
        return

    ws = get_worksheet(gc, "SOURCE")
    if not ws:
        print("SOURCE ワークシートの取得に失敗。処理を終了します。")
        return
        
    if not check_and_set_headers(ws):
        print("ヘッダー行の設定に失敗したため、処理を終了します。")
        return
        
    if not load_prompts():
        print("プロンプト読み込みに失敗。Gemini分析は実行されません。")

    initialize_gemini() # Gemini APIの初期化

    # --- ステップ① ニュースリスト取得 & ステップ② 本文・コメント取得 ---
    existing_urls = load_existing_urls(ws)
    print(f"  (現在 {len(existing_urls)} 件の記事URLをロード済み)")
    
    for keyword in SEARCH_KEYWORDS:
        print(f"\n===== 🔑 ステップ① ニュースリスト取得: {keyword} =====")
        new_articles = get_yahoo_news_search_results(keyword)
        
        print(f"\n===== 📝 ステップ② 本文/コメント更新 (キーワード: {keyword} 追加後) =====")
        update_source_sheet(ws, new_articles, existing_urls)


    # --- ステップ③ ソート & 書式設定 ---
    sort_and_format_sheet(gc)

    # --- ステップ④ Gemini 分析 ---
    analyze_with_gemini_and_update_sheet(gc)

    end_time = time.time()
    print(f"\n--- 統合スクリプト終了 (所要時間: {end_time - start_time:.2f}秒) ---")


if __name__ == "__main__":
    main()
