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
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# 環境変数からスプレッドシートキーを取得
SPREADSHEET_KEY = os.environ.get("SPREADSHEET_KEY")

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

# --- ヘッダー定義 (ユーザー要望反映) ---
# SOURCEシート: 記事一覧・本文・分析結果
HEADER_SOURCE = [
    "URL",          # A列
    "タイトル",     # B列
    "投稿日時",     # C列
    "ソース",       # D列
    "本文_P1",      # E列
    "本文_P2",      # F列
    "本文_P3",      # G列
    "本文_P4",      # H列
    "本文_P5",      # I列
    "本文_P6",      # J列
    "本文_P7",      # K列
    "本文_P8",      # L列
    "本文_P9",      # M列
    "本文_P10",     # N列
    "コメント数",   # O列
    "主題企業",     # P列
    "カテゴリ",     # Q列
    "ポジネガ",     # R列
    "日産関連文",   # S列
    "日産ネガ文"    # T列
]

# COMMENTSシート: コメント詳細 (分離)
HEADER_COMMENTS = [
    "URL",          # A列
    "タイトル",     # B列
    "投稿日時",     # C列
    "ソース",       # D列
    "コメント_1",   # E列
    "コメント_2",   # F列
    "コメント_3",   # G列
    "コメント_4",   # H列
    "コメント_5",   # I列
    "コメント_6",   # J列
    "コメント_7",   # K列
    "コメント_8",   # L列
    "コメント_9",   # M列
    "コメント_10"   # N列
]


def setup_gspread():
    """Google スプレッドシート API への認証を行う"""
    try:
        creds_json_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        if not creds_json_str:
            print("❌ 環境変数 'GCP_SERVICE_ACCOUNT_KEY' が設定されていません。")
            return None

        creds_dict = json.loads(creds_json_str)
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(credentials)
        
        # 接続テスト
        gc.open_by_key(SPREADSHEET_KEY)
        print("✅ Googleスプレッドシートへの認証に成功しました。")
        return gc

    except Exception as e:
        print(f"❌ Googleスプレッドシートへの認証に失敗しました: {e}")
        return None


def get_worksheet(gc, sheet_name):
    """ワークシートを取得、なければ作成する"""
    if not gc: return None
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except GSpreadAPIError:
            print(f"  シート '{sheet_name}' が見つからないため新規作成します。")
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=20)
        return worksheet
    except Exception as e:
        print(f"❌ ワークシート '{sheet_name}' 操作エラー: {e}")
        return None


def check_and_set_headers(ws, expected_headers):
    """ヘッダー行を確認し、不整合があれば設定（上書き）する"""
    try:
        current_headers = ws.row_values(1)
    except Exception:
        current_headers = []

    if current_headers != expected_headers:
        print(f"  '{ws.title}' のヘッダーを更新します...")
        try:
            # データ混在を防ぐため、ヘッダー不整合時は一度クリアして再設定
            if not current_headers: # 真っ白な場合のみクリアせずにセット(安全策)
                 ws.update('A1', [expected_headers], value_input_option='RAW')
            else:
                 # 既存データがあるがヘッダーが違う場合、1行目だけ書き換える
                 ws.update('A1', [expected_headers], value_input_option='RAW')
            
            print(f"  ✅ '{ws.title}' のヘッダーを設定しました。")
            return True
        except Exception as e:
            print(f"  ❌ ヘッダー設定失敗: {e}")
            return False
    return True


def load_existing_urls(ws):
    """SOURCEシートのA列(URL)を読み込み、セットで返す"""
    try:
        urls = ws.col_values(1) # A列
        return set(urls[1:]) # ヘッダー除く
    except Exception:
        return set()


# --- スクレイピング関連関数 ---

def get_yahoo_news_search_results(keyword):
    """Yahoo!ニュース検索結果を取得"""
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(search_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 検索結果コンテナを探す
        container = soup.find("ol", class_="newsFeed_list") or \
                    soup.find("div", class_="newsFeed") or \
                    soup.find("div", class_="NewsFeed") or \
                    soup.find("div", class_=re.compile(r"Search__ResultList"))

        if not container:
            return []

        articles = container.find_all("li")
        if not articles:
            articles = container.find_all("div", class_="newsFeed_item")

        results = []
        for article in articles:
            try:
                body_tag = article.find("div", class_="newsFeed_item_body")
                if not body_tag: continue

                title_tag = body_tag.find_parent("a")
                if not title_tag or "href" not in title_tag.attrs: continue

                url = title_tag["href"]
                if not url.startswith("https://news.yahoo.co.jp/articles/"): continue

                # タイトル・ソース・時間取得
                title = "（タイトル取得失敗）"
                source = "発行元不明"
                post_time_str = "時間不明"

                time_tag = body_tag.find("time")
                if time_tag:
                    post_time_str = time_tag.text.strip()
                    meta_div = time_tag.find_parent("div")
                    if meta_div:
                        source_span = meta_div.find("span")
                        if source_span: source = source_span.text.strip()

                # タイトル取得ロジック
                title_text_div = body_tag.find("div", class_=re.compile(r"^sc-"))
                if title_text_div:
                    title = title_text_div.get_text(strip=True)
                elif title_tag.text:
                    title = title_tag.text.strip().split("\n")[0]

                results.append({
                    "title": title, "url": url, "source": source,
                    "post_time_str": post_time_str, "keyword": keyword
                })
            except Exception:
                continue
        return results

    except Exception as e:
        print(f"  Search Error: {e}")
        return []


def get_article_details(article_url):
    """記事詳細（本文P1-P10、コメント数、正確な日時）を取得"""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    parts = []
    comment_count = "0"
    full_post_time = None

    try:
        # 1ページ目
        resp = requests.get(article_url, headers=headers)
        if resp.status_code != 200: return [""]*10, "0", None
        soup = BeautifulSoup(resp.text, "html.parser")

        # コメント数
        c_tag = soup.find("a", href=re.compile(r"/comments/")) or soup.find("button", class_=re.compile(r"sc-"))
        if c_tag:
            m = re.search(r"(\d+)", c_tag.text)
            if m: comment_count = m.group(1)

        # 日時
        t_tag = soup.find("time")
        if t_tag and t_tag.has_attr("datetime"):
            try:
                full_post_time = datetime.fromisoformat(t_tag["datetime"].replace("Z", "+00:00"))
            except: pass

        # 本文P1
        body_div = soup.find("div", class_="article_body")
        if body_div:
            parts.append(body_div.get_text(separator="\n", strip=True))
        else:
            parts.append("（本文取得失敗）")

        # P2以降
        for p in range(2, 11):
            next_url = f"{article_url}?page={p}"
            try:
                r_sub = requests.get(next_url, headers=headers)
                if r_sub.status_code != 200: break
                s_sub = BeautifulSoup(r_sub.text, "html.parser")
                b_sub = s_sub.find("div", class_="article_body")
                if b_sub:
                    text = b_sub.get_text(separator="\n", strip=True)
                    if text == parts[0]: break # 1ページ目と同じなら終了
                    parts.append(text)
                else:
                    break
                time.sleep(1)
            except: break
            
    except Exception:
        parts.append("（エラー）")

    # 10個に埋める
    if len(parts) < 10:
        parts.extend([""] * (10 - len(parts)))
    
    return parts[:10], comment_count, full_post_time


def get_yahoo_news_comments(article_id, article_url):
    """コメント本文を取得（最大10件）"""
    comments = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    try:
        base_url = f"{article_url}/comments"
        for p in range(1, 4): # 1-3ページ
            url = base_url if p == 1 else f"{base_url}?page={p}"
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200: break
            
            soup = BeautifulSoup(resp.text, "html.parser")
            main_area = soup.find("article", id="comment-main")
            if not main_area: break
            
            # 記事ごとのarticleタグ
            items = main_area.find_all("article", class_=re.compile(r"sc-"))
            if not items: break
            
            for item in items:
                name = "不明"
                text = ""
                
                h2 = item.find("h2")
                if h2: name = h2.get_text(strip=True)
                
                p_tag = item.find("p", class_=re.compile(r"sc-.*-\d{1,2}$"))
                if p_tag: text = p_tag.get_text(strip=True)
                
                comments.append(f"【{name}】{text}")
                if len(comments) >= 10: break
            
            if len(comments) >= 10: break
            time.sleep(1)

    except Exception:
        pass

    if not comments:
        comments = ["取得なし"]
        
    if len(comments) < 10:
        comments.extend([""] * (10 - len(comments)))
        
    return comments[:10]


# --- Gemini関連関数 ---

def load_prompts():
    global PROMPTS
    print("  プロンプトファイルを読み込んでいます...")
    try:
        for key, path in PROMPT_FILES.items():
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    PROMPTS[key] = f.read()
            else:
                PROMPTS[key] = ""
        return True
    except Exception as e:
        print(f"  プロンプト読み込みエラー: {e}")
        return False

def initialize_gemini():
    global gemini_model
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("❌ 環境変数 'GOOGLE_API_KEY' がありません。AI分析はスキップされます。")
        return
    try:
        genai.configure(api_key=api_key)
        gemini_model = genai.GenerativeModel('gemini-pro')
        print("✅ Gemini初期化成功")
    except Exception as e:
        print(f"❌ Gemini初期化失敗: {e}")

def analyze_article_with_gemini(article_body):
    if not gemini_model: return {}
    
    # 本文が長すぎる場合はカット
    text = article_body[:8000]
    
    prompt = f"""
{PROMPTS.get("role", "あなたはアナリストです。")}

【記事本文】
{text}

【タスク】
以下の情報をJSON形式で抽出してください。
keys: "sentiment", "category", "company_info", "nissan_mention", "nissan_sentiment"

1. sentiment: {PROMPTS.get("sentiment", "ポジティブ/ネガティブ/ニュートラル")}
2. category: {PROMPTS.get("category", "カテゴリ分類")}
3. company_info: {PROMPTS.get("company_info", "主題企業")}
4. nissan_mention: {PROMPTS.get("nissan_mention", "日産への言及有無")}
5. nissan_sentiment: {PROMPTS.get("nissan_sentiment", "日産言及の感情")}

出力はJSONのみ。
"""
    try:
        resp = gemini_model.generate_content(prompt)
        match = re.search(r"\{.*\}", resp.text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except Exception:
        pass
    
    return {}


# --- メイン処理フロー (改修版) ---

def update_sheets_data(gc):
    """記事収集、詳細更新、コメント分離を一括実行"""
    ws_source = get_worksheet(gc, "SOURCE")
    ws_comments = get_worksheet(gc, "コメントシート")

    if not ws_source or not ws_comments: return

    # 1. ヘッダー設定
    check_and_set_headers(ws_source, HEADER_SOURCE)
    check_and_set_headers(ws_comments, HEADER_COMMENTS)

    # 2. 既存URL確認
    existing_urls = load_existing_urls(ws_source)
    print(f"  (既存URL: {len(existing_urls)}件)")

    # 3. 新規記事検索 & 追加
    new_data = []
    for kw in SEARCH_KEYWORDS:
        articles = get_yahoo_news_search_results(kw)
        for art in articles:
            if art["url"] not in existing_urls:
                # A:URL, B:Title, C:Time, D:Source. 残りは空
                row = [art["url"], art["title"], art["post_time_str"], art["source"]]
                row.extend([""] * 16) # E列以降を空埋め
                new_data.append(row)
                existing_urls.add(art["url"])
    
    if new_data:
        print(f"  ✅ 新規記事 {len(new_data)} 件を追加します。")
        ws_source.append_rows(new_data, value_input_option="USER_ENTERED")
    else:
        print("  新規記事はありません。")

    # 4. 詳細情報の更新 (本文がない行を対象)
    all_rows = ws_source.get_all_values()
    if len(all_rows) <= 1: return

    batch_source = []
    batch_comments = []

    # ヘッダー除く
    for i, row in enumerate(all_rows[1:]):
        row_num = i + 2
        
        # 安全策: 列が足りない場合は拡張
        if len(row) < 5: row.extend([""] * (5 - len(row)))
        
        url = row[0]
        title = row[1]
        time_str = row[2]
        source = row[3]
        body_p1 = row[4]

        # 本文(P1)が空なら詳細取得対象
        if not body_p1 or body_p1 == "（本文取得失敗）":
            print(f"  詳細取得中: {title[:15]}...")
            
            # 記事ID
            m = re.search(r"/articles/([a-f0-9]+)", url)
            art_id = m.group(1) if m else ""
            
            # 詳細取得
            body_parts, c_count, dt_obj = get_article_details(url)
            comments_list = get_yahoo_news_comments(art_id, url)

            # 正確な日時があれば更新
            final_time = time_str
            if dt_obj:
                final_time = dt_obj.astimezone(timedelta(hours=9)).strftime("%Y/%m/%d %H:%M:%S")

            # SOURCEシート更新用データ (C列～O列)
            # C:Time, D:Source, E-N:Body, O:Count
            update_vals = [final_time, source] + body_parts + [c_count]
            
            batch_source.append({
                'range': f"C{row_num}:O{row_num}",
                'values': [update_vals]
            })

            # COMMENTSシート追加用データ
            # A:URL, B:Title, C:Time, D:Source, E-N:Comments
            c_row = [url, title, final_time, source] + comments_list
            batch_comments.append(c_row)
            
            time.sleep(2)

    if batch_source:
        print(f"  ... {len(batch_source)} 件の詳細データを SOURCEシートへ更新中...")
        ws_source.batch_update(batch_source, value_input_option="USER_ENTERED")
    
    if batch_comments:
        print(f"  ... {len(batch_comments)} 件のコメントを コメントシートへ追記中...")
        ws_comments.append_rows(batch_comments, value_input_option="USER_ENTERED")


def analyze_gemini_new(gc):
    """AI分析を実行して結果をP-T列に書き込む"""
    if not gemini_model:
        print("  Gemini未設定のため分析スキップ")
        return

    ws = get_worksheet(gc, "SOURCE")
    all_rows = ws.get_all_values()
    if len(all_rows) <= 1: return
    
    print("\n===== Gemini AI 分析開始 =====")
    batch_updates = []
    count = 0
    
    for i, row in enumerate(all_rows[1:]):
        if count >= 30: # リミット
            print("  30件に達したため分析中断")
            break

        row_num = i + 2
        # データ不足チェック
        if len(row) < 18: 
            # 行データが短い場合は空文字で埋めておくなどが必要だが、
            # ここでは単純にスキップまたは取得
            current_p1 = row[4] if len(row) > 4 else ""
            current_sent = "" # R列なし
        else:
            current_p1 = row[4]
            current_sent = row[17] # R列 (index 17)

        # 本文があり、かつ分析結果(R列)が空の場合
        if current_p1 and (not current_sent or current_sent == ""):
            count += 1
            print(f"  分析実行中: 行{row_num}")
            
            # 本文結合 E(4) - N(13)
            full_body = " ".join([x for x in row[4:14] if x])
            
            res = analyze_article_with_gemini(full_body)
            
            # P:主題(company), Q:カテゴリ, R:ポジネガ(sentiment), S:日産文, T:日産ネガ
            vals = [
                res.get("company_info", "-"),
                res.get("category", "-"),
                res.get("sentiment", "-"),
                res.get("nissan_mention", "-"),
                res.get("nissan_sentiment", "-")
            ]
            
            batch_updates.append({
                'range': f"P{row_num}:T{row_num}",
                'values': [vals]
            })
            time.sleep(2)

    if batch_updates:
        print(f"  ... {len(batch_updates)} 件の分析結果を書き込み中...")
        ws.batch_update(batch_updates, value_input_option="USER_ENTERED")
    else:
        print("  新規分析対象なし")


def sort_source_sheet(gc):
    """SOURCEシートを投稿日時(C列)で降順ソート"""
    ws = get_worksheet(gc, "SOURCE")
    if not ws: return
    print("  SOURCEシートを日時順にソート中...")
    try:
        req = {
            "sortRange": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 20
                },
                "sortSpecs": [{"dimensionIndex": 2, "sortOrder": "DESCENDING"}] # C列=Index 2
            }
        }
        ws.spreadsheet.batch_update({"requests": [req]})
    except Exception as e:
        print(f"  ソートエラー: {e}")


def main():
    print("--- 統合スクリプト開始 ---")
    
    gc
