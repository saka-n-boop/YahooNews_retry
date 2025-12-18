import time
import re
import requests
from bs4 import BeautifulSoup
import gspread

# --- Selenium 関連 ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# 設定
COMMENTS_SHEET_NAME = "Comments"
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_SELENIUM_PAGES = 10  # Seleniumで取得するページ数上限 (10ページ=100件)

def ensure_comments_sheet(sh: gspread.Spreadsheet):
    """ Commentsシートがなければ作成し、ヘッダーを設定する """
    try:
        ws = sh.worksheet(COMMENTS_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        # 列数は多めに確保 (300列 = KN列まで)
        ws = sh.add_worksheet(title=COMMENTS_SHEET_NAME, rows="1000", cols="300")
        
        # ヘッダー作成
        headers = [
            "URL", "タイトル", "投稿日時", "ソース", 
            "コメント数", "製品批判有無", 
            "コメント要約(全体)", "話題ランキング(TOP5)"
        ]
        
        # コメント本文列：1-10 ... (9列目から開始)
        for i in range(0, 240): 
            start = i * 10 + 1
            end = (i + 1) * 10
            headers.append(f"コメント：{start} - {end}")
            
        ws.update(range_name='A1', values=[headers])
        return ws
        
    return ws

def setup_driver():
    """ Seleniumドライバの初期化 """
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        print(f"      ! Driver初期化失敗: {e}")
        return None

def fetch_comments_hybrid(article_url: str) -> tuple[list[str], str]:
    """ 
    ハイブリッド方式でコメントを取得する
    Phase 1: 上位100件はSeleniumで「もっと見る」を展開して全文取得 (AI用)
    Phase 2: それ以降はRequestsで高速取得 (保存用)
    """
    
    # URL調整
    base_url = article_url.split('?')[0]
    if not base_url.endswith('/comments'):
        if '/comments' in base_url:
             base_url = base_url.split('/comments')[0] + '/comments'
        else:
             base_url = f"{base_url}/comments"

    all_comments_data = [] 
    seen_comments = set()
    
    print(f"    - コメント取得開始(ハイブリッド): {base_url}")

    # ==========================================
    # Phase 1: Selenium (Top 100件)
    # ==========================================
    driver = setup_driver()
    if driver:
        print("      > Phase 1: Seleniumで詳細取得中 (最大10ページ)...")
        for page in range(1, MAX_SELENIUM_PAGES + 1):
            target_url = f"{base_url}?page={page}" # おすすめ順
            try:
                driver.get(target_url)
                # 記事が表示されるまで待機
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "article"))
                )
                
                # 「もっと見る」系ボタンをすべてクリックして展開
                # (クラス名が動的なので、テキストやタグで探す)
                try:
                    # 'もっと見る' を含む要素などを探してクリック
                    expand_buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'もっと見る') or contains(text(), '続きを読む')]")
                    for btn in expand_buttons:
                        try:
                            if btn.is_displayed():
                                driver.execute_script("arguments[0].click();", btn)
                                time.sleep(0.1)
                        except: pass
                except: pass
                
                # 展開後のHTMLをパース
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                articles = soup.find_all('article')
                
                if not articles:
                    break 

                new_cnt = 0
                for art in articles:
                    user_tag = art.find('h2')
                    user_name = user_tag.get_text(strip=True) if user_tag else "匿名"
                    
                    p_tags = art.find_all('p')
                    comment_body = ""
                    if p_tags:
                        comment_body = max([p.get_text(strip=True) for p in p_tags], key=len)
                    
                    if comment_body:
                        # ノイズ除去
                        ignore = ["このコメントを削除しますか", "コメントを削除しました", "違反報告する", "非表示・報告", "投稿を受け付けました"]
                        if any(x in comment_body for x in ignore): continue

                        full_text = f"【投稿者: {user_name}】\n{comment_body}"
                        if full_text in seen_comments: continue
                        
                        seen_comments.add(full_text)
                        all_comments_data.append(full_text)
                        new_cnt += 1
                
                if new_cnt == 0: break # 新しいコメントがなければ終了
                
                time.sleep(1) # ページ遷移待機

            except Exception as e:
                print(f"      ! Seleniumエラー(p{page}): {e}")
                break
        
        driver.quit()
    else:
        print("      ! Selenium起動失敗。全件Requestsモードに切り替えます。")

    # ここまでのデータをAI用として確保
    limit_for_ai = len(all_comments_data)
    ai_target_text = "\n".join(all_comments_data) # Phase 1で取れた全文
    print(f"      > Phase 1 完了: {limit_for_ai}件取得 (AI分析対象)")

    # ==========================================
    # Phase 2: Requests (101件目〜)
    # ==========================================
    start_page = (limit_for_ai // 10) + 1
    if limit_for_ai % 10 != 0: start_page += 1 # 端数調整
    if start_page <= MAX_SELENIUM_PAGES: start_page = MAX_SELENIUM_PAGES + 1 # 重複しないように

    print(f"      > Phase 2: Requestsで残りを高速取得中 (p{start_page}〜)...")

    page = start_page
    while True:
        target_url = f"{base_url}?page={page}"
        try:
            res = requests.get(target_url, headers=REQ_HEADERS, timeout=10)
            if res.status_code == 404: break 
            res.raise_for_status()
        except Exception: break

        soup = BeautifulSoup(res.text, 'html.parser')
        articles = soup.find_all('article')
        if not articles: break 

        new_cnt = 0
        for art in articles:
            user_tag = art.find('h2')
            user_name = user_tag.get_text(strip=True) if user_tag else "匿名"
            
            p_tags = art.find_all('p')
            comment_body = ""
            if p_tags:
                comment_body = max([p.get_text(strip=True) for p in p_tags], key=len)
            
            if comment_body:
                ignore = ["このコメントを削除しますか", "コメントを削除しました", "違反報告する", "非表示・報告", "投稿を受け付けました"]
                if any(x in comment_body for x in ignore): continue

                full_text = f"【投稿者: {user_name}】\n{comment_body}"
                if full_text in seen_comments: continue
                
                seen_comments.add(full_text)
                all_comments_data.append(full_text)
                new_cnt += 1
        
        if new_cnt == 0: break 
        page += 1
        time.sleep(1) 

    # 保存用にデータを整形 (10件ごとに結合)
    merged_columns = []
    chunk_size = 10
    for i in range(0, len(all_comments_data), chunk_size):
        chunk = all_comments_data[i : i + chunk_size]
        merged_text = "\n\n".join(chunk)
        merged_columns.append(merged_text)
    
    print(f"    - 全取得完了: 合計{len(all_comments_data)}件")
    return merged_columns, ai_target_text

def set_row_height(ws, pixels):
    try:
        requests = [{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
            "properties": {"pixelSize": pixels}, "fields": "pixelSize"}}]
        ws.spreadsheet.batch_update({"requests": requests})
    except: pass

def run_comment_collection(gc: gspread.Client, source_sheet_id: str, source_sheet_name: str, summarizer_func):
    """ 
    summarizer_func: main.pyから渡されるGemini分析用関数 
    """
    print("\n=====   ステップ⑤ 条件付きコメント収集・要約・保存 =====")
    
    sh = gc.open_by_key(source_sheet_id)
    try: source_ws = sh.worksheet(source_sheet_name)
    except: return

    dest_ws = ensure_comments_sheet(sh)
    
    # 既存チェック
    dest_rows = dest_ws.get_all_values()
    existing_urls = set()
    if len(dest_rows) > 1:
        existing_urls = set(row[0] for row in dest_rows[1:] if row)

    source_rows = source_ws.get_all_values()
    if len(source_rows) < 2: return
    
    # 生データ
    raw_data_rows = source_rows[1:]
    
    # コメント数順にソートするためのリスト作成
    sorted_target_rows = []
    
    for i, row in enumerate(raw_data_rows):
        if len(row) < 11: continue
        
        # コメント数を数値化
        cnt = 0
        try:
            cnt = int(re.sub(r'\D', '', str(row[5])))
        except:
            cnt = 0
            
        sorted_target_rows.append({
            "original_index": i,
            "count": cnt,
            "data": row
        })
    
    # コメント数が多い順に並び替え
    sorted_target_rows.sort(key=lambda x: x['count'], reverse=True)
    
    print(f"  - 分析順序: コメント数が多い順に {len(sorted_target_rows)} 件をスキャンします。")

    process_count = 0

    for item in sorted_target_rows:
        row = item['data']
        i = item['original_index']
        comment_cnt = item['count'] 
        
        url = row[0]
        title = row[1]
        post_date = row[2]
        source = row[3]
        comment_count_str = row[5]
        target_company = row[6] # G列
        category = row[7]       # H列
        nissan_neg_text = row[10] # K列
        
        if url in existing_urls: continue



        # --- 条件判定 ---
        is_target = False

        # 共通の前提条件: カテゴリーが「その他」で始まらない かつ コメントあり
        if not category.startswith("その他") and comment_cnt > 0:
    
            # 条件①: 対象企業が日産系 かつ コメント数が100件以上
            if target_company.startswith("日産") and comment_cnt >= 100:
            is_target = True
    
            # 条件②: 日産ネガ文に記載がある ("なし" 以外) ※条件①を満たしていない場合のみ判定
            if not is_target:
                val = str(nissan_neg_text).strip()
                if val and val not in ["なし", "N/A", "N/A(No Body)", "-"]:
                    is_target = True
        
        if is_target:
            print(f"  - 対象記事発見(元行{i+2}, コメ数{comment_cnt}): {title[:20]}...")
            
            # ハイブリッド取得 (戻り値: 保存用リスト, AI用テキスト)
            comment_cols, full_text_for_ai = fetch_comments_hybrid(url)
            
            if comment_cols:
                # --- Gemini要約実行 ---
                print("    > Geminiでコメント要約中(Selenium取得分)...")
                summary_data = summarizer_func(full_text_for_ai)
                
                # 結果の展開
                prod_neg = summary_data.get("nissan_product_neg", "N/A")
                
                summaries_list = summary_data.get("summaries", [])
                summary_combined = "\n\n".join(summaries_list) if summaries_list else "-"
                
                rankings_list = summary_data.get("topic_ranking", [])
                ranking_combined = "\n".join(rankings_list) if rankings_list else "-"

                # データ構築
                row_data = [
                    url, title, post_date, source, 
                    comment_count_str, 
                    prod_neg,
                    summary_combined, 
                    ranking_combined
                ] + comment_cols
                
                dest_ws.append_rows([row_data], value_input_option='USER_ENTERED')
                process_count += 1
                
                print("    (Gemini実行完了: 60秒待機...)")
                time.sleep(60) 

    # 最後にソート (日時順)
    if process_count > 0:
        print("  - Commentsシートを日時順にソート中...")
        try:
            last_row = len(dest_ws.col_values(1))
            if last_row > 1:
                # KN列(300列) まで指定
                dest_ws.sort((3, 'des'), range=f'A2:KN{last_row}') 
        except Exception as e: print(f"  ! ソートエラー: {e}")
        set_row_height(dest_ws, 21)

    print(f" ? コメント収集・要約完了: 新たに {process_count} 件処理しました。")

