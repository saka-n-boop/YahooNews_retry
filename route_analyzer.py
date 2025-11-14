import os
import json
import gspread
from google.oauth2.service_account import Credentials
from google_genai import GoogleGenAI
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled
from typing import List, Dict, Optional

# --- 設定値 ---
SPREADSHEET_ID = "1tCXNUuwiIPFLWi1H3Pz4FI81oz4DCvHn5EDlkCTQ3Uk"
# スプレッドシートの列定義
URL_COLUMN_INDEX = 4  # E列 (0から数えて4)
START_COLUMN_INDEX = 12  # M列
END_COLUMN_INDEX = 23  # X列
WAYPOINT_COLUMNS_INDICES = list(range(13, 23))  # N列(13)からW列(22)まで

# --- APIクライアント初期化 ---
try:
    # 1. Google Sheets 認証設定 (サービスアカウント)
    # GitHub SecretsからJSONキーを取得し、一時ファイルとして保存
    sa_key_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
    if not sa_key_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment variables.")

    # サービスアカウントキーを一時ファイルに書き込み
    # gspreadはファイルパスを要求するため
    with open('service_account_key.json', 'w') as f:
        f.write(sa_key_json)

    # 認証
    creds = Credentials.from_service_account_file('service_account_key.json', scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    gc = gspread.authorize(creds)
    
    # 2. Gemini API クライアント初期化
    gemini_client = GoogleGenAI(api_key=os.environ.get('GEMINI_API_KEY'))

    # 3. YouTube APIキーの設定（youtube-transcript-apiはキーを環境変数から自動検出）
    # os.environ.get('YOUTUBE_API_KEY') は必要に応じて設定済みと仮定

except Exception as e:
    print(f"API Client Initialization Error: {e}")
    # 認証失敗は致命的なのでプログラムを終了させる
    exit(1)


# --- 関数定義 ---

def get_video_id(url: str) -> Optional[str]:
    """YouTube URLから動画IDを抽出する"""
    if "youtu.be" in url:
        return url.split("/")[-1].split("?")[0]
    elif "v=" in url:
        return url.split("v=")[-1].split("&")[0]
    return None

def get_transcript(video_id: str) -> Optional[str]:
    """YouTube動画のトランスクリプトを取得する"""
    try:
        # トランスクリプトを取得し、テキストを連結
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['ja', 'en'])
        
        # タイムスタンプを除去し、テキストのみを一つの文字列に結合
        full_transcript = " ".join([item['text'] for item in transcript_list])
        return full_transcript
        
    except TranscriptsDisabled:
        print(f"  > Error: Transcripts are disabled for video {video_id}.")
        return None
    except Exception as e:
        print(f"  > Error: Failed to get transcript for {video_id}. {e}")
        return None

def analyze_route_with_gemini(transcript: str) -> Dict[str, List[str]]:
    """Gemini APIを使用してトランスクリプトからルート情報を分析する"""
    
    # 構造化されたJSON出力を要求するプロンプト
    prompt = f"""
    あなたは、自動車レビューと地理に精通した**プロのテストドライバー**です。
    提供されたトランスクリプトを分析し、車両のレビュー目的で走行した具体的な**スタート地点、経由地、終着地点**を特定してください。
    特に、**具体的な道路名、IC/JCT名、およびランドマーク**を抽出することに重点を置いてください。
    結果は必ず、以下のJSON形式で出力してください。

    - 'start': 走行の開始地点（例: 東京スバル三鷹店）
    - 'end': 走行の終着地点（例: ハンガーエイト）
    - 'waypoints': 経由した場所や道路情報（最大10個。例: 国道4号線を走行、秦野中井IC入口通過、豊田JCT通過）
    
    --- トランスクリプト ---
    {transcript}
    """
    
    try:
        response = gemini_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_schema": {
                    "type": "object",
                    "properties": {
                        "start": {"type": "string"},
                        "end": {"type": "string"},
                        "waypoints": {"type": "array", "items": {"type": "string"}}
                    }
                }
            }
        )
        # JSON文字列をパース
        analysis_result = json.loads(response.text)
        
        # データ構造をチェック
        if 'start' in analysis_result and 'end' in analysis_result and 'waypoints' in analysis_result:
            return analysis_result
        else:
            print("  > Warning: Gemini analysis returned invalid JSON structure.")
            return {'start': '', 'end': '', 'waypoints': []}
            
    except Exception as e:
        print(f"  > Error: Gemini API call failed. {e}")
        return {'start': '', 'end': '', 'waypoints': []}


def main():
    """メイン処理"""
    print("--- YouTube Route Analyzer Start ---")

    try:
        # スプレッドシートとシートを開く
        sheet = gc.open_by_id(SPREADSHEET_ID).sheet1
        
        # 全データを取得
        # values_listで全行を取得し、ヘッダー行(1行目)をスキップ
        all_data = sheet.get_all_values()
        header = all_data[0]
        data_rows = all_data[1:]
        
        print(f"Found {len(data_rows)} data rows to process.")
        
        # 処理結果を格納するためのリスト
        updates = []
        
        for row_index, row in enumerate(data_rows):
            # スプレッドシート上での行番号 (2から開始)
            sheet_row_number = row_index + 2
            
            # URLと既存の書き込み状態をチェック
            url = row[URL_COLUMN_INDEX].strip() if len(row) > URL_COLUMN_INDEX else ""
            current_start = row[START_COLUMN_INDEX].strip() if len(row) > START_COLUMN_INDEX else ""

            if not url:
                print(f"Skipping row {sheet_row_number}: URL is empty.")
                continue

            # 既にデータが書き込まれていればスキップ（M列が空欄の場合のみ処理）
            if current_start:
                print(f"Skipping row {sheet_row_number}: Already analyzed (Start point exists).")
                continue

            print(f"\nProcessing row {sheet_row_number}: {url}")
            
            video_id = get_video_id(url)
            if not video_id:
                print("  > Error: Invalid YouTube URL format.")
                continue

            # 1. トランスクリプトの取得
            transcript = get_transcript(video_id)
            if not transcript:
                print("  > Skipping: Could not retrieve transcript.")
                continue

            # 2. Geminiによるルート分析
            analysis_result = analyze_route_with_gemini(transcript)
            
            # 3. 更新データの準備
            start_point = analysis_result.get('start', '')
            end_point = analysis_result.get('end', '')
            waypoints = analysis_result.get('waypoints', [])
            
            # 書き込み用のデータのリスト (M列からX列まで) を作成
            write_data = [start_point]  # M列 (出発地点)
            
            # N列(経由地1)からW列(経由地10)までを準備
            for i in range(10):
                if i < len(waypoints):
                    write_data.append(waypoints[i])
                else:
                    write_data.append("") # 10個に満たない場合は空欄
            
            write_data.append(end_point) # X列 (終着地点)
            
            # スプレッドシートへの一括書き込み用にデータを整形
            # gspreadのupdateメソッドはA1表記を使い、行全体ではなくM列からX列までを更新
            range_name = f'M{sheet_row_number}:X{sheet_row_number}'
            updates.append({
                'range': range_name,
                'values': [write_data]
            })

            print(f"  > Analyzed: Start='{start_point}', End='{end_point}', Waypoints={len(waypoints)}")

        # 4. スプレッドシートへの一括更新
        if updates:
            print(f"\nApplying {len(updates)} updates to the spreadsheet...")
            sheet.batch_update(updates)
            print("Successfully updated the spreadsheet.")
        else:
            print("\nNo new rows needed analysis or update.")
            
    except Exception as e:
        print(f"\nFATAL ERROR in main execution: {e}")
    finally:
        # スクリプト実行後、サービスアカウントキーファイルを削除（セキュリティのため）
        if os.path.exists('service_account_key.json'):
            os.remove('service_account_key.json')
            
    print("--- YouTube Route Analyzer End ---")

if __name__ == "__main__":
    main()
