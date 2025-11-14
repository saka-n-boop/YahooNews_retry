import os
import json
import gspread
from google.oauth2.service_account import Credentials
import google.generativeai as genai
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled
from typing import List, Dict, Optional

# --- 設定値 ---
SPREADSHEET_ID = "1tCXNUuwiIPFLWi1H3Pz4FI81oz4DCvHn5EDlkCTQ3Uk"
URL_COLUMN_INDEX = 4  # E列 (0から数えて4)
START_COLUMN_INDEX = 12  # M列
END_COLUMN_INDEX = 23  # X列
WAYPOINT_COLUMNS_INDICES = list(range(13, 23))  # N列(13)からW列(22)まで

# --- APIクライアント初期化 ---
try:
    # Google Sheets 認証設定 (サービスアカウント)
    sa_key_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
    if not sa_key_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment variables.")

    with open('service_account_key.json', 'w') as f:
        f.write(sa_key_json)

    creds = Credentials.from_service_account_file('service_account_key.json', scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    gc = gspread.authorize(creds)

    # Gemini API クライアント初期化（公式通り）
    genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))
    gemini_model = genai.GenerativeModel('gemini-1.5-flash')  # モデル名は用途に応じて

except Exception as e:
    print(f"API Client Initialization Error: {e}")
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
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['ja', 'en'])
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
        response = gemini_model.generate_content(prompt)
        analysis_result = json.loads(response.text)
        if 'start' in analysis_result and 'end' in analysis_result and 'waypoints' in analysis_result:
            return analysis_result
        else:
            print("  > Warning: Gemini analysis returned invalid JSON structure.")
            return {'start': '', 'end': '', 'waypoints': []}
    except Exception as e:
        print(f"  > Error: Gemini API call failed. {e}")
        return {'start': '', 'end': '', 'waypoints': []}

def main():
    print("--- YouTube Route Analyzer Start ---")
    try:
        sheet = gc.open_by_id(SPREADSHEET_ID).sheet1
        all_data = sheet.get_all_values()
        header = all_data[0]
        data_rows = all_data[1:]
        print(f"Found {len(data_rows)} data rows to process.")
        updates = []
        for row_index, row in enumerate(data_rows):
            sheet_row_number = row_index + 2
            url = row[URL_COLUMN_INDEX].strip() if len(row) > URL_COLUMN_INDEX else ""
            current_start = row[START_COLUMN_INDEX].strip() if len(row) > START_COLUMN_INDEX else ""
            if not url:
                print(f"Skipping row {sheet_row_number}: URL is empty.")
                continue
            if current_start:
                print(f"Skipping row {sheet_row_number}: Already analyzed (Start point exists).")
                continue
            print(f"\nProcessing row {sheet_row_number}: {url}")
            video_id = get_video_id(url)
            if not video_id:
                print("  > Error: Invalid YouTube URL format.")
                continue
            transcript = get_transcript(video_id)
            if not transcript:
                print("  > Skipping: Could not retrieve transcript.")
                continue
            analysis_result = analyze_route_with_gemini(transcript)
            start_point = analysis_result.get('start', '')
            end_point = analysis_result.get('end', '')
            waypoints = analysis_result.get('waypoints', [])
            write_data = [start_point]
            for i in range(10):
                if i < len(waypoints):
                    write_data.append(waypoints[i])
                else:
                    write_data.append("")
            write_data.append(end_point)
            range_name = f'M{sheet_row_number}:X{sheet_row_number}'
            updates.append({
                'range': range_name,
                'values': [write_data]
            })
            print(f"  > Analyzed: Start='{start_point}', End='{end_point}', Waypoints={len(waypoints)}")
        if updates:
            print(f"\nApplying {len(updates)} updates to the spreadsheet...")
            sheet.batch_update(updates)
            print("Successfully updated the spreadsheet.")
        else:
            print("\nNo new rows needed analysis or update.")
    except Exception as e:
        print(f"\nFATAL ERROR in main execution: {e}")
    finally:
        if os.path.exists('service_account_key.json'):
            os.remove('service_account_key.json')
    print("--- YouTube Route Analyzer End ---")

if __name__ == "__main__":
    main()
