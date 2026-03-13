from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaIoBaseDownload, HttpError
from googleapiclient.discovery import build
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import io, requests, os, sys
import signal
import shutil
import time
import mimetypes
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import questionary

# ロギングの設定
import logging
from colorama import init, Fore, Style

# coloramaの初期化（WindowsのANSIエスケープシーケンス対応）
init(autoreset=True)

jst_today = datetime.now().astimezone(timezone(timedelta(hours=9)))
jst_today_str = jst_today.strftime("%Y%m%d%H%M%S")

log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"{jst_today_str}.log")

logger = logging.getLogger("ClassroomArchiver")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

def log_info(msg):
    logger.info(msg)

def log_error(msg):
    logger.error(f"{Fore.RED}{msg}{Style.RESET_ALL}", exc_info=True)

def log_warning(msg):
    logger.warning(f"{Fore.YELLOW}{msg}{Style.RESET_ALL}")

def log_debug(msg, exc_info=False):
    logger.debug(msg, exc_info=exc_info)

def main():
    print("=========================================",
        "\n   Classroom Archiver v1.0",
        "\n=========================================",
        "\n※本ツールは個人開発のソフトです。",
        "\n※使用により生じた損害について作者は責任を負いません。",
        "\n※再配布および商用利用は禁止されています。\n")

    create_new = True
    os.makedirs(f"./classroomArchive", exist_ok=True)
    p = Path("./classroomArchive")
    folders = [f.name for f in p.iterdir() if f.is_dir()]
    if len(folders) > 0:
        result = input("過去のアーカイブ（中断されたものを含む）が存在します。利用しますか？ (y/N)")
        if result.lower() == "y":
            create_new = False
            folders.sort(reverse=True)
            choices = []
            for name in folders:
                if len(name) != 14 or not name.isdigit():
                    continue
                date = datetime.strptime(name, "%Y%m%d%H%M%S")
                choices.append(f"{name} ({date.strftime('%Y/%m/%d %H:%M:%S')} 作成)")
            selected = questionary.select(
                "どのアーカイブを利用しますか？",
                choices=choices,
            ).ask()
            if selected:
                archive_date = selected.split(" ")[0]
                log_info(f"アーカイブ: {archive_date} を利用します。")
            else:
                # Ctrl + C
                sys.exit(1)
    if create_new:
        archive_date = jst_today_str
        log_info(f"アーカイブ: {archive_date} を新しく作成します。")

    base_dir = f"classroomArchive/{archive_date}"
    os.makedirs(f"{base_dir}", exist_ok=True)
    log_info(f"保存先: {Path(base_dir).resolve()}")

    import threading
    thread_local = threading.local()
    lock = threading.Lock()
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda sig, frame: stop_event.set())

    def resource_path(path):
        if hasattr(sys, "_MEIPASS"):
            return os.path.join(sys._MEIPASS, path)
        return os.path.join(os.path.abspath("."), path)

    materials_dir = resource_path("materials")

    os.makedirs(f"{base_dir}/driveFiles", exist_ok=True)
    os.makedirs(f"{base_dir}/css", exist_ok=True)
    os.makedirs(f"{base_dir}/img", exist_ok=True)
    os.makedirs(f"{base_dir}/img/icons", exist_ok=True)
    shutil.copy(os.path.join(materials_dir, "style.css"), f"{base_dir}/css/style.css")
    shutil.copy(os.path.join(materials_dir, "assignment.svg"), f"{base_dir}/img/assignment.svg")
    shutil.copy(os.path.join(materials_dir, "book.svg"), f"{base_dir}/img/book.svg")
    shutil.copy(os.path.join(materials_dir, "user.svg"), f"{base_dir}/img/user.svg")


    SCOPES = [
        "https://www.googleapis.com/auth/classroom.courses.readonly",
        "https://www.googleapis.com/auth/classroom.announcements.readonly",
        "https://www.googleapis.com/auth/classroom.coursework.me",
        "https://www.googleapis.com/auth/classroom.courseworkmaterials.readonly",
        "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
        "https://www.googleapis.com/auth/classroom.rosters.readonly",
        "https://www.googleapis.com/auth/classroom.profile.photos",
        "https://www.googleapis.com/auth/classroom.topics.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.file",
    ]

    
    log_info("\nOAuth認証を行います。ClassroomをアーカイブするGoogleアカウントでログインして下さい。")
    
    input("Enterキーを押すと、ログインページに移動します。\n")


    flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
    creds = flow.run_local_server(port=0)
    service = build("classroom", "v1", credentials=creds)

    # API レート制限対応
    def execute_with_retry(api_request, max_retries=5):
        """
        APIリクエストを実行し、レート制限時に指数バックオフでリトライする汎用関数
        :param api_request: service.files().copy(...) などの実行前のリクエストオブジェクト
        """
        for n in range(max_retries):
            try:
                return api_request.execute()
            
            except HttpError as error:
                # 403 (Rate Limit) や 429 (Too Many Requests) を判定
                if error.resp.status in [403, 429]:
                    wait_time = (2 ** n)  # 指数バックオフ
                    log_warning(f"レート制限(status:{error.resp.status})が発生。{wait_time}秒待機して再試行します...")
                    time.sleep(wait_time)
                else:
                    raise error
                    
        raise Exception("最大再試行回数を超えてもAPI実行制限が解除されませんでした。")

    env = Environment(loader=FileSystemLoader(materials_dir))
    template = env.get_template("course.html")

    archive_folder_id = None

    try:
        drive_service = build("drive", "v3", credentials=creds)

        folder_name = "Classroom Archive"

        query = (
            f"name = '{folder_name}' "
            f"and 'root' in parents "
            f"and mimeType = 'application/vnd.google-apps.folder' "
            f"and trashed = false"
        )

        results = execute_with_retry(drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)', # 必要なフィールドだけ取得
            pageSize=10,
            supportsAllDrives=True,
        ))
        
        items = results.get('files', [])
        root_folder_id = None

        if not items:
            file_metadata = {
                "name": "Classroom Archive",
                "mimeType": "application/vnd.google-apps.folder",
            }
            file = execute_with_retry(drive_service.files().create(body=file_metadata, fields="id", supportsAllDrives=True,))
            root_folder_id = file.get("id")
        else:
            # 複数ヒットする可能性があるため、最初の一つを返す
            folder = items[0]
            root_folder_id = folder["id"]

        # 個別フォルダ作成
        file_metadata = {
            "name": archive_date,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [root_folder_id],
        }
        file = execute_with_retry(drive_service.files().create(body=file_metadata, fields="id", supportsAllDrives=True, ))
        archive_folder_id = file.get("id")
        
    except HttpError as error:
        log_error(f"An error occurred: {error}")
        log_error("プログラムを終了します。詳細はログファイルを確認してください。")
        sys.exit(1)


    def format_size(size):
        size = int(size)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024


    def list_all(method, key):
        items = []
        page_token = None
        
        while True:
            result = execute_with_retry(method(pageToken=page_token))
            items.extend(result.get(key, []))
            page_token = result.get("nextPageToken")

            if not page_token:
                break

        return items


    courses = list_all(
        lambda **kwargs: service.courses().list(**kwargs),
        "courses"
    )

    user_profiles = {}
    all_icons_to_download = set()
    all_drive_files_to_download = set()
    all_drive_files_to_copy = set()
    all_drive_folders_to_copy = set()
    all_files_to_download_size = 0
    file_cache = {}
    course_folders = {}

    # 1GBの閾値 (バイト単位)
    THRESHOLD_GB = 1 * 1024 * 1024 * 1024
    THRESHOLD_100MB = 100 * 1024 * 1024
    THRESHOLD_5MB = 5 * 1024 * 1024


    def get_jst_str(iso_str):
        utc_dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        jst_timezone = timezone(timedelta(hours=9))
        jst_dt = utc_dt.astimezone(jst_timezone)
        jst_dt_str = f"{jst_dt.year}年{jst_dt.month}月{jst_dt.day}日 {jst_dt.hour}時{jst_dt.minute}分"
        return jst_dt_str

    def get_file_type_name(mime_type):
        if mime_type == None:
            return None
        elif mime_type == "application/vnd.google-apps.document":
            return "Google ドキュメント"
        elif mime_type == "application/vnd.google-apps.presentation":
            return "Google スライド"
        elif mime_type == "application/vnd.google-apps.spreadsheet":
            return "Google スプレッドシート"
        else:
            return None

    def get_download_file_path(id, name):
        return f"{base_dir}/driveFiles/id_{id}_name_{name}"

    # (dict | None) を返す。
    # None の場合はダウンロード・コピー共に不可
    # ダウンロード可能なファイルのみリストに追加し、それ以外はドライブにコピーする。
    def fetch_drive_file_details(drive_file):
        file_name = drive_file["title"]
        file_id = drive_file["id"]

        # 強制終了用
        if stop_event.is_set():
            log_warning(f"Cancelled: {file_name}")
            return None

        if not hasattr(thread_local, "drive_service"):
            thread_local.drive_service = build("drive", "v3", credentials=creds)

        drive_service = thread_local.drive_service

        # 利用不可の文字を消す
        file_name = re.sub(r'[\\/*?:"<>|]', "_", file_name)
        file_name = file_name.replace("\n", " ")
        file_name = file_name.rstrip(" .")

        file_type = None

        # ファイル情報を取得する前の確認
        path = get_download_file_path(file_id, file_name)
        if os.path.exists(path):
            log_info(f"Skip (already exists): {file_name}")
            mime_type = mimetypes.guess_file_type(file_name)[0]
            if mime_type:
                extension = mimetypes.guess_extension(mime_type)
                file_type = extension.upper()[1:]
            return {
                "file_name": file_name,
                "file_type": file_type,
                "save_type": "download (skipped)",
                "size": 0,
            }
        
        if file_id in file_cache:
            file = file_cache[file_id]
        else:
            try:
                # 仮に404ならここでエラーが出る
                file = execute_with_retry(drive_service.files().get(
                    fileId=file_id,
                    fields="name,mimeType,size,capabilities",
                    supportsAllDrives=True,
                ))
                file_cache[file_id] = file
                if not "size" in file:
                    file["size"] = 0
            except HttpError as e:
                try:
                    # 課題等で稀にClassroomが返してるIDとDriveの実ファイルIDが別物になっている場合がある
                    m = re.search(r'/d/([a-zA-Z0-9_-]+)', drive_file["alternateLink"])
                    file_id = m.group(1) if m else None
                    if file_id:
                        file = execute_with_retry(drive_service.files().get(
                            fileId=file_id,
                            fields="name,mimeType,size,capabilities",
                            supportsAllDrives=True,
                        ))
                        file_cache[file_id] = file
                        if not "size" in file:
                            file["size"] = 0
                    else:
                        log_warning(f"ファイル（{file_name}）の情報が取得できなかったためスキップします。ステータスコード: {e.status_code}")
                        log_debug(f"Failed to get file information; drive_file: {drive_file}; error: {e}", exc_info=True)
                        return None
                except HttpError as e:
                    log_warning(f"ファイル（{file_name}）の情報が取得できなかったためスキップします。ステータスコード: {e.status_code}")
                    log_debug(f"Failed to get file information; drive_file: {drive_file}; error: {e}", exc_info=True)
                    return None
            
        mime_type = file["mimeType"]
        size = int(file["size"])

        drive_extension = mimetypes.guess_extension(mime_type)
        if drive_extension:
            file_type = f"{drive_extension.upper()[1:]} ファイル"
        else:
            file_type = get_file_type_name(mime_type)

        # 拡張子が必要な場合は付与
        if drive_extension and not file_name.lower().endswith(drive_extension.lower()) and not mime_type.startswith("application/vnd.google-apps"):
            file_name += drive_extension

        # ファイル情報を取得して拡張子を補完した後にもう一度確認
        path = get_download_file_path(file_id, file_name)
        if os.path.exists(path):
            log_info(f"Skip (already exists): {file_name}")
            return {
                "file_name": file_name,
                "file_type": file_type,
                "save_type": "download (skipped)",
                "size": 0,
            }

        if mime_type == "application/vnd.google-apps.folder":
            return {
                "file_name": file_name,
                "file_type": "ドライブフォルダ",
                "save_type": "copy (folder)",
                "size": 0,
            }

        elif mime_type.startswith("application/vnd.google-apps.") and file["capabilities"]["canCopy"]:
            return {
                "file_name": file_name,
                "file_type": file_type,
                "save_type": "copy",
                "size": 0,
            }
        
        elif file["capabilities"]["canDownload"]:
            return {
                "file_name": file_name,
                "file_type": file_type,
                "save_type": "download",
                "size": size,
            }
        else:
            if not file["capabilities"]["canDownload"]:
                log_info(f"ファイルのダウンロードが許可されていません。ファイル名: {file_name}, リンク: {drive_file['alternateLink']}")
            elif not file["capabilities"]["canCopy"]:
                log_info(f"ファイルのコピーが許可されていません。ファイル名: {file_name}, リンク: {drive_file['alternateLink']}")
            else:
                log_info(f"ダウンロード・コピーが両方できないファイル形式です。ファイル名: {file_name}, リンク: {drive_file['alternateLink']}")
                log_debug(f"Failed to get file information; drive_file: {drive_file};", exc_info=True)
            return None


    def download_file(url, path):
        # 強制終了用
        if stop_event.is_set():
            log_warning(f"Cancelled: {path}")
            return 
        
        r = requests.get(url, )
        if r.status_code == 200:
            with open(path, "wb") as f:
                f.write(r.content)
        else:
            log_warning(f"Failed to save {path}.png; status_code: {r.status_code};")


    # Google ファイル以外のダウンロード
    def download_drive_file(file_id, file_name, expected_size: int, pbar):
        # 強制終了用
        if stop_event.is_set():
            log_warning(f"Cancelled: {file_name}")
            pbar.update(expected_size)
            return None

        if not hasattr(thread_local, "drive_service"):
            thread_local.drive_service = build("drive", "v3", credentials=creds)

        drive_service = thread_local.drive_service
        
        path = get_download_file_path(file_id, file_name)
        downloaded_in_this_session = 0

        try:
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.FileIO(path, "wb")
            downloader = MediaIoBaseDownload(fh, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()
                log_debug(f"Downloading file... filename: {file_name}; file_id: {file_id}; progress: {int(status.progress() * 100)}%; done: {done}")
                if status:
                    current_total = int(status.resumable_progress)
                    delta = current_total - downloaded_in_this_session
                    pbar.update(delta)
                    downloaded_in_this_session = current_total
        except HttpError as error:
            log_warning(f"ファイル（{file_name}）をダウンロードできませんでした。ファイルID: {file_id}, ステータスコード: {error.status_code}")
            log_debug(f"Failed to download file; filename: {file_name}; file_id: {file_id}; error: {error}")
            done = True
        
        finally:
            # 最終的に「予定サイズ」と「実際に進んだサイズ」の差分を埋める
            # 権限エラー等で 0 バイトだった場合も、ここで expected_size 分進む
            diff = expected_size - downloaded_in_this_session
            if diff > 0:
                pbar.update(diff)
        

    def copy_drive_file(course_folder_id, file_id, file_name):
        # 強制終了用
        if stop_event.is_set():
            log_warning(f"Cancelled: {file_name}")
            return None

        if not hasattr(thread_local, "drive_service"):
            thread_local.drive_service = build("drive", "v3", credentials=creds)

        drive_service = thread_local.drive_service

        try:
            copied_file = execute_with_retry(drive_service.files().copy(
                fileId=file_id,
                body={
                    "name": file_name,
                    "parents": [course_folder_id] # Apps Script (.gs) は親フォルダ指定無視でドライブ直下に保存される
                },
                fields="id,name,webViewLink,mimeType",
            supportsAllDrives=True,
            ))
            log_debug(f"Copied file: {copied_file}")
        except HttpError as error:
            log_warning(f"ファイル（{file_name}）をコピーできませんでした。ファイルID: {file_id}, ステータスコード: {error.status_code}")
            log_debug(f"Failed to copy file; filename: {file_name}; file_id: {file_id}; error: {error}")


    def fetch_drive_folder_details_recursive(parent_folder_id, source_folder_id, source_folder_name):
        """
        フォルダ構造を再帰的に取得し、コピーすべきファイルのセットを取得する
        キャンセルされたり失敗した場合は空のセットを返す
        """
        # 強制終了用
        if stop_event.is_set():
            log_warning(f"Cancelled: {source_folder_name}")
            return set()
        
        log_debug(f"Creating a folder: {source_folder_name}")
        
        new_folder_metadata = {
            'name': source_folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]
        }
        
        try:
            new_folder = execute_with_retry(drive_service.files().create(
                body=new_folder_metadata, 
                fields='id',
                supportsAllDrives=True
            ))
            new_folder_id = new_folder.get('id')
        except Exception as e:
            log_error(f"フォルダ作成に失敗しました: {source_folder_name}")
            log_debug(f"詳細: {e}", exc_info=True)
            return set()

        query = f"'{source_folder_id}' in parents and trashed = false"
        items = []
        page_token = None
        
        while True:
            try:
                response = execute_with_retry(drive_service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ))
                items.extend(response.get('files', []))
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
            except HttpError as e:
                log_debug(f"詳細: {e}", exc_info=True)
                items = []
                break

        drive_files_to_copy = set()

        for item in items:
            item_id = item['id']
            item_name = item['name']
            item_mime = item['mimeType']

            if item_mime == 'application/vnd.google-apps.folder':
                # 子フォルダなら再帰呼び出し
                child_files = fetch_drive_folder_details_recursive(new_folder_id, item_id, item_name)
                drive_files_to_copy.update(child_files)
            else:
                log_debug(f"コピー対象追加: {item}")
                # ファイルならコピー対象に含める
                drive_files_to_copy.add((new_folder_id, item_id, item_name))

        return drive_files_to_copy


    courses_to_archive = []

    # コース別の処理
    for course in courses:
        # 強制終了用
        if stop_event.is_set():
            log_warning(f"Cancelled: {course}")
            exit()
        
        log_debug(f"Course: {course}")

        # クラス用フォルダをドライブに作成
        folder_metadata = {
            'name': course["name"],
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [archive_folder_id]
        }
        try:
            course_folder = execute_with_retry(drive_service.files().create(
                body=folder_metadata, 
                fields='id',
                supportsAllDrives=True
            ))
            course_folder_id = course_folder.get('id')
            course_folders[course["id"]] = {
                "folder_id": course_folder_id,
                "folder_name": course["name"]
            }
        except HttpError as e:
            log_error(f"{course["name"]} のドライブフォルダ作成に失敗しました。このクラスをアーカイブ対象から除外します。")
            logger.debug(f"course: {course}; 詳細: {e}", exc_info=True)
            continue

        drive_files_to_copy = set()
        drive_files_to_download = set()
        drive_folders_to_copy = set()
        icons_to_download = set()
        files_to_download_size = 0
        large_drive_files = set() # 100MB以上
        large_drive_files_size = 0
        middle_drive_files = set() # 5MB以上
        middle_drive_files_size = 0


        def fetch_course_data(course_id):
            # API呼び出しの定義をリスト化
            tasks = {
                "announcements": lambda: list_all(lambda **kw: service.courses().announcements().list(courseId=course_id, **kw), "announcements"),
                "course_works": lambda: list_all(lambda **kw: service.courses().courseWork().list(courseId=course_id, **kw), "courseWork"),
                "course_work_materials": lambda: list_all(lambda **kw: service.courses().courseWorkMaterials().list(courseId=course_id, **kw), "courseWorkMaterial"),
                "teachers": lambda: list_all(lambda **kw: service.courses().teachers().list(courseId=course_id, **kw), "teachers"),
                "students": lambda: list_all(lambda **kw: service.courses().students().list(courseId=course_id, **kw), "students"),
                "topics": lambda: list_all(lambda **kw: service.courses().topics().list(courseId=course_id, **kw), "topic"),
                "submissions": lambda: list_all(lambda **kw: service.courses().courseWork().studentSubmissions().list(courseId=course_id, courseWorkId="-", userId="me", **kw), "studentSubmissions")
            }

            results = {}
            # 7本のリクエストを並列実行
            with ThreadPoolExecutor(max_workers=1) as executor: # 適切なスレッド数に調整
                future_to_key = {executor.submit(func): key for key, func in tasks.items()}
                
                for future in tqdm(as_completed(future_to_key), total=len(future_to_key), desc=f"{course["name"]}の情報を取得中(1)"):
                    
                    # futureに対応する元のkeyを取得
                    key = future_to_key[future]
                    
                    try:
                        results[key] = future.result()
                    except Exception as e:
                        log_error(f"{key} の取得に失敗しました。詳細はログに記載しています。")
                        log_debug(e, exc_info=True)
                        results[key] = []  # 失敗時は空リストで埋める
            return results

        data = fetch_course_data(course["id"])
        announcements = data["announcements"]
        course_works = data["course_works"]
        course_work_materials = data["course_work_materials"]
        teachers = data["teachers"]
        students = data["students"]
        topics = data["topics"]
        submissions = data["submissions"]
        
        for user in list(teachers + students):
            if user["userId"] in user_profiles:
                continue
            profile = user["profile"]
            user_profiles[user["userId"]] = profile
            if "photoUrl" in profile:
                path = f"{base_dir}/img/icons/{profile["id"]}.png"
                if os.path.exists(path):
                    log_info(f"Skip (already exists): {Path(path).resolve()};")
                else:
                    icons_to_download.add((f"https:{profile["photoUrl"]}", path))

        # 授業のトピック
        topic_map = {
            topic["topicId"]: topic
            for topic in topics
        }
            
        # 提出物（課題の添付ファイル）
        submission_map = {
            s["courseWorkId"]: s
            for s in submissions
        }

        def clean_drive_file(drive_file):
            file_detail = fetch_drive_file_details(drive_file)
            if file_detail:
                drive_file["title"] = file_detail["file_name"] # 拡張子補完等のため必要
                drive_file["file_type"] = file_detail["file_type"]
                drive_file["save_type"] = file_detail["save_type"]
                drive_file["size"] = file_detail["size"]
                with lock:
                    nonlocal files_to_download_size
                    files_to_download_size += file_detail["size"]
                if drive_file["save_type"] == "copy":
                    drive_files_to_copy.add((course_folder_id, drive_file["id"], drive_file["title"]))
                elif drive_file["save_type"] == "copy (folder)":
                    drive_folders_to_copy.add((course_folder_id, drive_file["id"], drive_file["title"]))
                elif drive_file["save_type"] == "download":
                    drive_files_to_download.add((drive_file["id"], drive_file["title"], drive_file["size"]))
                    if drive_file["size"] > THRESHOLD_100MB:
                        large_drive_files.add((drive_file["id"], drive_file["title"], drive_file["size"]))
                        with lock:
                            nonlocal large_drive_files_size
                            large_drive_files_size += drive_file["size"]
                    if drive_file["size"] > THRESHOLD_5MB:
                        middle_drive_files.add((drive_file["id"], drive_file["title"], drive_file["size"]))
                        with lock:
                            nonlocal middle_drive_files_size
                            middle_drive_files_size += drive_file["size"]
                elif drive_file["save_type"] == "download (skipped)":
                    pass
                else:
                    log_warning(f"Unsupported save type. DriveFile: {drive_file}")

        # CourseWork の個別提出物・返却物の取得
        def get_course_work_attachments(course_work):
            submission = submission_map.get(course_work["id"])
            if not submission:
                return

            assignmentSubmission = submission.get("assignmentSubmission")
            if not assignmentSubmission:
                return

            attachments = assignmentSubmission.get("attachments", [])

            course_work["attachments"] = attachments
            for attachment in attachments:
                if "driveFile" in attachment:
                    # Materialとのズレを修正するため
                    # テンプレートではMaterialと同様に扱う
                    attachment["driveFile"]["driveFile"] = attachment["driveFile"]
                    drive_file = attachment["driveFile"]["driveFile"]
                    if "title" in drive_file:
                        drive_file = attachment["driveFile"]["driveFile"]
                        clean_drive_file(drive_file)

        # 投稿のObjectにフィールドを追加する
        def clean_item(item):
            if item["creatorUserId"] in user_profiles:
                item["creatorUserProfile"] = user_profiles[item["creatorUserId"]]
            else:
                # エラー防止でダミーオブジェクト挿入
                item["creatorUserProfile"] = {
                    "id": item["creatorUserId"],
                    "name": {
                        "givenName": "不明ユーザー",
                        "familyName": "",
                        "fullName": "不明ユーザー"
                    },
                    "photoUrl": None,
                }
            item["creationTime"] = get_jst_str(item["creationTime"])
            item["updateTime"] = get_jst_str(item["updateTime"])
            item["was_updated"] = item["creationTime"] != item["updateTime"]
            if "topicId" in item:
                item["topicName"] = topic_map[item["topicId"]]["name"]

        # 投稿の添付資料の取得
        def get_all_materials(item):
            if "materials" in item:
                for material in item["materials"]:
                    if "driveFile" in material and "title" in material["driveFile"]["driveFile"]:
                        drive_file = material["driveFile"]["driveFile"]
                        clean_drive_file(drive_file)

        def folders_to_files(folders):
            nonlocal drive_files_to_copy
            for folder in folders:
                new_files_to_copy = fetch_drive_folder_details_recursive(folder[0], folder[1], folder[2])
                drive_files_to_copy.update(new_files_to_copy)

        for item in announcements:
            item["item_type"] = "Announcement"
        for item in course_works:
            item["item_type"] = "CourseWork"
        for item in course_work_materials:
            item["item_type"] = "CourseWorkMaterial"


        all_items = announcements + course_works + course_work_materials
        # get_jst_str(item["creationTime"]) で変換する前に実行する必要あり
        all_items.sort(key=lambda item: item['updateTime'], reverse=True)

        try:
            with ThreadPoolExecutor(max_workers=8) as executor:
                # 先に全タスクをsubmitし、futureオブジェクトをリスト化する
                futures = [executor.submit(get_course_work_attachments, item) for item in course_works]
                futures += [executor.submit(get_all_materials, item) for item in all_items]
                futures += [executor.submit(clean_item, item) for item in all_items]
                
                # as_completedで終わったものから取り出し、tqdmでラップする
                results = []
                for future in tqdm(as_completed(futures), total=len(futures), desc=f"{course["name"]}の情報を取得中(2)"):
                    results.append(future.result())

            # フォルダの中身取得
            if len(drive_files_to_copy) > 0:
                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = [executor.submit(folders_to_files, list(drive_folders_to_copy))]
                    results = []
                    for future in tqdm(as_completed(futures), total=len(futures), desc=f"{course["name"]}の情報を取得中(3)"):
                        results.append(future.result())

        except KeyboardInterrupt:
            stop_event.set()

        log_info("\n==============================")
        log_info(f"クラス名: {course["name"]}")
        log_info(f"投稿（お知らせ・課題・資料）の合計数: {len(all_items)}")
        log_info(f"ドライブへコピー対象のファイル数: {len(drive_files_to_copy)}")
        log_info(f"ドライブへコピー対象のフォルダ数: {len(drive_folders_to_copy)}")
        log_info(f"ダウンロード対象ファイルの数: {len(drive_files_to_download)}")
        log_info(f"合計サイズ（ダウンロード対象のみ）: {format_size(files_to_download_size)}")
        log_info("==============================\n")

        if files_to_download_size > THRESHOLD_GB:
            log_warning(f"注意: ダウンロード対象ファイルの合計サイズが1GBを超えています ({format_size(files_to_download_size)})")
            log_info(f"100MBを超えているファイル:")
            for item in large_drive_files:
                log_info(f"- {item[1]}")
            log_info(f"100MBを超えているファイルの合計サイズ: {format_size(large_drive_files_size)}")

            log_info("\nオプションを選んでください:")
            log_info(f"[1] 全てダウンロード ({format_size(files_to_download_size)})")
            log_info(f"[2] 100 MB 以下のファイルのみダウンロード ({format_size(files_to_download_size - large_drive_files_size)})")
            log_info(f"[3] 5 MB 以下のファイルのみダウンロード ({format_size(files_to_download_size - middle_drive_files_size)})")

            def choice_input():
                while True:
                    choice = input("選択 (1/2/3): ").strip()
                    if choice in ["1", "2", "3"]:
                        return choice
                    else:
                        print("1,2,3のいずれかを入力してください。")

            choice = choice_input()

            if choice == '1':
                log_info(f"ダウンロード対象ファイルを全てダウンロードします。")
            elif choice == '2':
                log_info(f"100MB以上のファイルをダウンロード対象から除外します。")
                files_to_download_size -= large_drive_files_size
                drive_files_to_download -= large_drive_files
            elif choice == '3':
                log_info(f"100MB以上のファイルをダウンロード対象から除外します。")
                files_to_download_size -= middle_drive_files_size
                drive_files_to_download -= middle_drive_files
        else:
            log_info("ダウンロード対象ファイルの合計サイズが1GB未満のため、自動的にアーカイブ対象に登録します。")

        courses_to_archive.append(course)
        all_drive_files_to_copy |= drive_files_to_copy
        all_drive_files_to_download |= drive_files_to_download
        all_drive_folders_to_copy |= drive_folders_to_copy
        all_icons_to_download |= icons_to_download
        all_files_to_download_size += files_to_download_size

        html = template.render(
            course=course,
            announcements=announcements,
            course_work=course_works,
            course_work_materials=course_work_materials,
            teachers=teachers,
            students=students,
            students_count=(len(students) + 1),
            all_items=all_items
        )

        with open(f"{base_dir}/クラス_{course["name"]}.html", "w", encoding="utf-8") as f:
            f.write(html)


    log_info("\nアーカイブ対象のクラスが確定しました。")
    log_info("==============================")
    log_info("アーカイブ対象のクラス: ")
    for course in courses_to_archive:
        log_info(f"- {course["name"]}")
    log_info(f"計 {len(courses_to_archive)} クラス")
    log_info(f"ドライブへコピー対象のファイルの合計数: {len(all_drive_files_to_copy)}")
    log_info(f"ダウンロード対象ファイルの合計数: {len(all_drive_files_to_download)}")
    log_info(f"ダウンロード対象ファイルの合計容量: {format_size(all_files_to_download_size)}")
    log_info("==============================")

    confirm = input("アーカイブを開始しますか？ (y/N): ").strip().lower()

    if confirm != "y":
        log_warning("処理を中止しました。")
        sys.exit(1)


    try:
        

        with tqdm(total=all_files_to_download_size, unit='B', unit_scale=True, desc="ファイルをダウンロード中") as pbar:
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(download_file, picture[0], picture[1]) for picture in all_icons_to_download]
                futures += [executor.submit(download_drive_file, file[0], file[1], file[2], pbar=pbar) for file in all_drive_files_to_download]
                
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Error: {e}")

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(copy_drive_file, file[0], file[1], file[2]) for file in all_drive_files_to_copy]
            
            results = []
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"ドライブにファイルをコピー中"):
                results.append(future.result())

    except KeyboardInterrupt:
        stop_event.set()

    log_info(f"\nアーカイブが完了しました。\nアーカイブは {Path(base_dir).resolve()} に出力されています。")
    input("\nEnterキーを押すと終了します...")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_info("\n" + "="*50)
        log_info("予期せぬエラーが発生しました：")
        import traceback
        traceback.print_exc() # 詳細なエラー箇所を表示
        log_debug(f"詳細: {e}", exc_info=True)
        log_info("="*50)
        input("\nEnterキーを押すと終了します...") # これで画面が消えない