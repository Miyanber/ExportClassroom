from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaIoBaseDownload, HttpError
from googleapiclient.discovery import build
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import io, requests, os
import signal
import shutil
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed

signal.signal(signal.SIGINT, lambda sig, frame: stop_event.set())


jst_today = datetime.now().astimezone(timezone(timedelta(hours=9)))
jst_today_str = jst_today.strftime("%Y%m%d%H%M%S")

base_dir = f"classroomArchive/archive_{jst_today_str}"
print(f"保存先: {base_dir}")

os.makedirs(f"{base_dir}", exist_ok=True)
os.makedirs(f"{base_dir}/driveFiles", exist_ok=True)
os.makedirs(f"{base_dir}/css", exist_ok=True)
os.makedirs(f"{base_dir}/img", exist_ok=True)
os.makedirs(f"{base_dir}/img/icons", exist_ok=True)
shutil.copy('materials/style.css', f"{base_dir}/css/style.css")
shutil.copy('materials/assignment.svg', f"{base_dir}/img/assignment.svg")
shutil.copy('materials/book.svg', f"{base_dir}/img/book.svg")
shutil.copy('materials/user.svg', f"{base_dir}/img/user.svg")

SCOPES = [
    "https://www.googleapis.com/auth/classroom.courses.readonly",
    "https://www.googleapis.com/auth/classroom.announcements.readonly",
    "https://www.googleapis.com/auth/classroom.coursework.me",
    "https://www.googleapis.com/auth/classroom.courseworkmaterials.readonly",
    "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
    "https://www.googleapis.com/auth/classroom.rosters.readonly",
    "https://www.googleapis.com/auth/classroom.profile.photos",
    "https://www.googleapis.com/auth/classroom.addons.student",
    "https://www.googleapis.com/auth/classroom.topics.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.file",
]

flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
creds = flow.run_local_server(port=0)
service = build("classroom", "v1", credentials=creds)

archive_folder_id = None

try:
    file_name = "archive_folder_id.txt"
    drive_service = build("drive", "v3", credentials=creds)

    folder_name = "Classroom Archive"

    query = (
        f"name = '{folder_name}' "
        f"and 'root' in parents "
        f"and mimeType = 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )

    results = drive_service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)', # 必要なフィールドだけ取得
        pageSize=10
    ).execute()
    
    items = results.get('files', [])
    root_folder_id = None

    if not items:
        print(f"フォルダ '{folder_name}' は見つかりませんでした。")
        file_metadata = {
            "name": "Classroom Archive",
            "mimeType": "application/vnd.google-apps.folder",
        }
        file = drive_service.files().create(body=file_metadata, fields="id").execute()
        root_folder_id = file.get("id")
    else:
        # 複数ヒットする可能性があるため、最初の一つを返す
        folder = items[0]
        root_folder_id = folder["id"]

    # 個別フォルダ作成
    file_metadata = {
        "name": jst_today_str,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [root_folder_id],
    }
    file = drive_service.files().create(body=file_metadata, fields="id").execute()
    archive_folder_id = file.get("id")
    

except HttpError as error:
    print(f"An error occurred: {error}")
    exit(0)

env = Environment(loader=FileSystemLoader("materials"))
template = env.get_template("course.html")

def list_all(method, key):
    items = []
    page_token = None
    
    while True:
        result = method(pageToken=page_token).execute()
        items.extend(result.get(key, []))
        page_token = result.get("nextPageToken")

        if not page_token:
            break

    return items

courses = list_all(
    lambda **kwargs: service.courses().list(**kwargs),
    "courses"
)

import threading
thread_local = threading.local()
import re

stop_event = threading.Event()

user_profiles = {}
pictures_to_download = set()


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
    elif mime_type == "application/pdf":
        return "PDF"

# (dict | None) を返す。
# filename は sanitize 後のものを渡すこと！
def download_drive_file(file_id, file_name):
    # 強制終了用
    if stop_event.is_set():
        print(f"Cancelled: {file_name}")
        return None

    if not hasattr(thread_local, "drive_service"):
        thread_local.drive_service = build("drive", "v3", credentials=creds)

    drive_service = thread_local.drive_service

    # 利用不可の文字を消す
    file_name = re.sub(r'[\\/*?:"<>|]', "_", file_name)
    file_name = file_name.replace("\n", " ")
    file_name = file_name.rstrip(" .")

    file_type = None
    path = f"{base_dir}/driveFiles/id_{file_id}_name_{file_name}"

    if os.path.exists(path):
        print(f"Skip (already exists): {file_name}")
        mime_type = mimetypes.guess_file_type(file_name)[0]
        if mime_type:
            extension = mimetypes.guess_extension(mime_type)
            file_type = extension.upper()[1:]
        return {
            "file_name": file_name,
            "file_type": file_type,
            "was_saved": True
        }
    
    try:
        file = drive_service.files().get(
            fileId=file_id,
            fields="name,mimeType,size"
        ).execute()

        file_name = file["name"]
        mime_type = file["mimeType"]

        extension = mimetypes.guess_extension(mime_type)
        if extension:
            file_type = extension.upper()[1:]

        # 拡張子が必要な場合は付与
        if "." not in file_name and not mime_type.startswith("application/vnd.google-apps"):
            if extension:
                file_name += extension
        
        # Google ファイル以外はダウンロード試行
        if not mime_type.startswith("application/vnd.google-apps"):
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.FileIO(path, "wb")
            downloader = MediaIoBaseDownload(fh, request)

            done = False
            while not done:
                try:
                    status, done = downloader.next_chunk()
                    print(f"filename: {file_name}; file_id: {file_id}; progress: {int(status.progress() * 100)}%; done: {done}")
                    return {
                        "file_name": file_name,
                        "file_type": file_type,
                        "was_saved": True
                    }
                except HttpError as error:
                    if error.status_code == 404:
                        print(f"Failed to download file; filename: {file_name}; file_id: {file_id};")
                        print(error)
                        return None
        
        # 404以外で失敗 or Google系ファイルの場合
        file = drive_service.files().copy(
            fileId=file_id,
            body={
                "name": file_name,
                "parents": [archive_folder_id] # Apps Script (.gs) は親フォルダ指定無視でドライブ直下に保存される
            },
            fields="id,name,webViewLink,mimeType"
        ).execute()
        print(f"ダウンロードできなかったため、ドライブにコピーが作成されました。ファイル名: {file["name"]}, リンク: {file['webViewLink']}")

        return {
            "file_name": file_name,
            "file_type": file_type,
            "was_saved": True,
            "web_view_link": file["webViewLink"],
        }

    except HttpError as e:
        print(f"Failed to download file; filename: {file_name}; file_id: {file_id};")
        print(e)
    return None


def download_file(url, path):
    r = requests.get(url, timeout=10)
    if r.status_code == 200:
        with open(path, "wb") as f:
            f.write(r.content)
        print(f"Saved {path}.png")
    else:
        print(f"Failed to save {path}.png; status_code: {r.status_code};")


# コース別の処理
for course in courses:
    print(course)
    print(f"クラス名: {course["name"]}")
    if course["name"] != "赤団(高１～３)":
        continue

    announcements = list_all(
        lambda **kwargs: service.courses().announcements().list(courseId=course["id"], **kwargs),
        "announcements"
    )
    course_works = list_all(
        lambda **kwargs: service.courses().courseWork().list(courseId=course["id"], **kwargs),
        "courseWork"
    )
    course_work_materials = list_all(
        lambda **kwargs: service.courses().courseWorkMaterials().list(courseId=course["id"], **kwargs),
        "courseWorkMaterial"
    )
    teachers = list_all(
        lambda **kwargs: service.courses().teachers().list(courseId=course["id"], **kwargs),
        "teachers"
    )
    students = list_all(
        lambda **kwargs: service.courses().students().list(courseId=course["id"], **kwargs),
        "students"
    )
    topics = list_all(
        lambda **kwargs: service.courses().topics().list(courseId=course["id"], **kwargs),
        "topic"
    )
    submissions = list_all(
        lambda **kwargs: service.courses().courseWork().studentSubmissions().list(
            courseId=course["id"],
            courseWorkId="-",
            userId="me",
            **kwargs
        ),
        "studentSubmissions"
    )

    for user in list(teachers + students):
        if user["userId"] in user_profiles:
            continue
        profile = user["profile"]
        user_profiles[user["userId"]] = profile
        if "photoUrl" in profile:
            path = f"{base_dir}/img/icons/{profile["id"]}.png"
            if os.path.exists(path):
                print(f"Skip (already exists): {path}.png;")
            else:
                pictures_to_download.add((f"https:{profile["photoUrl"]}", path))

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
                    file_dict = download_drive_file(drive_file["id"], drive_file["title"])
                    drive_file["title"] = file_dict["file_name"] # 拡張子補完等のため必要
                    drive_file["file_type"] = file_dict["file_type"]
                    drive_file["was_saved"] = file_dict["was_saved"]
                    if "web_view_link" in file_dict:
                        drive_file["web_view_link"] = file_dict["web_view_link"]


    def get_all_materials(item):
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

        if "materials" in item:
            for material in item["materials"]:
                if "driveFile" in material and "title" in material["driveFile"]["driveFile"]:
                    drive_file = material["driveFile"]["driveFile"]
                    file_dict = download_drive_file(drive_file["id"], drive_file["title"])
                    drive_file["title"] = file_dict["file_name"] # 拡張子補完等のため必要
                    drive_file["file_type"] = file_dict["file_type"]
                    drive_file["was_saved"] = file_dict["was_saved"]
                    if "web_view_link" in file_dict:
                        drive_file["web_view_link"] = file_dict["web_view_link"]


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
            futures = [executor.submit(download_file, picture[0], picture[1]) for picture in pictures_to_download]
            futures += [executor.submit(get_course_work_attachments, item) for item in course_works]
            futures += [executor.submit(get_all_materials, item) for item in all_items]
            
            # as_completedで終わったものから取り出し、tqdmでラップする
            results = []
            for future in tqdm(as_completed(futures), total=len(futures), desc="ファイルを保存中"):
                results.append(future.result())

    except KeyboardInterrupt:
        stop_event.set()

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

print(f"完了しました。アーカイブは {base_dir} に出力されています。")