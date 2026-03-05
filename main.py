from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from jinja2 import Environment, FileSystemLoader

from googleapiclient.http import MediaIoBaseDownload, HttpError
import io
import requests, os
from datetime import datetime, timezone, timedelta

import shutil


jst_today = datetime.now().astimezone(timezone(timedelta(hours=9)))
jst_today_str = jst_today.strftime('%Y%m%d%H%m%S')

base_dir = f"classroomArchive/archive_{jst_today_str}"

os.makedirs(f"{base_dir}")
os.makedirs(f"{base_dir}/driveFiles")
os.makedirs(f"{base_dir}/icons")
shutil.copy('materials/style.css', f"{base_dir}/css/style.css")
shutil.copy('materials/assignment.svg', f"{base_dir}/img/assignment.svg")
shutil.copy('materials/book.svg', f"{base_dir}/img/book.svg")


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
]

flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
creds = flow.run_local_server(port=0)
service = build("classroom", "v1", credentials=creds)
courses = service.courses().list().execute()

env = Environment(loader=FileSystemLoader("materials"))
template = env.get_template("course.html")

user_profiles = {}

# for course in courses.get("courses", []):
course = courses.get("courses", [])[4]
print(f"コース情報: {course}")


announcements = service.courses().announcements().list(courseId=course["id"]).execute().get("announcements", [])
course_work = service.courses().courseWork().list(courseId=course["id"]).execute().get("courseWork", [])
course_work_materials = service.courses().courseWorkMaterials().list(courseId=course["id"]).execute().get("courseWorkMaterial", [])
teachers = service.courses().teachers().list(courseId=course["id"]).execute().get("teachers", [])
topics = service.courses().topics().list(courseId=course["id"]).execute().get("topic", [])

for teacher in teachers:
    if teacher["userId"] in user_profiles:
        continue
    profile = teacher["profile"]
    user_profiles[teacher["userId"]] = profile
    if "photoUrl" in profile:
        path = f"{base_dir}/icons/{profile["id"]}.png"
        if os.path.exists(path):
            print(f"Skip (already exists): {path}")
        else:
            r = requests.get(f"https:{profile["photoUrl"]}", )
            if r.status_code == 200:
                with open(path, "wb") as f:
                    f.write(r.content)
                print("Saved teacher icon:", profile["id"])
            else:
                print("Failed to save teacher icon:", profile["id"], "status_code:", r.status_code)


def get_jst_str(iso_str):
    utc_dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    jst_timezone = timezone(timedelta(hours=9))
    jst_dt = utc_dt.astimezone(jst_timezone)
    jst_dt_str = f"{jst_dt.year}年{jst_dt.month}月{jst_dt.day}日 {jst_dt.hour}時{jst_dt.minute}分"
    return jst_dt_str


# driveFile download
drive_service = build("drive", "v3", credentials=creds)

def download_drive_file(file_id, filename):
    path = f"{base_dir}/driveFiles/id_{file_id}_name_{filename}"

    if os.path.exists(path):
        print(f"Skip (already exists): {filename}")
        return
    
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        try:
            status, done = downloader.next_chunk()
            print(f"filename: {filename}; file_id: {file_id}; progress: {int(status.progress() * 100)}%; done: {done}")
        except HttpError as error:
            print(f"Failed to download file; filename: {filename}; file_id: {file_id};")
            print(error)

topic_map = {
    topic["topicId"]: topic
    for topic in topics
}

for item in list(announcements + course_work + course_work_materials):
    item["creatorUserProfile"] = user_profiles[item["creatorUserId"]]
    item["creationTime"] = get_jst_str(item["creationTime"])
    item["updateTime"] = get_jst_str(item["updateTime"])
    item["was_updated"] = item["creationTime"] != item["updateTime"]
    if "topicId" in item:
        item["topicName"] = topic_map[item["topicId"]]["name"]

    if "materials" in item:
        for material in item["materials"]:
            if "driveFile" in material and "title" in material["driveFile"]["driveFile"]:
                file_id = material["driveFile"]["driveFile"]["id"]
                file_name = material["driveFile"]["driveFile"]["title"]
                download_drive_file(file_id, file_name)

# 提出物取得
pageToken = None
all_submissions = []

while True:
    result = service.courses().courseWork().studentSubmissions().list(
        courseId=course["id"],
        courseWorkId="-",
        userId="me",
        pageToken=pageToken
    ).execute()

    all_submissions.extend(result.get("studentSubmissions", []))

    pageToken = result.get("nextPageToken")
    if not pageToken:
        break

submission_map = {
    s["courseWorkId"]: s
    for s in all_submissions
}

for item in course_work:
    submission = submission_map.get(item["id"])
    if not submission:
        continue

    assignmentSubmission = submission.get("assignmentSubmission")
    if not assignmentSubmission:
        continue

    attachments = assignmentSubmission.get("attachments", [])

    item["attachments"] = attachments
    for attachment in attachments:
        if "driveFile" in attachment:
            # Materialとのズレを修正するため
            attachment["driveFile"]["driveFile"] = attachment["driveFile"]
            if "title" in attachment["driveFile"]:
                file_id = attachment["driveFile"]["id"]
                file_name = attachment["driveFile"]["title"]
                download_drive_file(file_id, file_name)

for item in announcements:
    item["item_type"] = "Announcement"
for item in course_work:
    item["item_type"] = "CourseWork"
for item in course_work_materials:
    item["item_type"] = "CourseWorkMaterial"

all_items = announcements + course_work + course_work_materials
all_items.sort(key=lambda item: item['updateTime'], reverse=True)

html = template.render(
    name=course["name"],
    section=course.get("section", ""),
    announcements=announcements,
    course_work=course_work,
    course_work_materials=course_work_materials,
    all_items=all_items
)


with open(f"output/クラス_{course["name"]}.html", "w", encoding="utf-8") as f:
    f.write(html)