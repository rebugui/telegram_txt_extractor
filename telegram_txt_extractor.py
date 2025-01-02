import os
import re
import csv
import time
import pytz
import zipfile
import rarfile
from datetime import datetime, timedelta
from telethon.sync import TelegramClient

# Telegram API 정보
api_id = 'API ID'         # API ID 입력
api_hash = 'API Hash'     # API Hash 입력
phone_number = '전화번호'  # 전화번호 입력

# 로컬 시간대 설정
local_tz = pytz.timezone("Asia/Seoul")

# 파일 저장 경로
base_download_path = './downloads'

def format_file_name(channel_name, message):
    """파일명을 '채널명_파일명_업로드시간.확장자' 형식으로 생성"""
    try:
        # 파일 이름 확인
        if not message.file or not message.file.name:
            #print(f"파일 이름이 없습니다. 메시지 ID: {message.id}, 건너뜁니다.")
            return None  # 파일 이름이 없으면 None 반환
        
        # 메시지 작성 시간 (UTC -> 로컬 시간 변환)
        local_date = message.date.astimezone(local_tz)
        upload_time = local_date.strftime("%Y%m%d_%H%M%S")  # YYYYMMDD_HHMMSS 형식

        # 파일 이름 및 확장자 설정
        file_name = message.file.name
        file_ext = os.path.splitext(file_name)[1] or ".txt"  # 확장자가 없는 경우 기본 확장자
        formatted_name = f"{upload_time}_{os.path.splitext(file_name)[0]}{file_ext}"
        return formatted_name
    except Exception as e:
        print(f"파일명 포맷팅 오류: {e}")
        return None  # 오류 발생 시 None 반환

def extract_zip(file_path, extract_path):
    """ZIP 파일 압축 해제"""
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.testzip()  # 압축 파일이 정상적인지 검사
            zip_ref.extractall(extract_path)  # 압축 해제
        return True
    except zipfile.BadZipFile:
        print(f"ZIP 파일이 깨졌습니다: {file_path}")
    except RuntimeError:  # 비밀번호가 걸려있을 때 발생하는 오류
        print(f"ZIP 파일에 비밀번호가 걸려있습니다: {file_path}")
    return False

def extract_rar(file_path, extract_path):
    """RAR 파일 압축 해제"""
    try:
        with rarfile.RarFile(file_path) as rar_ref:
            rar_ref.test()  # 압축 파일이 정상적인지 검사
            rar_ref.extractall(extract_path)  # 압축 해제
        return True
    except rarfile.BadRarFile:
        print(f"RAR 파일이 깨졌습니다: {file_path}")
    except rarfile.PasswordRequired:
        print(f"RAR 파일에 비밀번호가 걸려있습니다: {file_path}")
    return False

ALLOWED_EXTENSIONS = {".txt", ".zip", ".rar"}  # 허용된 파일 확장자 목록

def download_files_from_channel(client, channel):
    """특정 채널의 파일 다운로드 및 텍스트 파일 처리"""
    print("")
    print("===============================================")
    print(f"채널 다운로드 시작: {channel.title}")
    print("===============================================")

    messages = client.iter_messages(channel)

    # 채널별 디렉토리 생성
    channel_path = os.path.join(base_download_path, re.sub(r'[\\/:*?"<>|]', '_', channel.title))
    os.makedirs(channel_path, exist_ok=True)

    try:
        for message in messages:
            if message.file:  # 파일이 있는 메시지만 처리
                try:
                    # 메시지 작성 시간 (로컬 시간 변환)
                    local_date = message.date.astimezone(local_tz)
                    formatted_time = local_date.strftime("%Y-%m-%d %H:%M:%S")

                    # 파일 작성 시간 (datetime 객체로 변환)
                    file_time = local_date

                    # 현재 시간 구하기 (로컬 시간대 적용)
                    now = datetime.now(local_tz)

                    # 24시간 이상 차이가 나면 처리하지 않음
                    if now - file_time > timedelta(hours=24):
                        #print(f"파일 작성 시간이 24시간 전입니다. 파일 {message.id}는 건너뜁니다.")
                        continue

                    # 파일명 생성
                    file_name = format_file_name(channel.title, message)
                    if not file_name:
                        continue

                    file_path = os.path.join(channel_path, file_name)

                    # 파일이 이미 존재하면 다운로드는 건너뛰고 처리로 바로 이동
                    if os.path.exists(file_path):
                        print(f"파일이 이미 존재합니다: {file_name} (다운로드 건너뜀)")
                    else:
                        # 파일 다운로드
                        print(f"다운로드 중: {file_name}")
                        message.download_media(file=file_path)

                        # 압축 파일 처리
                        if file_name.endswith('.zip'):
                            if not extract_zip(file_path, channel_path):
                                continue  # 압축 해제 실패 시 건너뜀
                        elif file_name.endswith('.rar'):
                            if not extract_rar(file_path, channel_path):
                                continue  # 압축 해제 실패 시 건너뜀

                    # 텍스트 파일 처리 호출
                    print(f"텍스트 파일 처리 중: {file_name}")
                    process_text_files(channel_path, "output.csv", channel.title, formatted_time)

                except Exception as e:
                    print(f"파일 다운로드 또는 처리 오류: {e}")
    except Exception as e:
        print(f"채널 메시지 순회 오류: {channel.title} - {e}")



def process_text_files(folder_path, output_csv, channel_name, formatted_time):
    """폴더 내 텍스트 파일을 처리하여 CSV로 저장 (채널 이름 포함)"""
    # 필터링할 도메인 목록
    filter_domains = ["NAVER.COM", "NAVER.COM", "NAVER.COM"]

    # 기존 CSV 데이터 로드 (중복 방지용 데이터 로드)
    existing_data = set()
    if os.path.exists(output_csv):
        print(f"기존 CSV 파일 로드: {output_csv}")
        with open(output_csv, "r", encoding="utf-8") as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader, None)  # 헤더 스킵
            for row in csv_reader:
                if len(row) >= 5:  # Line1, Line2, Line3 존재 확인
                    existing_data.add(tuple(row[2:5]))

    # 결과 저장용 딕셔너리
    results = {}

    # 폴더 내 파일 처리
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith(".txt"):
                file_path = os.path.join(root, file_name)
                
                try:
                    with open(file_path, "r", encoding="utf-8") as file:
                        for line in file:
                            line = line.strip()
                            if any(domain in line for domain in filter_domains):
                                line_parts = line.replace("https://", "").replace("http://", "").split(":")
                                
                                # 기본값 설정 (빈칸)
                                line1 = line_parts[0] if len(line_parts) > 0 else ""
                                line2 = line_parts[1] if len(line_parts) > 1 else ""
                                line3 = line_parts[2] if len(line_parts) > 2 else ""

                                # 중복 확인
                                if (line1, line2, line3) not in existing_data:
                                    existing_data.add((line1, line2, line3))
                                    results[(line1, line2, line3, file_name, channel_name)] = {
                                        "channel_name": channel_name,
                                        "file_name": file_name,
                                        "line_parts": (line1, line2, line3),
                                        "formatted_time": formatted_time,
                                    }
                except Exception as e:
                    print(f"파일 처리 오류: {file_path} - {e}")

    # CSV 파일 생성 (없을 경우 새로 생성)
    if not os.path.exists(output_csv):
        try:
            with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
                csv_writer = csv.writer(csvfile)
                header = ["Channel Name", "File Name", "Line1", "Line2", "Line3", "Creation Time"]
                csv_writer.writerow(header)
                print(f"CSV 파일 생성 및 헤더 추가: {output_csv}")
        except Exception as e:
            print(f"CSV 파일 생성 오류: {e}")
            return  # 파일 생성에 실패한 경우 추가 작업 중단

    # CSV로 저장 (추가 모드)
    if results:
        try:
            with open(output_csv, "a", newline="", encoding="utf-8") as csvfile:
                csv_writer = csv.writer(csvfile)
                for (line1, line2, line3, file_name, channel_name), data in results.items():
                    row = [
                        data["channel_name"],
                        data["file_name"],
                        line1, line2, line3,
                        data["formatted_time"]
                    ]
                    csv_writer.writerow(row)
            print(f"CSV 생성 및 데이터 추가 완료: {output_csv}")
        except Exception as e:
            print(f"CSV 저장 오류: {e}")
    else:
        print("저장할 데이터가 없습니다. CSV를 생성하지 않습니다.")

def main():
    with TelegramClient('session_name', api_id, api_hash) as client:
        dialogs = client.get_dialogs()
        channels = [dialog.entity for dialog in dialogs if dialog.is_channel]

        print(f"가입한 채널 수: {len(channels)}")

        for channel in channels:
            try:
                download_files_from_channel(client, channel)
            except Exception as e:
                print(f"채널 다운로드 중 오류 발생: {channel.title} - {e}")

        time.sleep(60)

if __name__ == "__main__":
    main()
