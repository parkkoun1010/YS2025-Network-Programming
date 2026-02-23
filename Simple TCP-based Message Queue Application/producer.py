import socket, time, sys

# 1. 커맨드 라인 인자 파싱
try:
    host = sys.argv[1]
    port = int(sys.argv[2])
    task_file = sys.argv[3]
except IndexError:
    print(f"사용법: python3 {sys.argv[0]} <호스트> <포트> <태스크_파일>")
    sys.exit(1)

# 2. 소켓 생성 및 서버 연결
try:
    sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sc.connect((host, port))
    print(f"{host}:{port} 서버에 접속함")
except ConnectionRefusedError:
    print(f"서버에 접속할 수 없음 ({host}:{port})")
    sys.exit(1)


# 3. 태스크 파일 읽기
tasks = []
try:
    with open(task_file, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 4:
                timestamp, priority, task_id, duration = parts
                tasks.append((float(timestamp), priority, task_id, duration))
except FileNotFoundError:
    print(f"'{task_file}' 파일을 찾을 수 없습니다.")
    sc.close()
    sys.exit(1)

# 4. 태스크 전송 루프
start_time = time.time()

for timestamp, priority, task_id, duration in tasks:
    while time.time() - start_time < timestamp:
        time.sleep(0.01)
    message = f'CREATE {priority} {task_id} {duration}\\n'
    sc.sendall(message.encode())
    print(f'[CREATE] {priority} {task_id} {duration}')

# 5. 종료
time.sleep(1) # 서버가 마지막 메시지를 처리할 시간을 줍니다.
sc.close()
print('모든 작업 전송, 클라이언트 종료')

