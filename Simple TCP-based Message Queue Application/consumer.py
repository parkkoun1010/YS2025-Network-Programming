# consumer.py
import socket, time, sys

try:
    host, port, worker_name = sys.argv[1], int(sys.argv[2]), sys.argv[3]
except IndexError:
    sys.exit(f"사용법: python3 {sys.argv[0]} <호스트> <포트> <워커_이름>")

try:
    sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sc.connect((host, port))
    print(f"{host}:{port} 서버에 접속")
except ConnectionRefusedError:
    sys.exit(f"서버에 접속할 수 없음 ({host}:{port})")

sc.sendall(f'{worker_name}\\n'.encode())

buffer = ""

try:
    while True:
        print('[REQUEST]')
        sc.sendall(b'REQUEST\\n')
        
        while '\\n' not in buffer:
            data = sc.recv(1024)
            if not data:
                raise ConnectionAbortedError("서버 연결 종료")
            buffer += data.decode()
        
        response, buffer = buffer.split('\\n', 1)
        response = response.strip()

        if response == 'NOTASK':
            print('NOTASK')
        elif len(response.split()) == 2:
            task_id, duration = response.split()
            print(f'[COMPLETE] {task_id}')
            time.sleep(float(duration))
        else:
            print(f"[예상치 못한 응답 수신: '{response}']")
            pass
        
        time.sleep(1)

except KeyboardInterrupt:
    print("\n[컨슈머 종료]")
except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError) as e:
    print(f"\n[서버와의 연결이 끊김 - ({e})]")
finally:
    sc.close()
