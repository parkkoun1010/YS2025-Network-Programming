import socket
import sys
from threading import Thread, Lock
import heapq
import time

# 전역 변수: 모든 스레드가 공유하는 데이터
task_queue = []
queue_lock = Lock()
consumers = {} # {conn: worker_name}
is_running = True
# 종료 시 모든 소켓을 한 번에 닫기 위해 리스트로 관리
all_sockets = []

# 1. Producer 요청 처리 함수 (producer_worker)
def producer_worker(conn, addr):
    global is_running
    print(f"[Producer] {addr}에서 연결되었습니다.")
    buffer = ""
    try:
        while is_running:
            data = conn.recv(1024).decode()
            if not data: break
            buffer += data
            while '\\n' in buffer:
                message, buffer = buffer.split('\\n', 1)
                parts = message.strip().split()
                if len(parts) == 4 and parts[0] == "CREATE":
                    priority, task_id, duration = int(parts[1]), parts[2], float(parts[3])
                    with queue_lock:
                        heapq.heappush(task_queue, (priority, task_id, duration))
                    print(f"[CREATE] {priority} {task_id} {duration}")
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        print(f"[Producer] {addr} 연결 종료")
        conn.close()

# 2. Consumer 요청 처리 함수 (consumer_worker)
def consumer_worker(conn, addr):
    global is_running, consumers
    print(f"[Consumer] {addr}에서 연결되었습니다.")
    worker_name = ""
    registered = False
    buffer = ""
    try:
        while is_running:
            data = conn.recv(1024).decode()
            if not data: break
            buffer += data
            while '\\n' in buffer:
                message, buffer = buffer.split('\\n', 1)
                message = message.strip()
                if not message: continue

                if not registered:
                    worker_name = message
                    consumers[conn] = worker_name
                    print(f"[{worker_name} connected]")
                    print(f"[{len(consumers)} consumers online]")
                    registered = True
                elif message == "REQUEST":
                    with queue_lock:
                        if task_queue:
                            priority, task_id, duration = heapq.heappop(task_queue)
                            response = f"{task_id} {duration}\\n"
                            conn.sendall(response.encode())
                            print(f"[ASSIGN] {task_id} - {worker_name}")
                        else:
                            conn.sendall(b"NOTASK\\n")
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        print(f"[Consumer] {addr} 연결 종료")
        if conn in consumers:
            print(f"[{consumers.pop(conn)} disconnected]")
            print(f"[{len(consumers)} consumers online]")
        conn.close()

# 3. 소켓 설정 함수 (setup_sockets)
def setup_sockets(host, p_port, c_port):
    producer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    producer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    producer_socket.bind((host, p_port))
    producer_socket.listen()
    print(f"[Producer] 서버가 {host}:{p_port}에서 연결을 기다리는 중...")

    consumer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    consumer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    consumer_socket.bind((host, c_port))
    consumer_socket.listen()
    print(f"[Consumer] 서버가 {host}:{c_port}에서 연결을 기다리는 중...")
    
    all_sockets.extend([producer_socket, consumer_socket])
    return producer_socket, consumer_socket

# 4. 종료 처리 함수 (shutdown)
def shutdown():
    global is_running
    print("\n서버 종료 절차를 시작합니다...")
    is_running = False
    time.sleep(1) 
    for sock in all_sockets:
        try:
            sock.close()
        except Exception:
            pass
    print("모든 소켓을 닫고 서버를 완전히 종료합니다.")

# 5. 메인 실행 블록
if __name__ == '__main__':
    if len(sys.argv) != 4:
        sys.exit("사용법: python3 server.py <IP> <Producer 포트> <Consumer 포트>")

    host, p_port, c_port = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    
    producer_socket, consumer_socket = setup_sockets(host, p_port, c_port)
    
    def accept_loop(sock, worker_func):
        while is_running:
            try:
                sock.settimeout(1.0)
                conn, addr = sock.accept()
                all_sockets.append(conn)
                thread = Thread(target=worker_func, args=(conn, addr))
                thread.daemon = True
                thread.start()
            except socket.timeout:
                continue
            except Exception:
                if is_running: print("Accept Loop 종료")
                break
    
    p_thread = Thread(target=accept_loop, args=(producer_socket, producer_worker))
    c_thread = Thread(target=accept_loop, args=(consumer_socket, consumer_worker))
    p_thread.daemon = True
    c_thread.daemon = True
    p_thread.start()
    c_thread.start()
    
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown()
