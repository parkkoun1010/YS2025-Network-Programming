import socket
import sys
import threading
import signal

# 전역 설정 및 상수
BUFFER_SIZE = 8192  # 8KB 버퍼
FILTER_IMAGE = False  # 이미지 필터링 플래그
REQ_COUNT = 0  # 요청 번호 카운터

# 제거해야 할 Hop-by-hop 헤더 목록
HOP_BY_HOP_HEADERS = [
    'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
    'te', 'trailers', 'transfer-encoding', 'upgrade', 'proxy-connection'
]

# 시스템 함수
def signal_handler(sig, frame):
    print("\n\n[System] Proxy Server is shutting down...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def get_request_id():
    global REQ_COUNT
    REQ_COUNT += 1
    return REQ_COUNT

def toggle_image_filter(enable):
    global FILTER_IMAGE
    FILTER_IMAGE = enable

# 핵심 로직
def parse_host_port(url, header_lines):
    http_pos = url.find("://")
    temp_url = url
    if http_pos != -1:
        temp_url = url[(http_pos + 3):]
    port_pos = temp_url.find(":")
    path_pos = temp_url.find("/")
    if path_pos == -1:
        path_pos = len(temp_url)
    
    host = ""
    port = 80
    
    # 포트가 명시된 경우
    if port_pos != -1 and path_pos > port_pos:
        port = int(temp_url[(port_pos+1):path_pos])
        host = temp_url[:port_pos]
    else:
        host = temp_url[:path_pos]
        
    return host, port

def handle_client(client_socket, client_addr):
    global FILTER_IMAGE

    try:
        request_data = client_socket.recv(BUFFER_SIZE)
        if not request_data:
            client_socket.close()
            return

        request_str = request_data.decode('utf-8', errors='ignore')
        lines = request_str.split('\r\n')
        first_line = lines[0]

        if first_line.startswith('CONNECT'):
            client_socket.close()
            return

    except Exception:
        client_socket.close()
        return

    req_id = get_request_id()

    try:
        try:
            method, url, version = first_line.split()
        except ValueError:
            client_socket.close()
            return

        if '?image_on' in url:
            toggle_image_filter(False)
        elif '?image_off' in url:
            toggle_image_filter(True)

        is_redirected = False
        target_host = ""
        target_port = 80

        if 'google' in url:
            temp_url_check = url.split('://')[1] if '://' in url else url
            domain_part = temp_url_check.split('/')[0]
            
            if 'google' in domain_part:
                is_redirected = True
                target_host = "mnet.yonsei.ac.kr"
                target_port = 80
                
                path = '/'
                if '/' in temp_url_check:
                    parts = temp_url_check.split('/', 1)
                    if len(parts) > 1:
                        path = "/" + parts[1]
                
                new_first_line = f"{method} http://{target_host}{path} {version}"
            else:
                target_host, target_port = parse_host_port(url, lines)
                new_first_line = first_line
        else:
            target_host, target_port = parse_host_port(url, lines)
            new_first_line = first_line


        filter_mark = "[O]" if FILTER_IMAGE else "[X]"
        img_log = "[O]" if FILTER_IMAGE else "[X]"
        red_log = "[O]" if is_redirected else "[X]"

        user_agent = "Unknown"
        for line in lines:
            if line.lower().startswith('user-agent:'):
                user_agent = line
                break
        
        log_msg = (
            f"{req_id} {red_log} Redirected {img_log} Image filter\n"
            f"[CLI connected to {client_addr[0]}:{client_addr[1]}]\n"
            f"[CLI ==> PRX --- SRV]\n"
            f"  > {first_line}\n"
            f"  > {user_agent}"
        )
        print(log_msg)

        server_headers = []
        server_headers.append(new_first_line)
        for line in lines[1:]:
            if not line:
                continue
            key = line.split(':')[0].lower()
            if key not in HOP_BY_HOP_HEADERS:
                if is_redirected and key == 'host':
                    server_headers.append(f"Host: {target_host}")
                else:
                    server_headers.append(line)
        server_headers.append("Connection: close")
        server_headers.append("\r\n")
        server_request_data = "\r\n".join(server_headers).encode('utf-8')

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(10)

        try:
            server_socket.connect((target_host, target_port))
            srv_log_msg = (
                f"[SRV connected to {target_host}:{target_port}]\n"
                f"[CLI --- PRX ==> SRV]\n"
                f"  > {new_first_line}\n"
                f"  > {user_agent}"
            )
            print(srv_log_msg)

            server_socket.sendall(server_request_data)

            print("[CLI <== PRX --- SRV]")

            response_buffer = b""
            while True:
                chunk = server_socket.recv(1)
                if not chunk:
                    break
                response_buffer += chunk
                if b"\r\n\r\n" in response_buffer:
                    break

            header_part = response_buffer.split(b"\r\n\r\n")[0].decode('utf-8', errors='ignore')
            headers = header_part.split('\r\n')
            status_line = headers[0]

            content_type = ""
            content_length = "0"
            for h in headers:
                lower_h = h.lower()
                if lower_h.startswith('content-type:'):
                    content_type = h.split(':', 1)[1].strip()
                elif lower_h.startswith('content-length:'):
                    content_length = h.split(':', 1)[1].strip()

            print(f"  > {status_line}")
            print(f"  > {content_type} {content_length}bytes")

            if FILTER_IMAGE and 'image/' in content_type:
                error_msg = (
                    "HTTP/1.1 404 Not Found\r\n"
                    "Content-Type: text/plain\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                    "Image Filtered"
                )
                client_socket.sendall(error_msg.encode('utf-8'))
                print("  > [Blocked] Image content dropped.")
            else:
                client_socket.sendall(response_buffer)
                while True:
                    data = server_socket.recv(BUFFER_SIZE)
                    if not data:
                        break
                    client_socket.sendall(data)

        except Exception as e:
            print(f"[Server Error] {e}")
        finally:
            server_socket.close()
            print("[SRV disconnected]")

    except Exception as e:
        print(f"[Client Error] {e}")
    finally:
        client_socket.close()
        print("[CLI disconnected]")
        print("-" * 60)



# 메인 실행부
def start_server(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(20)
        print(f"Starting proxy server on port {port}")
        print("-" * 60)
        
        while True:
            client_sock, client_addr = server_socket.accept()
            t = threading.Thread(target=handle_client, args=(client_sock, client_addr))
            t.daemon = True
            t.start()
            
    except Exception as e:
        print(e)
    finally:
        server_socket.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 prx.py [port]")
        sys.exit(1)
    start_server(int(sys.argv[1]))
