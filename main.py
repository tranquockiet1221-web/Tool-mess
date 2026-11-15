from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import cyrus_core as main
import os
import uuid

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
active_tools = {} 

# Mật khẩu Admin
ADMIN_PASSWORD = "hotwarquockiet" 

@app.route('/')
def index():
    return send_file('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    if data.get('password', '') == ADMIN_PASSWORD:
        return jsonify({"success": True, "msg": "Đăng nhập thành công!"})
    return jsonify({"success": False, "msg": "Sai mật khẩu!"})

@app.route('/api/get_box', methods=['POST'])
def get_box():
    data = request.json
    raw_cookies = data.get('cookie', '').strip()
    if not raw_cookies:
        return jsonify({"success": False, "msg": "Thiếu Cookie!"})
    
    first_cookie = raw_cookies.split('\n')[0].strip()
    
    threads = main.get_thread_list(first_cookie)
    if threads is None:
        return jsonify({"success": False, "msg": "Cookie die hoặc Facebook chặn IP!"})
    if not threads:
        return jsonify({"success": False, "msg": "Không tìm thấy Box!"})
    return jsonify({"success": True, "threads": threads})

@app.route('/api/start', methods=['POST'])
def start():
    data = request.json
    raw_cookies = data.get('cookie')
    box_id = data.get('box_id')
    message = data.get('message', 'Hello')
    delay = int(data.get('delay', 30))

    if not raw_cookies or not box_id:
        return jsonify({"success": False, "msg": "Thiếu Cookie hoặc ID Box"})

    cookie_list = [c.strip() for c in raw_cookies.split('\n') if c.strip()]
    task_id = str(uuid.uuid4())[:8]
    
    tool = main.SpamManager(cookie_list, box_id, message, delay)
    if tool.start():
        active_tools[task_id] = tool
        return jsonify({"success": True, "msg": f"Đã chạy ngầm {len(cookie_list)} acc!", "task_id": task_id})
    return jsonify({"success": False, "msg": "Không có Cookie nào hợp lệ!"})

@app.route('/api/stop', methods=['POST'])
def stop():
    data = request.json
    task_id = data.get('task_id')
    if task_id in active_tools:
        active_tools[task_id].stop()
        del active_tools[task_id]
        return jsonify({"success": True, "msg": "Đã dừng tất cả acc trong tiến trình này."})
    return jsonify({"success": False, "msg": "Không tìm thấy tiến trình."})

if __name__ == '__main__':
    # Uppcoder thường cấp cổng qua biến PORT, mặc định 8000
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port, threaded=True)