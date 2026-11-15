from utils import (
    parse_cookie_string,
    generate_offline_threading_id,
    generate_session_id,
    generate_client_id,
    json_minimal,
    formAll,
    mainRequests,
    dataGetHome,
)
import json
import random
import paho.mqtt.client as mqtt
from urllib.parse import urlparse
import ssl
import requests
import time
import threading
from datetime import datetime


class FacebookMQTTSender:
    def __init__(self, cookies: str, account_name: str = ""):
        self.cookies = cookies
        self.account_name = account_name
        self.dataFB = dataGetHome(self.cookies)
        self.user_id = self._extract_user_id()
        self.mqtt_client = None
        self.connected = False
        self.ws_req_number = 0
        self.req_lock = threading.Lock()
        self.last_seq_id = None
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"

    def _extract_user_id(self):
        cookie_dict = parse_cookie_string(self.cookies)
        user_id = cookie_dict.get("c_user")
        if not user_id:
            raise ValueError("Invalid cookies: c_user not found")
        return user_id

    def _get_seq_id(self):
        form = formAll(self.dataFB, "CometChatInboxQuery", "3336396659757871")
        form["queries"] = json_minimal({
            "o0": {
                "doc_id": "3336396659757871",
                "query_params": {
                    "limit": 1, "before": None, "tags": ["INBOX"],
                    "includeDeliveryReceipts": False, "includeSeqID": True,
                }
            }
        })
        req_params = mainRequests("https://www.facebook.com/api/graphqlbatch/", form, self.cookies)
        req_params["headers"]["x-fb-friendly-name"] = "CometChatInboxQuery"
        res = requests.post(**req_params)
        text = res.text
        if text.startswith("for(;;);"):
            text = text[9:]
        first = text.split("\n")[0]
        if not first.strip() or first.startswith("<"):
            raise Exception("Empty HTML response")
        data = json.loads(first)
        self.last_seq_id = data["o0"]["data"]["viewer"]["message_threads"]["sync_sequence_id"]

    def connect(self):
        if self.connected:
            return
        self._get_seq_id()
        session_id = generate_session_id()
        user = {
            "a": self.user_agent, "u": self.user_id, "s": session_id, "chat_on": True,
            "fg": False, "d": generate_client_id(), "ct": "websocket", "aid": "219994525426954",
            "mqtt_sid": "", "cp": 3, "ecp": 10, "st": [], "pm": [], "dc": "",
            "no_auto_fg": True, "gas": None, "pack": [],
        }
        host = f"wss://edge-chat.facebook.com/chat?sid={session_id}&cid={generate_client_id()}"
        cookie_dict = parse_cookie_string(self.cookies)
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookie_dict.items())

        client = mqtt.Client(client_id=f"mqttws_{self.user_id}_{int(time.time())}", clean_session=True, protocol=mqtt.MQTTv31, transport="websockets")
        client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLSv1_2)
        client.tls_insecure_set(True)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.username_pw_set(username=json_minimal(user))

        parsed_host = urlparse(host)
        client.ws_set_options(
            path=f"{parsed_host.path}?{parsed_host.query}",
            headers={"Cookie": cookie_str, "Origin": "https://www.facebook.com", "User-Agent": self.user_agent, "Referer": "https://www.facebook.com/", "Host": "edge-chat.facebook.com"},
        )
        self.mqtt_client = client
        client.connect(host="edge-chat.facebook.com", port=443, keepalive=60)
        client.loop_start()
        timeout = 15
        while not self.connected and timeout > 0:
            time.sleep(0.2)
            timeout -= 0.2
        if not self.connected:
            try:
                client.loop_stop()
                client.disconnect()
            except: pass
            raise Exception("MQTT timeout")

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0: return
        self.connected = True
        client.publish("/ls_app_settings", json_minimal({"ls_fdid": "", "ls_sv": "6928813347213944"}), qos=1)
        queue = {"sync_api_version": 10, "max_deltas_able_to_process": 1000, "delta_batch_size": 500, "encoding": "JSON", "entity_fbid": self.user_id, "initial_titan_sequence_id": self.last_seq_id, "device_params": None}
        client.publish("/messenger_sync_create_queue", json_minimal(queue), qos=1)

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False

    def send_message(self, text: str, thread_id: str):
        if not self.connected: return False
        if not text or not thread_id: return False
        with self.req_lock:
            self.ws_req_number += 1
            req_id = self.ws_req_number
            task_id_1 = req_id * 2
            task_id_2 = req_id * 2 + 1
        task_payload = {"initiating_source": 0, "multitab_env": 0, "otid": generate_offline_threading_id(), "send_type": 1, "skip_url_preview_gen": 0, "source": 0, "sync_group": 1, "text": text, "text_has_links": 0, "thread_id": int(thread_id)}
        task = {"failure_count": None, "label": "46", "payload": json.dumps(task_payload, separators=(",", ":")), "queue_name": str(thread_id), "task_id": task_id_1}
        task_mark = {"failure_count": None, "label": "21", "payload": json.dumps({"last_read_watermark_ts": int(time.time() * 1000), "sync_group": 1, "thread_id": int(thread_id)}, separators=(",", ":")), "queue_name": str(thread_id), "task_id": task_id_2}
        content = {"app_id": "2220391788200892", "payload": {"data_trace_id": None, "epoch_id": int(generate_offline_threading_id()), "tasks": [task, task_mark], "version_id": "7545284305482586"}, "request_id": req_id, "type": 3}
        content["payload"] = json.dumps(content["payload"], separators=(",", ":"))
        try:
            self.mqtt_client.publish("/ls_req", json.dumps(content, separators=(",", ":")), qos=1)
            return True
        except:
            self.connected = False
            return False

def get_thread_list(cookie):
    dataFB = dataGetHome(cookie)
    if not dataFB or "Unable to retrieve" in str(dataFB.get("fb_dtsg")):
        return None
    
    form = formAll(dataFB, "CometChatInboxQuery", "3336396659757871")
    form["queries"] = json.dumps({"o0": {"doc_id": "3336396659757871", "query_params": {"limit": 50, "before": None, "tags": ["INBOX"], "includeDeliveryReceipts": False, "includeSeqID": True}}})
    req_params = mainRequests("https://www.facebook.com/api/graphqlbatch/", form, cookie)
    req_params["headers"]["x-fb-friendly-name"] = "CometChatInboxQuery"
    try:
        res = requests.post(**req_params, timeout=15)
        text = res.text
        if text.startswith("for(;;);"): text = text[9:]
        first = text.split("\n")[0]
        if not first.strip() or first.startswith("<"): return None
        data = json.loads(first)
        if "errors" in data.get("o0", {}): return None
        nodes = data["o0"]["data"]["viewer"]["message_threads"]["nodes"]
        threads = []
        for node in nodes:
            tid = node.get("thread_key", {}).get("thread_fbid")
            name = node.get("name", "Không có tên")
            if tid: threads.append({"id": tid, "name": name})
        return threads
    except: return None

class Messenger:
    def __init__(self, cookie, account_number=0):
        self.cookie = cookie
        self.user_id = self.extract_uid()
        self.active = True
        self.success_count = 0
        self.sender = None
        self.account_number = account_number

    def extract_uid(self):
        try:
            for part in self.cookie.split(";"):
                if part.strip().startswith("c_user="):
                    uid = "".join(filter(str.isdigit, part.split("=")[1]))
                    if uid: return uid
        except: pass
        return "Unknown"

    def init_sender(self):
        if self.sender is None:
            try:
                self.sender = FacebookMQTTSender(self.cookie, f"ACC{self.account_number}")
                self.sender.connect()
                return True
            except:
                self.sender = None
                return False
        return self.sender.connected

    def gui_tn(self, recipient_id, message):
        if not self.active: return False
        if not self.init_sender():
            self.active = False
            return False
        if self.sender.send_message(message, recipient_id):
            self.success_count += 1
            return True
        return False

class SpamManager:
    def __init__(self, cookies_list, box_id, message, delay):
        self.cookies_list = cookies_list
        self.box_id = box_id
        # SỬA LỖI XUỐNG DÒNG: Biến chuỗi \n (escape) thành ký tự xuống dòng thực sự
        if isinstance(message, str):
            self.message = message.replace('\\n', '\n')
        else:
            self.message = message
            
        self.delay = delay
        self.is_running = True
        self.threads = []

    def start(self):
        self.is_running = True
        acc_count = 0
        for cookie in self.cookies_list:
            if not cookie or len(cookie) < 20: continue
            acc_count += 1
            acc = Messenger(cookie, acc_count)
            if acc.user_id != "Unknown":
                t = threading.Thread(target=self.worker, args=(acc,), daemon=True)
                self.threads.append(t)
                t.start()
                print(f"[*] Đã bật luồng ACC{acc_count} (UID: {acc.user_id})")
        return acc_count > 0

    def worker(self, account):
        while self.is_running and account.active:
            ok = account.gui_tn(self.box_id, self.message)
            now = datetime.now().strftime("%H:%M:%S")
            if ok: print(f"[{now}] [ACC{account.account_number}] Sent OK #{account.success_count}")
            else: print(f"[{now}] [ACC{account.account_number}] Sent Fail")
            time.sleep(self.delay * random.uniform(0.8, 1.2))

    def stop(self):
        self.is_running = False