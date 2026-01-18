# -*- coding: utf-8 -*-
"""
QMT 转发服务 - Windows 端
在 Windows 上运行，接收来自 Ubuntu 的「模块名+函数/类名+参数」请求，
在本地执行（可调用 xtquant/QMT），并返回结果。
仅在内网或可信环境使用。
"""

import base64
import json
import pickle
import sys
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
try:
    from http.server import ThreadingHTTPServer
except ImportError:
    ThreadingHTTPServer = HTTPServer

from importlib import import_module
from io import BytesIO

# 服务端保存的远程对象引用（object_id -> 实例）
_remote_objects = {}


def _resolve_callable(path):
    """根据路径 'xtquant.xttrader.XtQuantTrader' 解析出可调用对象（类或函数）。"""
    parts = path.strip().split(".")
    if len(parts) < 2:
        raise ValueError(f"无效路径: {path}")
    module_path = ".".join(parts[:-1])
    attr_name = parts[-1]
    mod = import_module(module_path)
    return getattr(mod, attr_name)


def _resolve_object_refs(obj):
    """递归把参数中的 {"__object_id": "xxx"} 替换为 Windows 端保存的真实对象。"""
    if isinstance(obj, dict) and set(obj.keys()) == {"__object_id"}:
        oid = obj["__object_id"]
        if oid not in _remote_objects:
            raise KeyError(f"未知 object_id: {oid}")
        return _remote_objects[oid]
    if isinstance(obj, list):
        return [_resolve_object_refs(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _resolve_object_refs(v) for k, v in obj.items()}
    return obj


def _call_path(path, args, kwargs):
    """执行 path 对应的可调用对象；args/kwargs 中的 __object_id 会被解析为真实对象。"""
    args = _resolve_object_refs(args)
    kwargs = _resolve_object_refs(kwargs)
    callable_obj = _resolve_callable(path)
    return callable_obj(*args, **kwargs)


def _try_serialize_result(result):
    """尝试序列化返回值。可 JSON 则直接返回；否则尝试 pickle+base64；否则存为远程对象返回 object_id。"""
    try:
        # 不要使用 default=str，否则 Pandas DataFrame 会被转成无用的字符串而跳过 pickle 序列化
        json.dumps(result)
        return {"type": "json", "result": result}
    except (TypeError, ValueError):
        pass
    try:
        raw = pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)
        return {"type": "pickle", "result_base64": base64.b64encode(raw).decode("ascii")}
    except Exception:
        pass
    # 无法序列化则保存在服务端，返回 object_id 供后续方法调用
    oid = str(uuid.uuid4())
    _remote_objects[oid] = result
    return {"type": "object_id", "object_id": oid}


def _call_method(object_id, method, args, kwargs):
    """对已保存的远程对象调用方法。"""
    if object_id not in _remote_objects:
        raise KeyError(f"未知 object_id: {object_id}")
    obj = _remote_objects[object_id]
    args = _resolve_object_refs(args)
    kwargs = _resolve_object_refs(kwargs)
    fn = getattr(obj, method)
    return fn(*args, **kwargs)


def _decode_args(raw):
    """从请求体解析 args/kwargs，支持 JSON 或 pickle base64。"""
    if not raw:
        return [], {}, {}
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        raise ValueError("请求体必须是 JSON")
    args = data.get("args", [])
    kwargs = data.get("kwargs", {})
    if data.get("args_pickle_b64"):
        try:
            args, kwargs = pickle.loads(base64.b64decode(data["args_pickle_b64"]))
        except Exception as e:
            raise ValueError(f"args_pickle_b64 解析失败: {e}")
    return args, kwargs, data


def _success_response(result_payload):
    """result_payload 为 _try_serialize_result 的返回值。"""
    return {"ok": True, **result_payload}


def _error_response(message, detail=None):
    return {"ok": False, "error": message, "detail": detail}


class RPCHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Token")
        self.end_headers()

    def do_POST(self):
        if self.path != "/rpc":
            self._send_json(404, _error_response("Not Found", self.path))
            return

        # 可选 token 校验
        try:
            from config import AUTH_TOKEN
            if AUTH_TOKEN and self.headers.get("X-Token") != AUTH_TOKEN:
                self._send_json(401, _error_response("Unauthorized"))
                return
        except ImportError:
            pass

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(content_length)
            args, kwargs, data = _decode_args(raw_body)
        except Exception as e:
            self._send_json(400, _error_response("请求解析失败", str(e)))
            return

        try:
            object_id = data.get("object_id")
            method = data.get("method")

            if object_id is not None and method:
                if method == "__del__":
                    _remote_objects.pop(object_id, None)
                    result = None
                else:
                    # 远程对象方法调用
                    result = _call_method(object_id, method, args, kwargs)
            elif data.get("path"):
                # 模块路径调用：path = "xtquant.xttrader.XtQuantTrader" 或 "xtquant.xtdata.get_full_tick"
                result = _call_path(data["path"], args, kwargs)
            else:
                self._send_json(400, _error_response("缺少 path 或 (object_id + method)"))
                return

            resp = _success_response(_try_serialize_result(result))
            self._send_json(200, resp)

        except Exception as e:
            self._send_json(500, _error_response(str(type(e).__name__), str(e)))

    def _send_json(self, status, obj):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"))

    def log_message(self, format, *args):
        print("[%s] %s" % (self.log_date_time_string(), format % args))


def run():
    try:
        from config import HOST, PORT
    except ImportError:
        HOST, PORT = "0.0.0.0", 8000
    server = ThreadingHTTPServer((HOST, PORT), RPCHandler)
    print("QMT 转发服务已启动: http://%s:%s  (POST /rpc)" % (HOST, PORT))
    print("按 Ctrl+C 停止")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        print("已停止")

HOST = "0.0.0.0"
PORT = 8000

# 可选：简单 token 校验（请求头 X-Token）
# AUTH_TOKEN = "your_secret_token"
AUTH_TOKEN = None

if __name__ == "__main__":
    run()
