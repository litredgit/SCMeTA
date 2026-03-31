# server_generate.py
from SCMeTA.core import Process
from flask import Flask, send_from_directory
import inspect
import os
import socketio
import json

# ------------------------------
# 1. 获取类方法及参数（保留原顺序，去掉 self）
# ------------------------------
methods = {}
for name, func in Process.__dict__.items():
    if callable(func):
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        # 去掉 self 参数
        if params and params[0].name == "self":
            params = params[1:]
        # 保存参数名和默认值
        params_info = []
        for p in params:
            if p.default is inspect.Parameter.empty:
                params_info.append({"name": p.name, "default": ""})
            else:
                params_info.append({"name": p.name, "default": repr(p.default)})
        methods[name] = params_info

# ------------------------------
# 2. 生成 HTML 文件（左右布局）
# ------------------------------
html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>SCMeTA Process Methods</title>
    <script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
    <style>
        body {{
            display: flex;
            flex-direction: row;
            height: 100vh;
            margin: 0;
            font-family: Arial, sans-serif;
        }}
        #forms {{
            width: 50%;
            padding: 20px;
            overflow-y: auto;
            box-sizing: border-box;
            border-right: 1px solid #ccc;
        }}
        #resultContainer {{
            width: 50%;
            padding: 20px;
            box-sizing: border-box;
            overflow-y: auto;
            background-color: #f9f9f9;
        }}
        input {{
            width: 90%;
            margin-bottom: 5px;
        }}
        button {{
            margin-top: 5px;
        }}
        .methodForm {{
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px dashed #aaa;
        }}
    </style>
</head>
<body>
    <div id="forms"></div>
    <div id="resultContainer">
        <h2>调用结果</h2>
        <pre id="result"></pre>
    </div>

    <script>
        const socket = io();

        socket.on("connect", () => {{
            console.log("Connected", socket.id);
        }});

        socket.on("result", (data) => {{
            const pre = document.getElementById("result");
            pre.textContent = JSON.stringify(data, null, 2);
        }});

        const methods = {methods_json};
        const formsDiv = document.getElementById("forms");

        for (const methodName of Object.keys(methods)) {{
            const paramList = methods[methodName];
            const form = document.createElement("div");
            form.className = "methodForm";
            form.innerHTML = "<b>" + methodName + "</b><br>";

            paramList.forEach(param => {{
                const input = document.createElement("input");
                input.placeholder = param.name;
                input.value = param.default !== "" ? param.default : "";
                input.id = methodName + "_" + param.name;
                form.appendChild(input);
                form.appendChild(document.createElement("br"));
            }});

            const btn = document.createElement("button");
            btn.textContent = "调用";
            btn.onclick = () => {{
                const paramValues = {{}};
                paramList.forEach(param => {{
                    const val = document.getElementById(methodName + "_" + param.name).value;
                    if (val !== "") {{ 
                    try {{
                        paramValues[param.name] = JSON.parse(val);
                    }} catch {{
                        paramValues[param.name] = val;
                    }}
                }}
                }});
                socket.emit("call_method", {{method: methodName, params: paramValues}});
            }};
            form.appendChild(btn);
            formsDiv.appendChild(form);
        }}
    </script>
</body>
</html>
"""

html_content = html_template.format(methods_json=json.dumps(methods))

# 保存 HTML 文件
html_file_path = os.path.join(os.path.dirname(__file__), "index.html")
with open(html_file_path, "w", encoding="utf-8") as f:
    f.write(html_content)
print(f"HTML file generated at {html_file_path}")

# ------------------------------
# 3. Flask + Socket.IO
# ------------------------------
app = Flask(__name__)
sio = socketio.Server(cors_allowed_origins="*")
app.wsgi_app = socketio.WSGIApp(sio, app.wsgi_app)

@app.route("/")
def index():
    return send_from_directory(os.path.dirname(__file__), "index.html")

@app.route("/favicon.ico")
def favicon():
    return "", 204

@sio.event
def connect(sid, environ):
    print("Client connected:", sid)

@sio.event
def call_method(sid, data):
    obj = Process()
    method_name = data["method"]
    params = {k: v for k, v in data.get("params", {}).items() if v != ""}  # 去掉空字符串
    try:
        result = getattr(obj, method_name)(**params)
        sio.emit("result", {"success": True, "result": str(result)}, to=sid)
    except Exception as e:
        sio.emit("result", {"success": False, "result": str(e)}, to=sid)

# ------------------------------
# 4. 启动服务器
# ------------------------------
def web(host="0.0.0.0", port=5000):
    print(f"Server running at http://{host}:{port}")
    app.run(host=host, port=port)

if __name__ == "__main__":
    web()