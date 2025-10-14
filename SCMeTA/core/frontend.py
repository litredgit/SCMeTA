from flask import Flask, send_file
from flask_socketio import SocketIO, emit
from SCMeTA.core import Process
import sys

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")
proc = None

class StreamRedirector:
    def __init__(self, stream_type):
        self.stream_type = stream_type
        self.buffer = ""
    def write(self, message):
        if message.strip():
            socketio.emit('terminal_output', {'type': self.stream_type, 'message': message})
    def flush(self):
        pass

@app.route('/')
def index():
    return send_file('frontend.html')

@socketio.on('init')
def ws_init(data):
    global proc
    proc = Process(ref_mz=data.get('ref_mz', 760.58), config_path=data.get('config_path'))
    emit('step_done', {'step': 'init'})

@socketio.on('load')
def ws_load(args):
    proc.load(**args)
    emit('step_done', {'step': 'load'})

@socketio.on('pre_process')
def ws_pre_process(args):
    proc.pre_process(**args)
    emit('step_done', {'step': 'pre_process'})

@socketio.on('process')
def ws_process(args):
    proc.process(**args)
    emit('step_done', {'step': 'process'})

@socketio.on('post_process')
def ws_post_process(args):
    proc.post_process(**args)
    emit('step_done', {'step': 'post_process'})

@socketio.on('save')
def ws_save(args):
    proc.save(**args)
    emit('step_done', {'step': 'save'})

def web(app, host="0.0.0.0", port=5000, debug=True):
    sys.stdout = StreamRedirector('stdout')
    sys.stderr = StreamRedirector('stderr')
    socketio.run(app=app, host=host, port=port, debug=debug, allow_unsafe_werkzeug=True)