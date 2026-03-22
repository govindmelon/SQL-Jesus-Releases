# He built, while otheres erased, he made something greater. Even greater than himself. SQL JESUS


"""
SQL Jesus -- MySQL proxy with dashboard.
Single file. Run as Administrator.

    python jesus.py
"""

import logging
import math
import os
import queue
import re
import socket
import ssl
import subprocess
import sys
import threading
import time
import tkinter as tk
from collections import defaultdict, deque
from datetime import datetime
from tkinter import font as tkfont
from tkinter import ttk

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


# ──────────────────────────────────────────────
# Auto-updater
# ──────────────────────────────────────────────
GITHUB_REPO   = "govindmelon/SQL-Jesus-Releases"
BRANCH        = "main"
CURRENT_SHA   = "dev"   # overwritten by build.py when packaged


def _load_bundled_sha():
    """Read version.txt from the PyInstaller bundle or local dir."""
    global CURRENT_SHA
    try:
        # PyInstaller bundles files into sys._MEIPASS
        base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base, "version.txt")
        with open(path) as f:
            sha = f.read().strip()
            if sha and sha != "0" * 40:
                CURRENT_SHA = sha
    except Exception:
        pass


_load_bundled_sha()


def _check_for_updates():
    """
    Background thread. Compares current SHA to latest commit on GitHub.
    Pushes an 'update_available' event to ui_queue if newer.
    """
    import urllib.request, json
    url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/{BRANCH}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "sql-jesus"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data   = json.loads(r.read())
            latest = data["sha"]
            msg    = data["commit"]["message"].split("\n")[0][:60]
            if latest != CURRENT_SHA and CURRENT_SHA != "dev":
                push({"type": "update_available",
                      "sha": latest, "msg": msg})
    except Exception:
        pass   # silently ignore network errors


def _do_update(new_sha):
    """Download new exe from GitHub releases and swap via updater.py."""
    import urllib.request, tempfile, subprocess
    exe_url = (
        f"https://github.com/{GITHUB_REPO}/raw/{new_sha}/jesus.exe"
    )
    push({"type": "update_progress", "msg": "Downloading update..."})
    try:
        tmp = tempfile.mktemp(suffix=".exe")
        urllib.request.urlretrieve(exe_url, tmp)

        current_exe = sys.executable if getattr(sys, "frozen", False) else None
        if current_exe is None:
            # Running as .py — just tell user to pull from GitHub
            push({"type": "update_progress",
                  "msg": "Pull latest jesus.py from GitHub to update."})
            return

        # Find or extract updater.py
        updater_path = os.path.join(os.path.dirname(current_exe), "updater.py")
        if not os.path.exists(updater_path):
            base = getattr(sys, "_MEIPASS", "")
            src  = os.path.join(base, "updater.py")
            if os.path.exists(src):
                import shutil
                shutil.copy(src, updater_path)

        push({"type": "update_progress", "msg": "Restarting to apply update..."})
        subprocess.Popen(
            [sys.executable, updater_path, tmp, current_exe],
            creationflags=0x00000008  # DETACHED_PROCESS on Windows
        )
        # Exit so updater can replace the exe
        os._exit(0)

    except Exception as e:
        push({"type": "update_progress", "msg": f"Update failed: {e}"})

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
PROXY_HOST  = "0.0.0.0"
PROXY_PORT  = 3306
MYSQL_HOST  = "127.0.0.1"
MYSQL_PORT  = 3307
LOG_FILE    = "proxy.log"
MAX_THREADS = 50
PROXY_CERT  = "proxy-cert.pem"
PROXY_KEY   = "proxy-key.pem"

# ──────────────────────────────────────────────
# Blocked SQL patterns
# ──────────────────────────────────────────────
BLOCKED_PATTERNS = [
    r"\bDROP\s+(DATABASE|SCHEMA|TABLE|VIEW|PROCEDURE|FUNCTION|TRIGGER|EVENT|INDEX)\b",
    r"\bTRUNCATE\b",
    r"\bALTER\s+TABLE\b",
    r"\bRENAME\s+TABLE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bCREATE\s+USER\b",
    r"\bDROP\s+USER\b",
    r"\bALTER\s+USER\b",
    r"\bFLUSH\s+PRIVILEGES\b",
    r"\bSHUTDOWN\b",
    r"\bRESET\s+MASTER\b",
    r"\bRESET\s+SLAVE\b",
    r"\bPURGE\b",
    r"\bLOAD\s+DATA\s+INFILE\b",
    r"\bINTO\s+OUTFILE\b",
    r"\bINTO\s+DUMPFILE\b",
]
COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE | re.DOTALL) for p in BLOCKED_PATTERNS]
patterns_lock    = threading.Lock()
blocking_enabled = True   # global kill-switch — set False to let everything through

# ──────────────────────────────────────────────
# Shared state
# ──────────────────────────────────────────────
ui_queue     = queue.Queue()
stats_lock   = threading.Lock()
stats = {
    "total":       0,
    "blocked":     0,
    "allowed":     0,
    "connections": 0,
    "active":      0,
}
blocked_ips     = defaultdict(int)
session_history = defaultdict(list)  # ip -> list of {time, sql, status}
session_lock    = threading.Lock()
_server_sock    = None
_proxy_thread   = None
_start_time     = None   # set when proxy starts
_shutdown_event = threading.Event()  # set to kill all active connection threads

# Health time-series (last 60 seconds, one sample per second)
HEALTH_WINDOW = 60
health_lock   = threading.Lock()
health = {
    "cpu":         deque([0.0] * HEALTH_WINDOW, maxlen=HEALTH_WINDOW),
    "mem":         deque([0.0] * HEALTH_WINDOW, maxlen=HEALTH_WINDOW),
    "qps":         deque([0.0] * HEALTH_WINDOW, maxlen=HEALTH_WINDOW),
    "conns":       deque([0.0] * HEALTH_WINDOW, maxlen=HEALTH_WINDOW),
    "_last_total": 0,
}


def push(event: dict):
    ui_queue.put(event)


def inc(key, n=1):
    with stats_lock:
        stats[key] += n


# ──────────────────────────────────────────────
# Health sampler (runs every second)
# ──────────────────────────────────────────────
def _health_sampler():
    proc = psutil.Process(os.getpid()) if HAS_PSUTIL else None
    while True:
        time.sleep(1)
        with stats_lock:
            total = stats["total"]
            active = stats["active"]
        with health_lock:
            qps = total - health["_last_total"]
            health["_last_total"] = total
            health["qps"].append(float(qps))
            health["conns"].append(float(active))
            if proc:
                try:
                    health["cpu"].append(proc.cpu_percent(interval=None))
                    health["mem"].append(proc.memory_info().rss / 1024 / 1024)
                except Exception:
                    health["cpu"].append(0.0)
                    health["mem"].append(0.0)
            else:
                health["cpu"].append(0.0)
                health["mem"].append(0.0)


threading.Thread(target=_health_sampler, daemon=True).start()


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
class UIQueueHandler(logging.Handler):
    def emit(self, record):
        push({"type": "log", "level": record.levelname, "msg": self.format(record)})


log = logging.getLogger("sql_jesus")
log.setLevel(logging.DEBUG)
_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
log.addHandler(_sh)
_uh = UIQueueHandler()
_uh.setFormatter(_fmt)
log.addHandler(_uh)


# ──────────────────────────────────────────────
# SSL cert auto-generation
# ──────────────────────────────────────────────
def ensure_ssl_cert(progress_cb=None):
    def report(msg):
        if progress_cb:
            progress_cb(msg)

    if os.path.exists(PROXY_CERT) and os.path.exists(PROXY_KEY):
        report("SSL cert found.")
        return

    report("Generating SSL certificate...")
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        import datetime as dt

        key  = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u"mysql-proxy")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name).issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(dt.datetime.now(dt.timezone.utc))
            .not_valid_after(dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(key, hashes.SHA256())
        )
        with open(PROXY_KEY, "wb") as f:
            f.write(key.private_bytes(serialization.Encoding.PEM,
                    serialization.PrivateFormat.TraditionalOpenSSL,
                    serialization.NoEncryption()))
        with open(PROXY_CERT, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        report("SSL certificate generated.")
        return
    except ImportError:
        pass

    report("Trying openssl CLI...")
    cmd = ["openssl", "req", "-x509", "-newkey", "rsa:2048",
           "-keyout", PROXY_KEY, "-out", PROXY_CERT,
           "-days", "3650", "-nodes", "-subj", "/CN=mysql-proxy"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode == 0:
            report("SSL certificate generated via openssl.")
        else:
            report("openssl failed. Run: pip install cryptography")
            sys.exit(1)
    except FileNotFoundError:
        report("ERROR: pip install cryptography")
        sys.exit(1)


# ──────────────────────────────────────────────
# Proxy helpers
# ──────────────────────────────────────────────
def is_blocked(sql):
    if not blocking_enabled:
        return None
    with patterns_lock:
        pairs = list(zip(COMPILED_PATTERNS, BLOCKED_PATTERNS))
    for pattern, original in pairs:
        if pattern.search(sql):
            return original
    return None


def set_patterns(new_patterns):
    """Replace the live pattern list — called from the Rules UI."""
    global BLOCKED_PATTERNS, COMPILED_PATTERNS
    compiled = []
    valid    = []
    for p in new_patterns:
        p = p.strip()
        if not p:
            continue
        try:
            compiled.append(re.compile(p, re.IGNORECASE | re.DOTALL))
            valid.append(p)
        except re.error:
            pass  # silently skip bad regex
    with patterns_lock:
        BLOCKED_PATTERNS  = valid
        COMPILED_PATTERNS = compiled


def make_error_packet(message):
    msg     = message.encode("utf-8")
    payload = bytes([0xFF]) + (1064).to_bytes(2, "little") + b"#HY000" + msg
    return len(payload).to_bytes(3, "little") + b"\x01" + payload


def recv_all(sock, n):
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf += chunk
    return buf


def read_packet(sock):
    header = recv_all(sock, 4)
    if header is None:
        return None
    plen = int.from_bytes(header[:3], "little")
    if plen == 0:
        return header
    payload = recv_all(sock, plen)
    if payload is None:
        return None
    return header + payload


CLIENT_SSL = 0x00000800

def client_wants_ssl(pkt):
    if len(pkt) < 8:
        return False
    return bool(int.from_bytes(pkt[4:8], "little") & CLIENT_SSL)


def make_server_ssl_ctx():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=PROXY_CERT, keyfile=PROXY_KEY)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def make_client_ssl_ctx():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    return ctx


# ──────────────────────────────────────────────
# Per-connection handler
# ──────────────────────────────────────────────
def handle_client(client_sock, client_addr):
    addr = f"{client_addr[0]}:{client_addr[1]}"
    ip   = client_addr[0]
    log.info("CONNECT  | %s", addr)
    inc("connections")
    inc("active")
    push({"type": "connect", "addr": addr})

    try:
        raw_server = socket.create_connection((MYSQL_HOST, MYSQL_PORT), timeout=10)
    except OSError as exc:
        log.error("Cannot reach MySQL: %s", exc)
        client_sock.close()
        inc("active", -1)
        return

    client_sock.settimeout(300)
    raw_server.settimeout(300)

    def close_both(c, s):
        try: c.close()
        except: pass
        try: s.close()
        except: pass

    greeting = read_packet(raw_server)
    if greeting is None:
        close_both(client_sock, raw_server); inc("active", -1); return
    try:
        client_sock.sendall(greeting)
    except OSError:
        close_both(client_sock, raw_server); inc("active", -1); return

    client_resp = read_packet(client_sock)
    if client_resp is None:
        close_both(client_sock, raw_server); inc("active", -1); return

    if client_wants_ssl(client_resp):
        try:
            raw_server.sendall(client_resp)
            server_sock = make_client_ssl_ctx().wrap_socket(raw_server, server_hostname=MYSQL_HOST)
            client_conn = make_server_ssl_ctx().wrap_socket(client_sock, server_side=True)
        except ssl.SSLError as exc:
            log.error("SSL upgrade failed: %s", exc)
            close_both(client_sock, raw_server); inc("active", -1); return
    else:
        server_sock = raw_server
        client_conn = client_sock
        try:
            server_sock.sendall(client_resp)
        except OSError:
            close_both(client_conn, server_sock); inc("active", -1); return

    auth_done    = threading.Event()
    client_spoke = threading.Event()

    def server_to_client():
        while not _shutdown_event.is_set():
            pkt = read_packet(server_sock)
            if pkt is None: break
            first = pkt[4] if len(pkt) > 4 else None
            if not auth_done.is_set() and client_spoke.is_set() and first == 0x00:
                auth_done.set()
            try:
                client_conn.sendall(pkt)
            except OSError:
                break
        auth_done.set()
        close_both(client_conn, server_sock)

    def client_to_server():
        client_spoke.set()
        while not _shutdown_event.is_set():
            pkt = read_packet(client_conn)
            if pkt is None: break
            seq = pkt[3]
            cmd = pkt[4] if len(pkt) > 4 else 0
            if not auth_done.is_set():
                try: server_sock.sendall(pkt)
                except OSError: break
                continue
            if seq == 0x00 and cmd == 0x03:
                try:
                    sql = pkt[5:].decode("utf-8", errors="replace")
                    sql = sql.lstrip("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09").strip()
                except Exception:
                    sql = ""
                inc("total")
                blocked_by = is_blocked(sql)
                ts = datetime.now().strftime("%H:%M:%S")
                if blocked_by:
                    inc("blocked")
                    with stats_lock: blocked_ips[ip] += 1
                    with session_lock:
                        session_history[ip].append(
                            {"time": ts, "sql": sql, "status": "BLOCKED", "rule": blocked_by})
                    log.warning("BLOCKED | %s | %.120s", addr, sql)
                    push({"type": "blocked", "addr": addr, "ip": ip,
                          "sql": sql, "rule": blocked_by, "time": ts})
                    try:
                        client_conn.sendall(make_error_packet(
                            "[SQL JESUS] : I shield the innocent from your hands of sin..."
                        ))
                    except OSError: break
                    continue
                else:
                    inc("allowed")
                    with session_lock:
                        session_history[ip].append(
                            {"time": ts, "sql": sql, "status": "ALLOWED", "rule": ""})
                    log.info("ALLOWED | %s | %.120s", addr, sql)
                    push({"type": "allowed", "addr": addr, "sql": sql, "time": ts})
            try: server_sock.sendall(pkt)
            except OSError: break
        close_both(client_conn, server_sock)

    t_s2c = threading.Thread(target=server_to_client, daemon=True)
    t_c2s = threading.Thread(target=client_to_server, daemon=True)
    t_s2c.start(); t_c2s.start()
    t_s2c.join();  t_c2s.join()
    inc("active", -1)
    push({"type": "disconnect", "addr": addr})
    log.info("CLOSE    | %s", addr)


# ──────────────────────────────────────────────
# Proxy server loop
# ──────────────────────────────────────────────
def proxy_loop(progress_cb=None):
    global _server_sock, _start_time
    _shutdown_event.clear()

    def report(msg):
        log.info(msg)
        if progress_cb:
            progress_cb(msg)

    report("Checking SSL certificate...")
    ensure_ssl_cert(progress_cb)
    time.sleep(0.3)

    report("Creating server socket...")
    time.sleep(0.2)

    try:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        report(f"Binding to port {PROXY_PORT}...")
        time.sleep(0.2)
        srv.bind((PROXY_HOST, PROXY_PORT))
        srv.listen(MAX_THREADS)
        _server_sock = srv
        _start_time  = time.time()
        report("READY")
        push({"type": "status", "running": True})

        active = 0
        while True:
            try:
                cs, ca = srv.accept()
            except OSError:
                break
            if active >= MAX_THREADS:
                cs.close(); continue
            active += 1
            def run(s=cs, a=ca):
                nonlocal active
                handle_client(s, a)
                active -= 1
            threading.Thread(target=run, daemon=True).start()

    except OSError as exc:
        log.error("Proxy error: %s", exc)
        push({"type": "progress", "msg": f"ERROR: {exc}"})
    finally:
        push({"type": "status", "running": False})
        log.info("Proxy stopped.")


def start_proxy(progress_cb=None):
    global _proxy_thread
    _proxy_thread = threading.Thread(
        target=proxy_loop, args=(progress_cb,), daemon=True)
    _proxy_thread.start()


def stop_proxy():
    global _server_sock, _start_time
    _shutdown_event.set()   # signal all connection threads to exit
    _start_time = None
    if _server_sock:
        try: _server_sock.close()
        except OSError: pass
        _server_sock = None


# ══════════════════════════════════════════════
# UI COMPONENTS
# ══════════════════════════════════════════════

BG       = "#000000"
SURFACE  = "#111111"
SURFACE2 = "#1a1a1a"
ACCENT   = "#ffffff"
TEXT     = "#ededed"
MUTED    = "#555555"
BORDER   = "#222222"
RED      = "#ff4444"


# ──────────────────────────────────────────────
# Slim scrollbar
# ──────────────────────────────────────────────
class SlimScrollbar(tk.Canvas):
    TRACK = "#1a1a1a"
    THUMB = "#444444"
    HOVER = "#666666"

    def __init__(self, master, orient="vertical", command=None, **kw):
        kw.setdefault("bg", self.TRACK)
        kw.setdefault("highlightthickness", 0)
        kw.setdefault("bd", 0)
        kw.setdefault("relief", "flat")
        kw.setdefault("width" if orient == "vertical" else "height", 6)
        super().__init__(master, **kw)
        self._orient  = orient
        self._command = command
        self._thumb   = None
        self._pos     = (0.0, 1.0)
        self._drag_y  = self._drag_x = None
        self.bind("<Configure>",       self._redraw)
        self.bind("<ButtonPress-1>",   self._on_press)
        self.bind("<B1-Motion>",       self._on_drag)
        self.bind("<ButtonRelease-1>", self._on_release)
        self.bind("<Enter>",  lambda e: self._set_color(self.HOVER))
        self.bind("<Leave>",  lambda e: self._set_color(self.THUMB))

    def set(self, first, last):
        self._pos = (float(first), float(last))
        self._redraw()

    def _redraw(self, _=None):
        self.delete("thumb")
        w, h = self.winfo_width(), self.winfo_height()
        if w < 1 or h < 1: return
        f, l = self._pos
        p = 2
        if self._orient == "vertical":
            self._thumb = self.create_rectangle(p, f*h+p, w-p, l*h-p,
                fill=self.THUMB, outline="", tags="thumb", width=0)
        else:
            self._thumb = self.create_rectangle(f*w+p, p, l*w-p, h-p,
                fill=self.THUMB, outline="", tags="thumb", width=0)

    def _set_color(self, c):
        if self._thumb: self.itemconfig(self._thumb, fill=c)

    def _on_press(self, e):
        self._drag_y = e.y; self._drag_x = e.x

    def _on_drag(self, e):
        if not self._command: return
        f, l = self._pos
        size = l - f
        if self._orient == "vertical":
            delta = (e.y - self._drag_y) / max(self.winfo_height(), 1)
            self._drag_y = e.y
        else:
            delta = (e.x - self._drag_x) / max(self.winfo_width(), 1)
            self._drag_x = e.x
        self._command("moveto", max(0.0, min(1.0 - size, f + delta)))

    def _on_release(self, _):
        self._drag_y = self._drag_x = None


# ──────────────────────────────────────────────
# Sparkline canvas chart
# ──────────────────────────────────────────────
class Sparkline(tk.Canvas):
    def __init__(self, master, color="#ffffff", label="", unit="", **kw):
        kw.setdefault("bg", SURFACE2)
        kw.setdefault("highlightthickness", 0)
        kw.setdefault("bd", 0)
        super().__init__(master, **kw)
        self._color  = color
        self._label  = label
        self._unit   = unit
        self._data   = [0.0] * HEALTH_WINDOW
        self.bind("<Configure>", lambda _: self._draw())

    def update_data(self, data):
        self._data = list(data)
        self._draw()

    def _draw(self):
        self.delete("all")
        w = self.winfo_width()
        h = self.winfo_height()
        if w < 4 or h < 4:
            return

        data  = self._data
        top   = 22   # header row
        bot   = h - 6
        plotH = bot - top
        if plotH < 4:
            return

        # avoid divide-by-zero; if all zeros show flat baseline
        mx = max(data) if max(data) > 0 else 1.0

        def vy(v):
            # high value -> low y (near top), low value -> high y (near bot)
            return top + plotH * (1.0 - min(v, mx) / mx)

        # subtle grid
        for i in range(1, 4):
            gy = top + plotH * i / 4
            self.create_line(0, gy, w, gy, fill=BORDER, width=1)

        n = len(data)
        if n < 2:
            return

        # x positions evenly spaced
        xs = [w * i / (n - 1) for i in range(n)]
        ys = [vy(v) for v in data]

        # filled area: polygon from baseline-left -> data points -> baseline-right
        poly = [xs[0], bot]
        for x, y in zip(xs, ys):
            poly += [x, y]
        poly += [xs[-1], bot]
        # dim fill using multiple alpha-simulated rectangles — just use a simple polygon
        # with the line color at low opacity via stipple
        self.create_polygon(poly, fill=self._color, stipple="gray12", outline="")

        # bright line
        line_pts = []
        for x, y in zip(xs, ys):
            line_pts += [x, y]
        self.create_line(line_pts, fill=self._color, width=1, smooth=False)

        # labels
        cur = data[-1]
        self.create_text(6, top // 2, anchor="w",
                         text=self._label, fill=MUTED, font=("Segoe UI", 8))
        self.create_text(w - 6, top // 2, anchor="e",
                         text=f"{cur:.1f}{self._unit}",
                         fill=self._color, font=("Segoe UI", 9, "bold"))
        self.create_text(w - 6, top + 4, anchor="ne",
                         text=f"max {mx:.1f}{self._unit}",
                         fill=MUTED, font=("Segoe UI", 7))


# ──────────────────────────────────────────────
# Splash screen
# ──────────────────────────────────────────────
class SplashScreen(tk.Toplevel):
    def __init__(self, parent, on_done):
        super().__init__(parent)
        self.overrideredirect(True)
        self.configure(bg=BG)
        self.resizable(False, False)

        W, H = 420, 340
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        self.geometry(f"{W}x{H}+{(sw-W)//2}+{(sh-H)//2}")

        self._on_done   = on_done
        self._angle     = 0.0
        self._messages  = []
        self._done      = False

        # border
        tk.Frame(self, bg=BORDER, padx=1, pady=1).place(x=0, y=0, relwidth=1, relheight=1)
        inner = tk.Frame(self, bg=BG)
        inner.place(x=1, y=1, width=W-2, height=H-2)

        # cross canvas
        self._cross_canvas = tk.Canvas(inner, width=90, height=90,
                                       bg=BG, highlightthickness=0)
        self._cross_canvas.pack(pady=(36, 0))

        # title
        tk.Label(inner, text="SQL Jesus", bg=BG, fg=ACCENT,
                 font=("Georgia", 20, "italic")).pack(pady=(10, 0))
        tk.Label(inner, text="initialising...", bg=BG, fg=MUTED,
                 font=("Segoe UI", 9)).pack(pady=(2, 16))

        # progress message
        self._msg_var = tk.StringVar(value="Starting up...")
        tk.Label(inner, textvariable=self._msg_var, bg=BG, fg=MUTED,
                 font=("Consolas", 8)).pack(pady=(0, 10))

        # progress bar track
        bar_track = tk.Frame(inner, bg=SURFACE2, height=2)
        bar_track.pack(fill="x", padx=40, pady=(0, 0))
        self._bar = tk.Frame(bar_track, bg=ACCENT, height=2)
        self._bar.place(x=0, y=0, relheight=1, relwidth=0)

        self._progress = 0.0
        self._target   = 0.0

        self._animate_cross()
        self._animate_bar()

    # ── spinning halo around the cross ──
    def _animate_cross(self):
        if self._done: return
        c  = self._cross_canvas
        cx = cy = 45
        r  = 34

        c.delete("all")

        # halo dots
        n = 10
        for i in range(n):
            a   = math.radians(self._angle + i * 360 / n)
            x   = cx + r * math.sin(a)
            y   = cy - r * math.cos(a)
            alpha = (i / n)
            grey  = int(80 + 160 * alpha)
            color = f"#{grey:02x}{grey:02x}{grey:02x}"
            size  = 2 + 3 * alpha
            c.create_oval(x - size, y - size, x + size, y + size,
                          fill=color, outline="")

        # cross
        arm = 22
        thick = 5
        c.create_rectangle(cx - thick//2, cy - arm, cx + thick//2, cy + arm,
                            fill=ACCENT, outline="")
        c.create_rectangle(cx - arm, cy - thick//2, cx + arm, cy + thick//2,
                            fill=ACCENT, outline="")

        self._angle = (self._angle + 4) % 360
        self.after(30, self._animate_cross)

    # ── smooth progress bar ──
    def _animate_bar(self):
        if self._done: return
        self._progress += (self._target - self._progress) * 0.12
        self._bar.place(relwidth=min(self._progress, 1.0))
        self.after(16, self._animate_bar)

    def set_progress(self, msg, fraction):
        self._msg_var.set(msg)
        self._target = fraction
        if fraction >= 1.0:
            self.after(600, self._finish)

    def _finish(self):
        self._done = True
        self.destroy()
        self._on_done()


# ──────────────────────────────────────────────
# Main dashboard
# ──────────────────────────────────────────────
class Dashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.withdraw()   # hidden until splash done
        self.title("SQL Jesus -- Proxy Dashboard")
        self.configure(bg=BG)
        self.geometry("1150x760")
        self.minsize(950, 620)
        self.proxy_running = False
        self._current_page = None

        self._build_shell()
        self._show_splash()

    # ── splash ───────────────────────────────────
    def _show_splash(self):
        splash = SplashScreen(self, self._on_splash_done)

        steps = [
            ("Checking environment...",      0.15),
            ("Loading configuration...",     0.30),
            ("Preparing SSL certificate...", 0.55),
            ("Building dashboard...",        0.75),
            ("Almost there...",              0.90),
            ("Ready.",                       1.00),
        ]

        def run_steps(i=0):
            if i >= len(steps):
                return
            msg, frac = steps[i]
            splash.set_progress(msg, frac)
            delay = 400 if i < len(steps) - 1 else 100
            self.after(delay, lambda: run_steps(i + 1))

        self.after(300, lambda: run_steps())

    def _on_splash_done(self):
        self.deiconify()
        self._show_page("dashboard")
        # Start update check in background
        threading.Thread(target=_check_for_updates, daemon=True).start()

    # ── shell (topbar + page container) ──────────
    def _build_shell(self):
        # topbar
        topbar = tk.Frame(self, bg=SURFACE, height=56)
        topbar.pack(fill="x", side="top")
        topbar.pack_propagate(False)
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x", side="top")

        title_frame = tk.Frame(topbar, bg=SURFACE)
        title_frame.pack(side="left", padx=20)
        tk.Label(title_frame, text="\u271d", bg=SURFACE,
                 fg=ACCENT, font=("Georgia", 22, "italic")).pack(side="left", padx=(0, 6))
        tk.Label(title_frame, text="SQL Jesus", bg=SURFACE,
                 fg=ACCENT, font=("Georgia", 16, "italic")).pack(side="left")

        # nav buttons
        nav = tk.Frame(topbar, bg=SURFACE)
        nav.pack(side="left", padx=20)
        self._nav_btns = {}
        for name, label in [("dashboard", "Dashboard"), ("health", "Health"), ("rules", "Rules")]:
            btn = tk.Button(nav, text=label, bg=SURFACE, fg=MUTED,
                            font=("Segoe UI", 9), relief="flat",
                            padx=12, pady=4, cursor="hand2",
                            activebackground=SURFACE2, activeforeground=TEXT,
                            command=lambda n=name: self._show_page(n))
            btn.pack(side="left", padx=2)
            self._nav_btns[name] = btn

        # status pill
        self._status_canvas = tk.Canvas(topbar, bg=SURFACE,
                                        highlightthickness=0, height=28)
        self._status_canvas.pack(side="left", padx=(8, 20))
        self._status_pill_running = False
        self._draw_status_pill(False, "Stopped")

        self.toggle_btn = tk.Button(topbar, text="\u25b6  Start Proxy",
                                    bg="#ffffff", fg="#000000",
                                    font=("Segoe UI", 10, "bold"),
                                    relief="flat", padx=16, pady=6,
                                    cursor="hand2", command=self._toggle_proxy)
        self.toggle_btn.pack(side="right", padx=20, pady=10)

        # update banner (hidden by default, shown when update available)
        self._update_bar = tk.Frame(self, bg="#1a1a1a", pady=6)
        # not packed until update found
        self._update_bar_packed = False

        update_inner = tk.Frame(self._update_bar, bg="#1a1a1a")
        update_inner.pack()
        tk.Label(update_inner, text="Update available", bg="#1a1a1a",
                 fg="#ededed", font=("Segoe UI", 9, "bold")).pack(side="left", padx=(0,8))
        self._update_msg_var = tk.StringVar(value="")
        tk.Label(update_inner, textvariable=self._update_msg_var,
                 bg="#1a1a1a", fg="#555555",
                 font=("Segoe UI", 9)).pack(side="left", padx=(0, 16))
        self._update_btn = tk.Button(update_inner, text="Update & restart",
                                     bg="#ffffff", fg="#000000",
                                     font=("Segoe UI", 9, "bold"),
                                     relief="flat", padx=12, pady=3,
                                     cursor="hand2",
                                     command=self._start_update)
        self._update_btn.pack(side="left")
        self._pending_sha = None

        # page container
        self._container = tk.Frame(self, bg=BG)
        self._container.pack(fill="both", expand=True)

    # ── page routing ─────────────────────────────
    def _show_page(self, name):
        for w in self._container.winfo_children():
            w.destroy()
        self._current_page = name

        # nav highlight
        for n, btn in self._nav_btns.items():
            if n == name:
                btn.config(fg=ACCENT, bg=SURFACE2)
            else:
                btn.config(fg=MUTED, bg=SURFACE)

        if name == "dashboard":
            self._build_dashboard(self._container)
        elif name == "health":
            self._build_health(self._container)
        elif name == "rules":
            self._build_rules(self._container)

    # ══════════════════════════════════════════
    # DASHBOARD PAGE
    # ══════════════════════════════════════════
    def _build_dashboard(self, parent):
        mono = tkfont.Font(family="Consolas", size=9)

        # stat cards
        cards = tk.Frame(parent, bg=BG)
        cards.pack(fill="x", padx=16, pady=(12, 0))
        self.stat_vars = {}
        defs = [
            ("total",       "Total Queries", TEXT),
            ("allowed",     "Allowed",       TEXT),
            ("blocked",     "Blocked",       RED),
            ("connections", "Connections",   "#888888"),
            ("active",      "Active Now",    TEXT),
        ]
        for key, label, colour in defs:
            border = tk.Frame(cards, bg=BORDER, padx=1, pady=1)
            border.pack(side="left", expand=True, fill="both", padx=6)
            card = tk.Frame(border, bg=SURFACE2, padx=18, pady=12)
            card.pack(fill="both", expand=True)
            v = tk.StringVar(value="0")
            self.stat_vars[key] = v
            tk.Label(card, textvariable=v, bg=SURFACE2, fg=colour,
                     font=("Segoe UI", 26, "bold")).pack(anchor="w")
            tk.Label(card, text=label, bg=SURFACE2, fg=MUTED,
                     font=("Segoe UI", 9)).pack(anchor="w")

        main = tk.Frame(parent, bg=BG)
        main.pack(fill="both", expand=True, padx=16, pady=12)

        # live log
        tk.Label(main, text="Live feed", bg=BG, fg=MUTED,
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(0, 4))
        log_border = tk.Frame(main, bg=BORDER, padx=1, pady=1)
        log_border.pack(fill="both", expand=True)
        log_frame = tk.Frame(log_border, bg=SURFACE)
        log_frame.pack(fill="both", expand=True)

        self.log_box = tk.Text(log_frame, bg=SURFACE, fg=TEXT,
                               font=mono, relief="flat", bd=0,
                               state="disabled", wrap="none",
                               insertbackground=TEXT)
        log_vsb = SlimScrollbar(log_frame, orient="vertical",   command=self.log_box.yview)
        log_hsb = SlimScrollbar(log_frame, orient="horizontal", command=self.log_box.xview)
        self.log_box.configure(yscrollcommand=log_vsb.set, xscrollcommand=log_hsb.set)
        log_vsb.pack(side="right",  fill="y")
        log_hsb.pack(side="bottom", fill="x")
        self.log_box.pack(fill="both", expand=True)
        self.log_box.tag_config("BLOCKED", foreground=RED)
        self.log_box.tag_config("ALLOWED", foreground="#aaaaaa")
        self.log_box.tag_config("INFO",    foreground=TEXT)
        self.log_box.tag_config("DEBUG",   foreground=MUTED)
        self.log_box.tag_config("WARNING", foreground="#cccccc")
        self.log_box.tag_config("ERROR",   foreground="#ffffff")

        # blocked + offenders strip
        bottom = tk.Frame(main, bg=BG)
        bottom.pack(fill="x", pady=(12, 0))

        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Dark.Treeview",
                        background=SURFACE, foreground=TEXT,
                        fieldbackground=SURFACE, rowheight=24,
                        font=("Consolas", 9))
        style.configure("Dark.Treeview.Heading",
                        background=SURFACE2, foreground=MUTED,
                        font=("Segoe UI", 9, "bold"), relief="flat")
        style.map("Dark.Treeview",
                  background=[("selected", "#333333")],
                  foreground=[("selected", "#ffffff")])

        blocked_col = tk.Frame(bottom, bg=BG)
        blocked_col.pack(side="left", fill="both", expand=True)
        tk.Label(blocked_col, text="Blocked attempts", bg=BG, fg=MUTED,
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(0, 4))
        tree_border = tk.Frame(blocked_col, bg=BORDER, padx=1, pady=1)
        tree_border.pack(fill="both", expand=True)
        tree_frame = tk.Frame(tree_border, bg=SURFACE)
        tree_frame.pack(fill="both", expand=True)
        cols = ("time", "ip", "sql", "rule")
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings",
                                 style="Dark.Treeview", height=7, selectmode="browse")
        self.tree.heading("time", text="Time")
        self.tree.heading("ip",   text="IP")
        self.tree.heading("sql",  text="Query")
        self.tree.heading("rule", text="Rule")
        self.tree.column("time", width=70,  stretch=False)
        self.tree.column("ip",   width=120, stretch=False)
        self.tree.column("sql",  width=400)
        self.tree.column("rule", width=200, stretch=False)
        tree_vsb = SlimScrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_vsb.set)
        tree_vsb.pack(side="right", fill="y")
        self.tree.pack(fill="both", expand=True)



    # ══════════════════════════════════════════
    # RULES PAGE
    # ══════════════════════════════════════════
    def _build_rules(self, parent):
        global blocking_enabled

        wrapper = tk.Frame(parent, bg=BG)
        wrapper.pack(fill="both", expand=True, padx=16, pady=12)

        # ── kill-switch row ──
        ks_border = tk.Frame(wrapper, bg=BORDER, padx=1, pady=1)
        ks_border.pack(fill="x", pady=(0, 16))
        ks_card = tk.Frame(ks_border, bg=SURFACE2, padx=20, pady=16)
        ks_card.pack(fill="x")

        ks_left = tk.Frame(ks_card, bg=SURFACE2)
        ks_left.pack(side="left", fill="x", expand=True)
        tk.Label(ks_left, text="Blocking", bg=SURFACE2, fg=ACCENT,
                 font=("Segoe UI", 14, "bold")).pack(anchor="w")
        tk.Label(ks_left,
                 text="When off, all queries pass through unfiltered.",
                 bg=SURFACE2, fg=MUTED, font=("Segoe UI", 9)).pack(anchor="w")

        self._blocking_var = tk.BooleanVar(value=blocking_enabled)
        self._ks_btn = tk.Button(
            ks_card, text="",
            font=("Segoe UI", 10, "bold"),
            relief="flat", padx=20, pady=8, cursor="hand2",
            command=self._toggle_blocking)
        self._ks_btn.pack(side="right")
        self._update_ks_btn()

        # ── pattern editor ──
        tk.Label(wrapper, text="Blocked patterns", bg=BG, fg=MUTED,
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(0, 6))

        editor_border = tk.Frame(wrapper, bg=BORDER, padx=1, pady=1)
        editor_border.pack(fill="both", expand=True)
        editor_card = tk.Frame(editor_border, bg=SURFACE)
        editor_card.pack(fill="both", expand=True)

        # toolbar
        toolbar = tk.Frame(editor_card, bg=SURFACE2, pady=6, padx=8)
        toolbar.pack(fill="x", side="top")
        tk.Label(toolbar,
                 text="One regex per line. Changes apply instantly when saved.",
                 bg=SURFACE2, fg=MUTED, font=("Segoe UI", 8)).pack(side="left")

        save_btn = tk.Button(toolbar, text="Save changes",
                             bg="#ffffff", fg="#000000",
                             font=("Segoe UI", 9, "bold"),
                             relief="flat", padx=14, pady=4,
                             cursor="hand2", command=self._save_patterns)
        save_btn.pack(side="right", padx=(8, 0))

        reset_btn = tk.Button(toolbar, text="Reset to defaults",
                              bg=SURFACE, fg=MUTED,
                              font=("Segoe UI", 9),
                              relief="flat", padx=12, pady=4,
                              cursor="hand2", command=self._reset_patterns)
        reset_btn.pack(side="right")

        # text area
        mono = tkfont.Font(family="Consolas", size=10)
        text_frame = tk.Frame(editor_card, bg=SURFACE)
        text_frame.pack(fill="both", expand=True)

        self._rules_text = tk.Text(
            text_frame, bg=SURFACE, fg=TEXT, font=mono,
            relief="flat", bd=0, insertbackground=TEXT,
            selectbackground="#333333", wrap="none", padx=12, pady=10)
        rules_vsb = SlimScrollbar(text_frame, orient="vertical",
                                  command=self._rules_text.yview)
        self._rules_text.configure(yscrollcommand=rules_vsb.set)
        rules_vsb.pack(side="right", fill="y")
        self._rules_text.pack(fill="both", expand=True)

        # populate with current patterns
        with patterns_lock:
            current = list(BLOCKED_PATTERNS)
        self._rules_text.insert("1.0", "\n".join(current))

        # status label
        self._rules_status = tk.StringVar(value="")
        tk.Label(editor_card, textvariable=self._rules_status,
                 bg=SURFACE, fg=MUTED,
                 font=("Consolas", 8)).pack(anchor="w", padx=12, pady=(0, 6))

    def _toggle_blocking(self):
        global blocking_enabled
        blocking_enabled = not blocking_enabled
        self._blocking_var.set(blocking_enabled)
        self._update_ks_btn()
        state = "ENABLED" if blocking_enabled else "DISABLED"
        log.info("Blocking %s", state)

    def _update_ks_btn(self):
        if not hasattr(self, "_ks_btn"):
            return
        if blocking_enabled:
            self._ks_btn.config(text="Blocking ON",
                                bg="#ffffff", fg="#000000")
        else:
            self._ks_btn.config(text="Blocking OFF",
                                bg="#333333", fg="#888888")

    def _save_patterns(self):
        raw  = self._rules_text.get("1.0", "end").strip()
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
        before = len(BLOCKED_PATTERNS)
        set_patterns(lines)
        after = len(BLOCKED_PATTERNS)
        skipped = len(lines) - after
        msg = f"Saved {after} pattern(s)."
        if skipped:
            msg += f"  ({skipped} invalid regex skipped)"
        self._rules_status.set(msg)
        log.info("Rules updated: %d patterns active", after)

    def _reset_patterns(self):
        defaults = [
            r"\bDROP\s+(DATABASE|SCHEMA|TABLE|VIEW|PROCEDURE|FUNCTION|TRIGGER|EVENT|INDEX)\b",
            r"\bTRUNCATE\b",
            r"\bALTER\s+TABLE\b",
            r"\bRENAME\s+TABLE\b",
            r"\bGRANT\b",
            r"\bREVOKE\b",
            r"\bCREATE\s+USER\b",
            r"\bDROP\s+USER\b",
            r"\bALTER\s+USER\b",
            r"\bFLUSH\s+PRIVILEGES\b",
            r"\bSHUTDOWN\b",
            r"\bRESET\s+MASTER\b",
            r"\bRESET\s+SLAVE\b",
            r"\bPURGE\b",
            r"\bLOAD\s+DATA\s+INFILE\b",
            r"\bINTO\s+OUTFILE\b",
            r"\bINTO\s+DUMPFILE\b",
        ]
        set_patterns(defaults)
        self._rules_text.delete("1.0", "end")
        self._rules_text.insert("1.0", "\n".join(defaults))
        self._rules_status.set(f"Reset to {len(defaults)} default patterns.")
        log.info("Rules reset to defaults.")

    # ══════════════════════════════════════════
    # HEALTH PAGE
    # ══════════════════════════════════════════
    def _build_health(self, parent):
        pad = dict(padx=16, pady=(12, 0))

        # uptime + psutil warning row
        top_row = tk.Frame(parent, bg=BG)
        top_row.pack(fill="x", **pad)

        uptime_border = tk.Frame(top_row, bg=BORDER, padx=1, pady=1)
        uptime_border.pack(side="left")
        uptime_card = tk.Frame(uptime_border, bg=SURFACE2, padx=24, pady=14)
        uptime_card.pack()
        self._uptime_var = tk.StringVar(value="--")
        tk.Label(uptime_card, textvariable=self._uptime_var, bg=SURFACE2,
                 fg=ACCENT, font=("Segoe UI", 28, "bold")).pack(anchor="w")
        tk.Label(uptime_card, text="Uptime", bg=SURFACE2, fg=MUTED,
                 font=("Segoe UI", 9)).pack(anchor="w")
        psutil_status = "psutil active" if HAS_PSUTIL else "psutil not found — pip install psutil"
        psutil_color  = MUTED if HAS_PSUTIL else RED
        tk.Label(uptime_card, text=psutil_status, bg=SURFACE2, fg=psutil_color,
                 font=("Consolas", 8)).pack(anchor="w", pady=(4, 0))

        if not HAS_PSUTIL:
            tk.Label(top_row,
                     text="pip install psutil  for CPU & memory metrics",
                     bg=BG, fg=MUTED, font=("Consolas", 9),
                     padx=14).pack(side="left", padx=(14, 0))

        # chart grid
        charts_outer = tk.Frame(parent, bg=BG)
        charts_outer.pack(fill="both", expand=True, padx=16, pady=12)

        chart_defs = [
            ("cpu",   "#ededed", "CPU",         "%"),
            ("mem",   "#aaaaaa", "Memory",      "MB"),
            ("qps",   "#ffffff", "Queries/sec", ""),
            ("conns", "#888888", "Active conns",""),
        ]
        self._sparklines = {}
        for i, (key, color, label, unit) in enumerate(chart_defs):
            row = i // 2
            col = i  % 2

            cell = tk.Frame(charts_outer, bg=BG)
            cell.grid(row=row, column=col, sticky="nsew", padx=6, pady=6)
            charts_outer.grid_rowconfigure(row, weight=1)
            charts_outer.grid_columnconfigure(col, weight=1)

            tk.Label(cell, text=label, bg=BG, fg=MUTED,
                     font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(0, 4))

            border = tk.Frame(cell, bg=BORDER, padx=1, pady=1)
            border.pack(fill="both", expand=True)
            spark = Sparkline(border, color=color, label=label, unit=unit,
                              bg=SURFACE2)
            spark.pack(fill="both", expand=True)
            self._sparklines[key] = spark

    # ══════════════════════════════════════════
    # PROXY TOGGLE
    def _start_update(self):
        self._update_btn.config(state="disabled", text="Downloading...")
        threading.Thread(target=_do_update,
                         args=(self._pending_sha,), daemon=True).start()

    # ══════════════════════════════════════════
    def _toggle_proxy(self):
        if not self.proxy_running:
            self.toggle_btn.config(state="disabled")

            def progress_cb(msg):
                push({"type": "progress_msg", "msg": msg})

            start_proxy(progress_cb=progress_cb)
        else:
            stop_proxy()
            self.toggle_btn.config(text="\u25b6  Start Proxy",
                                   bg="#ffffff", fg="#000000", state="normal")
            self._set_status(False)

    def _set_status(self, running):
        self.proxy_running = running
        if running:
            self._draw_status_pill(True, f"Running  :3306")
        else:
            self._draw_status_pill(False, "Stopped")

    def _draw_status_pill(self, running, text):
        c = self._status_canvas
        c.delete("all")

        font_obj = ("Segoe UI", 9, "bold" if running else "normal")

        # measure text width roughly
        char_w  = 7 if not running else 7
        text_w  = len(text) * char_w
        dot_w   = 18
        pad_x   = 12
        pad_y   = 4
        height  = 24
        width   = dot_w + text_w + pad_x * 2 + 4

        c.config(width=width)

        bg_color  = "#1a1a1a" if running else "#111111"
        border_c  = "#333333" if running else "#222222"
        dot_color = "#ffffff" if running else "#444444"
        text_c    = "#ededed" if running else "#555555"

        # pill background
        r = height // 2
        c.create_oval(0, 0, height, height, fill=bg_color, outline=border_c, width=1)
        c.create_oval(width-height, 0, width, height, fill=bg_color, outline=border_c, width=1)
        c.create_rectangle(r, 0, width-r, height, fill=bg_color, outline=bg_color)
        # border top/bottom lines
        c.create_line(r, 0, width-r, 0, fill=border_c)
        c.create_line(r, height, width-r, height, fill=border_c)

        # dot
        dot_r = 4
        dot_x = pad_x + dot_r
        dot_y = height // 2
        c.create_oval(dot_x-dot_r, dot_y-dot_r, dot_x+dot_r, dot_y+dot_r,
                      fill=dot_color, outline="")

        # text
        c.create_text(dot_x + dot_r + 6, dot_y,
                      text=text, anchor="w",
                      fill=text_c, font=font_obj)

        # pulse animation when running
        self._status_pill_running = running
        if running:
            self._pulse_frame = 0
            self._animate_pulse()

    def _animate_pulse(self):
        if not self._status_pill_running:
            return
        c = self._status_canvas
        c.delete("pulse")
        # expanding ring around the dot
        f     = self._pulse_frame % 20
        alpha = 1.0 - f / 20.0
        r     = 4 + f * 0.6
        pad_x = 12
        dot_x = pad_x + 4
        dot_y = 12
        grey  = int(60 * alpha)
        color = f"#{grey:02x}{grey:02x}{grey:02x}"
        c.create_oval(dot_x-r, dot_y-r, dot_x+r, dot_y+r,
                      outline=color, width=1, tags="pulse")
        self._pulse_frame += 1
        self.after(50, self._animate_pulse)

    # ══════════════════════════════════════════
    # LOG / TABLE HELPERS
    # ══════════════════════════════════════════
    def _append_log(self, msg, tag="INFO"):
        if not hasattr(self, "log_box"): return
        try:
            if not self.log_box.winfo_exists(): return
        except Exception: return
        self.log_box.config(state="normal")
        self.log_box.insert("end", msg + "\n", tag)
        self.log_box.see("end")
        lines = int(self.log_box.index("end-1c").split(".")[0])
        if lines > 2000:
            self.log_box.delete("1.0", f"{lines-2000}.0")
        self.log_box.config(state="disabled")

    def _add_blocked(self, event):
        if not hasattr(self, "tree"): return
        sql_short  = event["sql"][:60] + ("\u2026" if len(event["sql"]) > 60 else "")
        rule_short = event["rule"].replace(r"\b","").replace(r"\s+"," ")[:30]
        self.tree.insert("", 0, values=(
            event["time"], event["ip"], sql_short, rule_short))
        rows = self.tree.get_children()
        if len(rows) > 200:
            self.tree.delete(rows[-1])


    def _refresh_stats(self):
        if not hasattr(self, "stat_vars"): return
        with stats_lock:
            snap = dict(stats)
        for key, var in self.stat_vars.items():
            var.set(str(snap.get(key, 0)))

    def _refresh_uptime(self):
        global _start_time
        if not hasattr(self, "_uptime_var"): return
        if _start_time:
            elapsed = int(time.time() - _start_time)
            h = elapsed // 3600
            m = (elapsed % 3600) // 60
            s = elapsed % 60
            self._uptime_var.set(f"{h:02d}:{m:02d}:{s:02d}")
        else:
            self._uptime_var.set("Not started")

    def _refresh_sparklines(self):
        if not hasattr(self, "_sparklines"): return
        with health_lock:
            snap = {k: list(v) for k, v in health.items() if k != "_last_total"}
        for key, spark in self._sparklines.items():
            if key in snap:
                spark.update_data(snap[key])

    # ══════════════════════════════════════════
    # POLL LOOP
    # ══════════════════════════════════════════
    def _poll(self):
        dirty_ips = False
        try:
            while True:
                event = ui_queue.get_nowait()
                etype = event.get("type")
                if etype == "log":
                    msg = event.get("msg", "")
                    tag = ("BLOCKED" if "BLOCKED" in msg
                           else "ALLOWED" if "ALLOWED" in msg
                           else event.get("level", "INFO"))
                    self._append_log(msg, tag)
                elif etype == "blocked":
                    self._add_blocked(event)
                    dirty_ips = True
                elif etype == "status":
                    running = event.get("running", False)
                    self._set_status(running)
                    if running:
                        self.toggle_btn.config(
                            text="\u23f9  Stop Proxy", bg="#333333",
                            fg="#ffffff", state="normal")
                    else:
                        self.toggle_btn.config(
                            text="\u25b6  Start Proxy", bg="#ffffff",
                            fg="#000000", state="normal")
                elif etype == "progress_msg":
                    self._draw_status_pill(False, event.get("msg", "")[:28])
                elif etype == "update_available":
                    self._pending_sha = event.get("sha")
                    self._update_msg_var.set(event.get("msg", ""))
                    if not self._update_bar_packed:
                        tk.Frame(self, bg=BORDER, height=1).pack(
                            fill="x", before=self._container)
                        self._update_bar.pack(fill="x", before=self._container)
                        self._update_bar_packed = True
                elif etype == "update_progress":
                    self._update_btn.config(text=event.get("msg", "")[:30])
        except queue.Empty:
            pass

        self._refresh_stats()
        self._refresh_uptime()
        if self._current_page == "health":
            self._refresh_sparklines()

        self.after(150, self._poll)


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────
if __name__ == "__main__":
    app = Dashboard()
    app._poll()
    app.mainloop()
