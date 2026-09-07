import os
import secrets
import socket
import subprocess
import threading
import time
import json
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import quote

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import firebase_admin
from firebase_admin import credentials, firestore


# =========================================================
# ENVIRONMENT
# =========================================================

load_dotenv()


# =========================================================
# APP
# =========================================================

app = FastAPI(title="CAC Radio Live Backend")


# =========================================================
# FIREBASE ADMIN / FIRESTORE
# =========================================================

FIREBASE_CREDENTIALS = "/etc/secrets/firebase-service-account.json"

if not firebase_admin._apps:

    if not os.path.exists(FIREBASE_CREDENTIALS):
        raise RuntimeError(
            f"Firebase service account file not found: "
            f"{FIREBASE_CREDENTIALS}"
        )

    cred = credentials.Certificate(
        FIREBASE_CREDENTIALS
    )

    firebase_admin.initialize_app(cred)


firestore_db = firestore.client()


# =========================================================
# CORS
# =========================================================

cors_origins = os.getenv(
    "CORS_ORIGINS",
    "https://cac-radio-frontend.vercel.app"
)

origins = [
    origin.strip()
    for origin in cors_origins.split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# CASTER.FM / ICECAST SETTINGS
# =========================================================

ICECAST_HOST = os.getenv(
    "ICECAST_HOST",
    "sapircast.caster.fm"
)

ICECAST_PORT = os.getenv(
    "ICECAST_PORT",
    "19269"
)

ICECAST_SOURCE_USER = os.getenv(
    "ICECAST_SOURCE_USER",
    "source"
)

ICECAST_SOURCE_PASSWORD = os.getenv(
    "ICECAST_SOURCE_PASSWORD",
    ""
)

ICECAST_MOUNT = os.getenv(
    "ICECAST_MOUNT",
    "/vnFKR"
)

PUBLIC_BASE = os.getenv(
    "ICECAST_PUBLIC_BASE",
    ""
).rstrip("/")


# =========================================================
# SESSION
# =========================================================

@dataclass
class Session:

    branch_id: str

    branch_name: str

    token: str

    mount: str

    broadcast_id: Optional[str] = None

    process: Optional[subprocess.Popen] = None

    stopping: bool = False

    ffmpeg_error: Optional[str] = None

    started_at: float = 0.0


sessions: Dict[str, Session] = {}

hq_relay_branch: Optional[str] = None
hq_relay_process: Optional[subprocess.Popen] = None
hq_relay_lock = threading.Lock()


# =========================================================
# REQUEST MODELS
# =========================================================

class StartRequest(BaseModel):

    branchId: str

    branchName: str

    title: str

    presenter: Optional[str] = None

    programmeId: Optional[str] = None


class StopRequest(BaseModel):

    branchId: str

    broadcastId: Optional[str] = None


class ConnectRequest(BaseModel):

    branchId: str


# =========================================================
# FIRESTORE
# =========================================================

def mark_broadcast_ended(
    broadcast_id: Optional[str]
):

    if not broadcast_id:
        return

    try:

        firestore_db.collection(
            "broadcasts"
        ).document(
            broadcast_id
        ).update({

            "status": "ended",

            "updatedAt":
                firestore.SERVER_TIMESTAMP

        })

        print(
            f"Broadcast {broadcast_id} marked as ended.",
            flush=True
        )

    except Exception as exc:

        print(
            f"Unable to update broadcast "
            f"{broadcast_id}: {exc}",
            flush=True
        )


# =========================================================
# MOUNT
# =========================================================

# Optional per-branch mount mapping. Example:
# BRANCH_MOUNTS_JSON={"branch-a":"/branchA","branch-b":"/branchB"}
try:
    BRANCH_MOUNTS = json.loads(os.getenv("BRANCH_MOUNTS_JSON", "{}"))
    if not isinstance(BRANCH_MOUNTS, dict):
        BRANCH_MOUNTS = {}
except Exception:
    BRANCH_MOUNTS = {}

ICECAST_HQ_MOUNT = os.getenv("ICECAST_HQ_MOUNT", "/main")
HQ_RELAY_INPUT_BASE = os.getenv("HQ_RELAY_INPUT_BASE", PUBLIC_BASE).rstrip("/")
HQ_RELAY_SCHEME = os.getenv("HQ_RELAY_SCHEME", "https")


def normalize_mount(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return "/"
    return value if value.startswith("/") else "/" + value


def mount_for(branch_id: str) -> str:
    configured = BRANCH_MOUNTS.get(branch_id)
    if configured:
        return normalize_mount(configured)
    # Keep the legacy mount for a single-branch deployment.
    return normalize_mount(ICECAST_MOUNT)


def hq_mount() -> str:
    return normalize_mount(ICECAST_HQ_MOUNT)


# =========================================================
# PUBLIC STREAM
# =========================================================

def public_stream_url(
    mount: str
) -> str:

    if PUBLIC_BASE:

        return f"{PUBLIC_BASE}{mount}"

    return mount


# =========================================================
# CASTER CONFIG
# =========================================================

def caster_config():

    return {

        "host":
            ICECAST_HOST,

        "port":
            int(ICECAST_PORT),

        "username":
            ICECAST_SOURCE_USER,

        "mount":
            normalize_mount(ICECAST_MOUNT),

        "protocol":
            "icecast",

        "codec":
            "mp3",

        "bitrate":
            "96k"
    }


# =========================================================
# FFMPEG COMMAND
# =========================================================

def ffmpeg_cmd(
    mount: str
):

    if not ICECAST_HOST:

        raise RuntimeError(
            "ICECAST_HOST is not configured."
        )

    if not ICECAST_SOURCE_PASSWORD:

        raise RuntimeError(
            "ICECAST_SOURCE_PASSWORD is not configured."
        )

    safe_user = quote(
        ICECAST_SOURCE_USER,
        safe=""
    )

    safe_password = quote(
        ICECAST_SOURCE_PASSWORD,
        safe=""
    )

    target = (
        f"icecast://"
        f"{safe_user}:"
        f"{safe_password}@"
        f"{ICECAST_HOST}:"
        f"{ICECAST_PORT}"
        f"{mount}"
    )

    return [

        "ffmpeg",

        "-hide_banner",

        # IMPORTANT:
        # Use INFO while debugging.
        "-loglevel",
        "info",

        # =================================================
        # INPUT
        # =================================================

        "-f",
        "webm",

        "-i",
        "pipe:0",

        # =================================================
        # AUDIO
        # =================================================

        "-vn",

        "-ac",
        "2",

        "-ar",
        "44100",

        "-b:a",
        "96k",

        # =================================================
        # ICECAST METADATA
        # =================================================

        "-content_type",
        "audio/mpeg",

        "-ice_name",
        "CAC Radio Live",

        "-ice_description",
        "CAC Radio Live Broadcast",

        "-ice_genre",
        "Christian Radio",

        "-ice_public",
        "1",

        # =================================================
        # OUTPUT
        # =================================================

        "-f",
        "mp3",

        target
    ]


# =========================================================
# FFMPEG LOGGING
# =========================================================

def log_ffmpeg_output(
    process,
    session: Session
):

    branch_id = session.branch_id

    try:

        if process.stderr is None:
            return

        for raw_line in iter(
            process.stderr.readline,
            b""
        ):

            if not raw_line:
                break

            text = raw_line.decode(
                errors="replace"
            ).rstrip()

            if not text:
                continue

            print(
                f"[FFmpeg {branch_id}] {text}",
                flush=True
            )

            session.ffmpeg_error = text

    except Exception as exc:

        print(
            f"FFmpeg logger error for "
            f"{branch_id}: {exc}",
            flush=True
        )


# =========================================================
# STOP FFMPEG
# =========================================================

def stop_ffmpeg(
    process
):

    if process is None:
        return

    try:

        if process.stdin:

            try:
                process.stdin.close()
            except Exception:
                pass

        if process.poll() is None:

            process.terminate()

            try:

                process.wait(
                    timeout=5
                )

            except subprocess.TimeoutExpired:

                print(
                    "FFmpeg did not terminate. Killing process.",
                    flush=True
                )

                process.kill()

                try:
                    process.wait(
                        timeout=2
                    )
                except Exception:
                    pass

    except Exception as exc:

        print(
            f"Unable to stop FFmpeg: {exc}",
            flush=True
        )


# =========================================================
# CLEANUP
# =========================================================

def cleanup_session(
    branch_id: str,
    session: Optional[Session] = None
):

    if session is None:

        session = sessions.get(
            branch_id
        )

    if session is None:
        return

    session.stopping = True

    process = session.process

    session.process = None

    if process:

        stop_ffmpeg(
            process
        )

    mark_broadcast_ended(
        session.broadcast_id
    )

    global hq_relay_branch
    if hq_relay_branch == branch_id:
        stop_hq_relay()

    current = sessions.get(
        branch_id
    )

    if current is session:

        sessions.pop(
            branch_id,
            None
        )

    print(
        f"Live session cleaned up for {branch_id}.",
        flush=True
    )


# =========================================================
# HQ RELAY
# =========================================================

def branch_input_url(session: Session) -> str:
    """URL that FFmpeg uses to pull a live branch into HQ.

    Caster.fm Free may protect direct stream URLs. In that case set
    HQ_RELAY_INPUT_BASE to an authorized/direct input endpoint, or use a
    streaming provider that permits server-side pulling.
    """
    if HQ_RELAY_INPUT_BASE:
        return f"{HQ_RELAY_INPUT_BASE}{session.mount}"
    return f"{HQ_RELAY_SCHEME}://{ICECAST_HOST}:{ICECAST_PORT}{session.mount}"


def hq_relay_cmd(session: Session):
    if not ICECAST_SOURCE_PASSWORD:
        raise RuntimeError("ICECAST_SOURCE_PASSWORD is not configured.")

    safe_user = quote(ICECAST_SOURCE_USER, safe="")
    safe_password = quote(ICECAST_SOURCE_PASSWORD, safe="")
    output = (
        f"icecast://{safe_user}:{safe_password}@"
        f"{ICECAST_HOST}:{ICECAST_PORT}{hq_mount()}"
    )

    return [
        "ffmpeg", "-hide_banner", "-loglevel", "info",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", branch_input_url(session),
        "-vn", "-c:a", "libmp3lame",
        "-b:a", "96k", "-ar", "44100", "-ac", "2",
        "-content_type", "audio/mpeg",
        "-ice_name", "CAC Radio HQ",
        "-ice_description", "CAC Radio Headquarters Feed",
        "-ice_genre", "Christian Radio",
        "-ice_public", "1",
        "-f", "mp3", output
    ]


def stop_hq_relay():
    global hq_relay_process, hq_relay_branch
    with hq_relay_lock:
        process = hq_relay_process
        hq_relay_process = None
        hq_relay_branch = None

    if process is not None:
        stop_ffmpeg(process)
        print("HQ relay stopped.", flush=True)


def log_hq_relay(process):
    try:
        if process.stderr is None:
            return
        for raw_line in iter(process.stderr.readline, b""):
            if not raw_line:
                break
            text = raw_line.decode(errors="replace").rstrip()
            if text:
                print(f"[FFmpeg HQ] {text}", flush=True)
    except Exception as exc:
        print(f"HQ relay logger error: {exc}", flush=True)


def start_hq_relay(session: Session):
    global hq_relay_process, hq_relay_branch

    # A previous relay must be stopped before switching branches.
    stop_hq_relay()

    command = hq_relay_cmd(session)
    print(
        f"Starting HQ relay from {session.branch_id} "
        f"({session.mount}) -> {hq_mount()}",
        flush=True
    )

    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        bufsize=0,
    )

    with hq_relay_lock:
        hq_relay_process = process
        hq_relay_branch = session.branch_id

    threading.Thread(
        target=log_hq_relay, args=(process,), daemon=True
    ).start()

    # Give FFmpeg a moment to fail fast on bad input/output credentials.
    time.sleep(0.35)
    if process.poll() is not None:
        with hq_relay_lock:
            hq_relay_process = None
            hq_relay_branch = None
        raise RuntimeError(
            f"HQ relay FFmpeg exited immediately with code {process.returncode}. "
            "Check the branch input URL, Caster mount, and HQ mount/password."
        )

    return process


# =========================================================
# ROOT
# =========================================================

@app.get("/")
async def root():

    return {

        "service":
            "CAC Radio Live Backend",

        "status":
            "online"
    }


# =========================================================
# HEALTH
# =========================================================

@app.get("/api/health")
async def health():

    live = {}

    for branch_id, session in sessions.items():

        process_alive = (
            session.process is not None
            and session.process.poll() is None
        )

        live[branch_id] = {

            "branchName":
                session.branch_name,

            "broadcastId":
                session.broadcast_id,

            "ffmpegAlive":
                process_alive,

            "startedAt":
                session.started_at,

            "ffmpegError":
                session.ffmpeg_error
        }

    return {

        "ok":
            True,

        "liveBranches":
            list(sessions.keys()),

        "sessions":
            live,

        "hqRelayBranch":
            hq_relay_branch,

        "hqRelayAlive":
            hq_relay_process is not None
            and hq_relay_process.poll() is None,

        "hqMount":
            hq_mount(),

        "caster":
            {

                "host":
                    ICECAST_HOST,

                "port":
                    int(ICECAST_PORT),

                "mount":
                    ICECAST_MOUNT
            }
    }


# =========================================================
# TEST CASTER CONNECTION
# =========================================================

@app.get("/api/test-icecast")
async def test_icecast():

    try:

        sock = socket.create_connection(

            (
                ICECAST_HOST,
                int(ICECAST_PORT)
            ),

            timeout=10
        )

        sock.close()

        return {

            "ok":
                True,

            "message":
                "Render can reach Caster.fm",

            "host":
                ICECAST_HOST,

            "port":
                int(ICECAST_PORT)
        }

    except Exception as exc:

        return {

            "ok":
                False,

            "message":
                str(exc),

            "host":
                ICECAST_HOST,

            "port":
                int(ICECAST_PORT)
        }


# =========================================================
# CASTER CONFIG
# =========================================================

@app.get("/api/caster/config")
async def get_caster_config():

    return {

        "ok":
            True,

        "caster":
            caster_config()
    }


# =========================================================
# START
# =========================================================

@app.post("/api/live/start")
async def start_live(
    body: StartRequest
):

    existing = sessions.get(
        body.branchId
    )

    if existing:

        raise HTTPException(

            status_code=409,

            detail="Branch is already live."
        )

    token = secrets.token_urlsafe(
        24
    )

    mount = mount_for(
        body.branchId
    )

    # Never allow two active branches to publish to the same Icecast mount.
    for active in sessions.values():
        if active.mount == mount:
            raise HTTPException(
                status_code=409,
                detail=f"Icecast mount {mount} is already in use by another live branch."
            )

    if mount == hq_mount():
        raise HTTPException(
            status_code=409,
            detail="A branch mount cannot be the same as the HQ output mount."
        )

    session = Session(

        branch_id=
            body.branchId,

        branch_name=
            body.branchName,

        token=
            token,

        mount=
            mount,

        started_at=
            time.time()
    )

    sessions[
        body.branchId
    ] = session

    stream_url = public_stream_url(
        mount
    )

    print(
        f"Live session prepared for {body.branchId}",
        flush=True
    )

    return {

        "ok":
            True,

        "sessionToken":
            token,

        "branchId":
            body.branchId,

        "branchName":
            body.branchName,

        "mount":
            mount,

        "publicStreamUrl":
            stream_url,

        "caster":
            caster_config(),

        "message":
            "Live session ready. Connect the browser microphone."
    }


# =========================================================
# GET SESSION
# =========================================================

@app.get(
    "/api/live/session/{branch_id}"
)
async def get_live_session(
    branch_id: str
):

    session = sessions.get(
        branch_id
    )

    if not session:

        return {

            "ok":
                True,

            "live":
                False,

            "branchId":
                branch_id
        }

    process_alive = (
        session.process is not None
        and session.process.poll() is None
    )

    return {

        "ok":
            True,

        "live":
            True,

        "branchId":
            session.branch_id,

        "branchName":
            session.branch_name,

        "mount":
            session.mount,

        "broadcastId":
            session.broadcast_id,

        "ffmpegAlive":
            process_alive,

        "ffmpegError":
            session.ffmpeg_error,

        "publicStreamUrl":
            public_stream_url(
                session.mount
            )
    }


# =========================================================
# LIVE WEBSOCKET
# =========================================================

@app.websocket(
    "/ws/live/{branch_id}"
)
async def live_ws(

    websocket: WebSocket,

    branch_id: str,

    token: Optional[str] = None,

    broadcastId: Optional[str] = None

):

    print(
        f"WebSocket connection requested "
        f"for branch={branch_id}",
        flush=True
    )

    session = sessions.get(
        branch_id
    )

    if session is None:

        await websocket.accept()

        await websocket.close(
            code=4404
        )

        return

    await websocket.accept()

    if not token:

        await websocket.close(
            code=4403
        )

        return

    if not secrets.compare_digest(
        session.token,
        token
    ):

        await websocket.close(
            code=4403
        )

        return

    if broadcastId:

        session.broadcast_id = (
            broadcastId
        )

    print(
        f"WebSocket authenticated for {branch_id}",
        flush=True
    )

    process = None

    try:

        # =================================================
        # START FFMPEG
        # =================================================

        command = ffmpeg_cmd(
            session.mount
        )

        print(
            f"Starting FFmpeg for {branch_id}",
            flush=True
        )

        process = subprocess.Popen(

            command,

            stdin=subprocess.PIPE,

            stdout=subprocess.DEVNULL,

            stderr=subprocess.PIPE,

            bufsize=0
        )

        session.process = process

        threading.Thread(

            target=log_ffmpeg_output,

            args=(
                process,
                session
            ),

            daemon=True

        ).start()

        print(
            f"FFmpeg started for {branch_id}",
            flush=True
        )

        # =================================================
        # RECEIVE AUDIO
        # =================================================

        while True:

            # ---------------------------------------------
            # Check FFmpeg before waiting for browser data
            # ---------------------------------------------

            return_code = process.poll()

            if return_code is not None:

                error_message = (
                    session.ffmpeg_error
                    or
                    f"FFmpeg exited with code {return_code}"
                )

                print(
                    f"FFmpeg exited for "
                    f"{branch_id}: {error_message}",
                    flush=True
                )

                raise RuntimeError(
                    error_message
                )

            # ---------------------------------------------
            # Receive browser audio
            # ---------------------------------------------

            chunk = (
                await websocket.receive_bytes()
            )

            if not chunk:
                continue

            # ---------------------------------------------
            # Check again
            # ---------------------------------------------

            if process.poll() is not None:

                error_message = (
                    session.ffmpeg_error
                    or
                    "FFmpeg stopped unexpectedly."
                )

                raise RuntimeError(
                    error_message
                )

            # ---------------------------------------------
            # Write to FFmpeg
            # ---------------------------------------------

            if process.stdin is None:

                raise RuntimeError(
                    "FFmpeg stdin is unavailable."
                )

            try:

                process.stdin.write(
                    chunk
                )

                process.stdin.flush()

            except BrokenPipeError:

                error_message = (
                    session.ffmpeg_error
                    or
                    "FFmpeg input pipe closed."
                )

                raise RuntimeError(
                    error_message
                )

    except WebSocketDisconnect:

        print(
            f"Browser WebSocket disconnected "
            f"for {branch_id}.",
            flush=True
        )

    except Exception as exc:

        print(
            f"Live stream error for "
            f"{branch_id}: {exc}",
            flush=True
        )

    finally:

        cleanup_session(
            branch_id,
            session
        )


# =========================================================
# STOP
# =========================================================

@app.post("/api/live/stop")
async def stop_live(
    body: StopRequest
):

    session = sessions.get(
        body.branchId
    )

    if session:

        if body.broadcastId:

            session.broadcast_id = (
                body.broadcastId
            )

        print(
            f"Stopping live session for "
            f"{body.branchId}",
            flush=True
        )

        cleanup_session(
            body.branchId,
            session
        )

    else:

        print(
            f"No active backend session for "
            f"{body.branchId}.",
            flush=True
        )

        mark_broadcast_ended(
            body.broadcastId
        )

    return {

        "ok":
            True,

        "branchId":
            body.branchId,

        "message":
            "Live session stopped."
    }


# =========================================================
# CONNECT HQ
# =========================================================

@app.post("/api/live/connect-hq")
async def connect_hq(
    body: ConnectRequest
):

    global hq_relay_branch

    session = sessions.get(
        body.branchId
    )

    if not session:
        raise HTTPException(
            status_code=404,
            detail="That branch is not live."
        )

    if session.process is None or session.process.poll() is not None:
        raise HTTPException(
            status_code=409,
            detail="That branch is live in Firestore but its audio publisher is not connected."
        )

    try:
        start_hq_relay(session)
    except Exception as exc:
        print(f"Unable to connect HQ to {body.branchId}: {exc}", flush=True)
        raise HTTPException(
            status_code=502,
            detail=f"HQ relay could not start: {exc}"
        )

    stream_url = public_stream_url(hq_mount())

    print(
        f"HQ relay connected to {body.branchId}.",
        flush=True
    )

    return {
        "ok": True,
        "branchId": body.branchId,
        "sourceMount": session.mount,
        "mount": hq_mount(),
        "publicStreamUrl": stream_url,
        "message": "Branch audio is now being relayed to the Headquarters output."
    }


# =========================================================
# DISCONNECT HQ
# =========================================================

@app.post("/api/live/disconnect-hq")
async def disconnect_hq():

    stop_hq_relay()

    print(
        "HQ relay disconnected.",
        flush=True
    )

    return {
        "ok": True,
        "message": "HQ relay disconnected."
    }
