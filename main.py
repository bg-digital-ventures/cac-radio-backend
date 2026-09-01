```python
import os
import secrets
import socket
import subprocess
import threading
import time
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

def mount_for(
    branch_id: str
) -> str:

    return ICECAST_MOUNT


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
            ICECAST_MOUNT,

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

    hq_relay_branch = (
        body.branchId
    )

    stream_url = public_stream_url(
        session.mount
    )

    print(
        f"HQ relay connected to {body.branchId}",
        flush=True
    )

    return {

        "ok":
            True,

        "branchId":
            body.branchId,

        "publicStreamUrl":
            stream_url,

        "mount":
            session.mount,

        "message":
            "Branch connected to Headquarters output."
    }


# =========================================================
# DISCONNECT HQ
# =========================================================

@app.post("/api/live/disconnect-hq")
async def disconnect_hq():

    global hq_relay_branch

    hq_relay_branch = None

    print(
        "HQ relay disconnected.",
        flush=True
    )

    return {

        "ok":
            True,

        "message":
            "HQ relay disconnected."
    }
```
