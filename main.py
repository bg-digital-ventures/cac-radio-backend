import os
import secrets
import subprocess
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import quote

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

app = FastAPI(title="CAC Radio Live Backend")


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

ICECAST_HOST = os.getenv("ICECAST_HOST", "sapircast.caster.fm")
ICECAST_PORT = os.getenv("ICECAST_PORT", "19269")
ICECAST_SOURCE_USER = os.getenv("ICECAST_SOURCE_USER", "source")
ICECAST_SOURCE_PASSWORD = os.getenv("ICECAST_SOURCE_PASSWORD", "")

# Your Caster.fm mount point
ICECAST_MOUNT = os.getenv("ICECAST_MOUNT", "/vnFKR")

# Optional public stream base URL
PUBLIC_BASE = os.getenv("ICECAST_PUBLIC_BASE", "").rstrip("/")


# =========================================================
# LIVE SESSION STORAGE
# =========================================================

@dataclass
class Session:
    branch_id: str
    branch_name: str
    token: str
    mount: str
    process: Optional[subprocess.Popen] = None


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
# MOUNT POINT
# =========================================================

def mount_for(branch_id: str) -> str:
    """
    All branches currently broadcast through the Caster.fm
    mount point configured in ICECAST_MOUNT.
    """

    return ICECAST_MOUNT


# =========================================================
# FFMPEG COMMAND
# =========================================================

def ffmpeg_cmd(mount: str):

    if not ICECAST_HOST:
        raise RuntimeError("ICECAST_HOST is not configured.")

    if not ICECAST_SOURCE_PASSWORD:
        raise RuntimeError(
            "ICECAST_SOURCE_PASSWORD is not configured."
        )

    # Escape credentials in case the password contains
    # characters such as @, :, /, #, etc.
    safe_user = quote(ICECAST_SOURCE_USER, safe="")
    safe_password = quote(ICECAST_SOURCE_PASSWORD, safe="")

    target = (
        f"icecast://"
        f"{safe_user}:{safe_password}@"
        f"{ICECAST_HOST}:{ICECAST_PORT}"
        f"{mount}"
    )

    return [
        "ffmpeg",

        "-hide_banner",
        "-loglevel",
        "warning",

        # Browser sends WebM/Opus
        "-f",
        "webm",

        "-i",
        "pipe:0",

        # Audio only
        "-vn",

        # Stereo
        "-ac",
        "2",

        # 44.1 kHz
        "-ar",
        "44100",

        # Radio bitrate
        "-b:a",
        "96k",

        # MP3 stream
        "-content_type",
        "audio/mpeg",

        "-f",
        "mp3",

        target,
    ]


# =========================================================
# HEALTH CHECK
# =========================================================

@app.get("/")
async def root():
    return {
        "service": "CAC Radio Live Backend",
        "status": "online",
    }


@app.get("/api/health")
async def health():

    return {
        "ok": True,
        "liveBranches": list(sessions.keys()),
        "hqRelayBranch": hq_relay_branch,
    }

@app.get("/api/test-icecast")
async def test_icecast():
    import socket

    try:
        sock = socket.create_connection(
            (ICECAST_HOST, int(ICECAST_PORT)),
            timeout=10
        )
        sock.close()

        return {
            "ok": True,
            "message": "Render can reach Caster.fm",
            "host": ICECAST_HOST,
            "port": ICECAST_PORT
        }

    except Exception as e:
        return {
            "ok": False,
            "message": str(e),
            "host": ICECAST_HOST,
            "port": ICECAST_PORT
        }
        

# =========================================================
# START LIVE
# =========================================================

@app.post("/api/live/start")
async def start_live(body: StartRequest):

    if body.branchId in sessions:
        raise HTTPException(
            status_code=409,
            detail="Branch is already live."
        )

    token = secrets.token_urlsafe(24)

    mount = mount_for(body.branchId)

    session = Session(
        branch_id=body.branchId,
        branch_name=body.branchName,
        token=token,
        mount=mount,
    )

    sessions[body.branchId] = session

    public_stream_url = (
        f"{PUBLIC_BASE}{mount}"
        if PUBLIC_BASE
        else mount
    )

    return {
        "ok": True,
        "sessionToken": token,
        "mount": mount,
        "publicStreamUrl": public_stream_url,
    }


# =========================================================
# LIVE MICROPHONE WEBSOCKET
# =========================================================

@app.websocket("/ws/live/{branch_id}")
async def live_ws(
    websocket: WebSocket,
    branch_id: str,
    token: str,
):

    session = sessions.get(branch_id)

    # Check session/token before accepting connection
    if not session:
        await websocket.close(code=4404)
        return

    if not secrets.compare_digest(session.token, token):
        await websocket.close(code=4403)
        return

    await websocket.accept()

    process = None

    try:

        # Start FFmpeg
        process = subprocess.Popen(
    ffmpeg_cmd(session.mount),
    stdin=subprocess.PIPE,
    stdout=subprocess.DEVNULL,
    stderr=None,
)

        session.process = process

        while True:

            # Receive microphone audio from browser
            chunk = await websocket.receive_bytes()

            if process.poll() is not None:
                raise RuntimeError(
                    "FFmpeg stopped unexpectedly."
                )

            if process.stdin is None:
                raise RuntimeError(
                    "FFmpeg input is unavailable."
                )

            process.stdin.write(chunk)
            process.stdin.flush()

    except WebSocketDisconnect:

        # Browser closed connection normally
        pass

    except Exception as exc:

        print(
            f"Live stream error for {branch_id}: {exc}",
            flush=True
        )

    finally:

        # Close FFmpeg input
        if process is not None:

            try:
                if process.stdin:
                    process.stdin.close()
            except Exception:
                pass

            try:
                process.terminate()
                process.wait(timeout=5)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass

        session.process = None

        # Remove session
        sessions.pop(branch_id, None)


# =========================================================
# STOP LIVE
# =========================================================

@app.post("/api/live/stop")
async def stop_live(body: StopRequest):

    session = sessions.get(body.branchId)

    if session and session.process:

        try:
            session.process.terminate()
        except Exception:
            pass

    sessions.pop(body.branchId, None)

    return {
        "ok": True
    }


# =========================================================
# CONNECT BRANCH TO HQ
# =========================================================

@app.post("/api/live/connect-hq")
async def connect_hq(body: ConnectRequest):

    global hq_relay_branch

    session = sessions.get(body.branchId)

    if not session:
        raise HTTPException(
            status_code=404,
            detail="That branch is not live."
        )

    hq_relay_branch = body.branchId

    public_stream_url = (
        f"{PUBLIC_BASE}{session.mount}"
        if PUBLIC_BASE
        else session.mount
    )

    return {
        "ok": True,
        "branchId": body.branchId,
        "publicStreamUrl": public_stream_url,
    }


# =========================================================
# DISCONNECT HQ
# =========================================================

@app.post("/api/live/disconnect-hq")
async def disconnect_hq():

    global hq_relay_branch

    hq_relay_branch = None

    return {
        "ok": True
    }
