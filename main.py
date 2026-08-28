import os
import secrets
import subprocess
from typing import Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

app = FastAPI(title="CAC Radio Live Backend")

# --------------------------------------------------
# CORS
# --------------------------------------------------

cors_origins = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5500"
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

# --------------------------------------------------
# ICECAST / CASTER.FM SETTINGS
# --------------------------------------------------

ICECAST_HOST = os.getenv("ICECAST_HOST", "")
ICECAST_PORT = os.getenv("ICECAST_PORT", "8000")
ICECAST_USER = os.getenv("ICECAST_SOURCE_USER", "source")
ICECAST_PASSWORD = os.getenv("ICECAST_SOURCE_PASSWORD", "")
ICECAST_PUBLIC_BASE = os.getenv(
    "ICECAST_PUBLIC_BASE",
    ""
).rstrip("/")


# --------------------------------------------------
# LIVE SESSION
# --------------------------------------------------

class LiveSession:
    def __init__(
        self,
        branch_id: str,
        branch_name: str,
        token: str,
        mount: str
    ):
        self.branch_id = branch_id
        self.branch_name = branch_name
        self.token = token
        self.mount = mount
        self.process: Optional[subprocess.Popen] = None


sessions: Dict[str, LiveSession] = {}

hq_relay_branch: Optional[str] = None


# --------------------------------------------------
# REQUEST MODELS
# --------------------------------------------------

class StartRequest(BaseModel):
    branchId: str
    branchName: str
    title: str = ""
    presenter: Optional[str] = None
    programmeId: Optional[str] = None


class StopRequest(BaseModel):
    branchId: str
    broadcastId: Optional[str] = None


class ConnectRequest(BaseModel):
    branchId: str


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def mount_for(branch_id: str) -> str:
    """
    Creates a safe Icecast mount point from the branch ID.
    """

    safe = "".join(
        character
        for character in branch_id
        if character.isalnum() or character in "-_"
    )

    if not safe:
        safe = "branch"

    return "/" + safe + ".mp3"


def public_stream_url(mount: str) -> str:
    """
    Creates the public listening URL.
    """

    if ICECAST_PUBLIC_BASE:
        return ICECAST_PUBLIC_BASE + mount

    return mount


def ffmpeg_command(mount: str):
    """
    Creates the FFmpeg command used to send browser audio
    to the Icecast/Caster.fm server.
    """

    if not ICECAST_HOST:
        raise RuntimeError(
            "ICECAST_HOST is not configured."
        )

    if not ICECAST_PASSWORD:
        raise RuntimeError(
            "ICECAST_SOURCE_PASSWORD is not configured."
        )

    target = (
        "icecast://"
        + ICECAST_USER
        + ":"
        + ICECAST_PASSWORD
        + "@"
        + ICECAST_HOST
        + ":"
        + ICECAST_PORT
        + mount
    )

    return [
        "ffmpeg",

        "-hide_banner",
        "-loglevel",
        "warning",

        "-f",
        "webm",

        "-i",
        "pipe:0",

        "-vn",

        "-ac",
        "2",

        "-ar",
        "44100",

        "-b:a",
        "96k",

        "-content_type",
        "audio/mpeg",

        "-f",
        "mp3",

        target,
    ]


# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------

@app.get("/")
async def root():
    return {
        "name": "CAC Radio Live Backend",
        "status": "running"
    }


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "liveBranches": list(sessions.keys()),
        "hqRelayBranch": hq_relay_branch
    }


# --------------------------------------------------
# START LIVE
# --------------------------------------------------

@app.post("/api/live/start")
async def start_live(body: StartRequest):

    if body.branchId in sessions:
        raise HTTPException(
            status_code=409,
            detail="Branch is already live."
        )

    token = secrets.token_urlsafe(24)

    mount = mount_for(body.branchId)

    session = LiveSession(
        branch_id=body.branchId,
        branch_name=body.branchName,
        token=token,
        mount=mount
    )

    sessions[body.branchId] = session

    return {
        "ok": True,
        "branchId": body.branchId,
        "branchName": body.branchName,
        "sessionToken": token,
        "mount": mount,
        "publicStreamUrl": public_stream_url(mount)
    }


# --------------------------------------------------
# LIVE AUDIO WEBSOCKET
# --------------------------------------------------

@app.websocket("/ws/live/{branch_id}")
async def live_websocket(
    websocket: WebSocket,
    branch_id: str,
    token: str
):

    session = sessions.get(branch_id)

    if session is None:
        await websocket.close(code=4404)
        return

    if not secrets.compare_digest(
        session.token,
        token
    ):
        await websocket.close(code=4403)
        return

    await websocket.accept()

    process = None

    try:

        command = ffmpeg_command(
            session.mount
        )

        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE
        )

        session.process = process

        while True:

            audio_chunk = await websocket.receive_bytes()

            if process.poll() is not None:
                raise RuntimeError(
                    "FFmpeg stopped."
                )

            if process.stdin is not None:
                process.stdin.write(
                    audio_chunk
                )
                process.stdin.flush()

    except WebSocketDisconnect:
        pass

    except Exception as error:
        print(
            "Live streaming error:",
            error
        )

    finally:

        if process is not None:

            if process.stdin is not None:
                try:
                    process.stdin.close()
                except Exception:
                    pass

            try:
                process.terminate()
            except Exception:
                pass

        session.process = None

        sessions.pop(
            branch_id,
            None
        )


# --------------------------------------------------
# STOP LIVE
# --------------------------------------------------

@app.post("/api/live/stop")
async def stop_live(body: StopRequest):

    session = sessions.get(
        body.branchId
    )

    if session is not None:

        if session.process is not None:

            try:
                session.process.terminate()
            except Exception:
                pass

    sessions.pop(
        body.branchId,
        None
    )

    return {
        "ok": True,
        "branchId": body.branchId
    }


# --------------------------------------------------
# HQ CONNECT TO BRANCH
# --------------------------------------------------

@app.post("/api/live/connect-hq")
async def connect_hq(
    body: ConnectRequest
):

    global hq_relay_branch

    session = sessions.get(
        body.branchId
    )

    if session is None:
        raise HTTPException(
            status_code=404,
            detail="That branch is not live."
        )

    hq_relay_branch = body.branchId

    return {
        "ok": True,
        "branchId": body.branchId,
        "publicStreamUrl": public_stream_url(
            session.mount
        )
    }


# --------------------------------------------------
# HQ DISCONNECT
# --------------------------------------------------

@app.post("/api/live/disconnect-hq")
async def disconnect_hq():

    global hq_relay_branch

    hq_relay_branch = None

    return {
        "ok": True
    }