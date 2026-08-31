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

import firebase_admin
from firebase_admin import credentials, firestore


load_dotenv()

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

    cred = credentials.Certificate(FIREBASE_CREDENTIALS)
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
# LIVE SESSION STORAGE
# =========================================================

@dataclass
class Session:
    branch_id: str
    branch_name: str
    token: str
    mount: str
    broadcast_id: Optional[str] = None
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
# FIRESTORE HELPERS
# =========================================================

def mark_broadcast_ended(broadcast_id: Optional[str]):
    """
    Mark the Firestore broadcast as ended.

    This is called when:
    - the broadcaster presses Stop Live
    - the browser/WebSocket disconnects
    - FFmpeg fails
    """

    if not broadcast_id:
        return

    try:
        firestore_db.collection("broadcasts").document(
            broadcast_id
        ).update({
            "status": "ended",
            "updatedAt": firestore.SERVER_TIMESTAMP,
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
# MOUNT POINT
# =========================================================

def mount_for(branch_id: str) -> str:
    """
    All branches currently use the same Caster.fm
    mount point.
    """

    return ICECAST_MOUNT


# =========================================================
# FFMPEG COMMAND
# =========================================================

def ffmpeg_cmd(mount: str):

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
            (
                ICECAST_HOST,
                int(ICECAST_PORT)
            ),
            timeout=10
        )

        sock.close()

        return {
            "ok": True,
            "message": "Render can reach Caster.fm",
            "host": ICECAST_HOST,
            "port": ICECAST_PORT,
        }

    except Exception as e:

        return {
            "ok": False,
            "message": str(e),
            "host": ICECAST_HOST,
            "port": ICECAST_PORT,
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
    broadcastId: Optional[str] = None,
):

    session = sessions.get(branch_id)

    # -----------------------------------------------------
    # Check live session
    # -----------------------------------------------------

    if not session:

        await websocket.close(code=4404)

        return

    # -----------------------------------------------------
    # Check security token
    # -----------------------------------------------------

    if not secrets.compare_digest(
        session.token,
        token
    ):

        await websocket.close(code=4403)

        return

    # -----------------------------------------------------
    # Save broadcast ID
    # -----------------------------------------------------

    if broadcastId:

        session.broadcast_id = broadcastId

    # -----------------------------------------------------
    # Accept WebSocket
    # -----------------------------------------------------

    await websocket.accept()

    process = None

    try:

        # -------------------------------------------------
        # Start FFmpeg
        # -------------------------------------------------

        process = subprocess.Popen(
            ffmpeg_cmd(session.mount),
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=None,
        )

        session.process = process

        print(
            f"Live stream started for "
            f"{branch_id}",
            flush=True
        )

        # -------------------------------------------------
        # Receive microphone audio
        # -------------------------------------------------

        while True:

            chunk = await websocket.receive_bytes()

            # ---------------------------------------------
            # Check FFmpeg
            # ---------------------------------------------

            if process.poll() is not None:

                raise RuntimeError(
                    "FFmpeg stopped unexpectedly."
                )

            if process.stdin is None:

                raise RuntimeError(
                    "FFmpeg input is unavailable."
                )

            # ---------------------------------------------
            # Send audio to FFmpeg
            # ---------------------------------------------

            process.stdin.write(chunk)
            process.stdin.flush()

    except WebSocketDisconnect:

        print(
            f"Live WebSocket disconnected "
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

        # -------------------------------------------------
        # Stop FFmpeg
        # -------------------------------------------------

        if process is not None:

            try:

                if process.stdin:

                    process.stdin.close()

            except Exception:

                pass

            try:

                process.terminate()

                process.wait(
                    timeout=5
                )

            except Exception:

                try:

                    process.kill()

                except Exception:

                    pass

        # -------------------------------------------------
        # Clear process
        # -------------------------------------------------

        session.process = None

        # -------------------------------------------------
        # Mark Firestore broadcast ended
        # -------------------------------------------------

        mark_broadcast_ended(
            session.broadcast_id
        )

        # -------------------------------------------------
        # Remove live session
        # -------------------------------------------------

        sessions.pop(
            branch_id,
            None
        )

        print(
            f"Live session cleaned up "
            f"for {branch_id}.",
            flush=True
        )


# =========================================================
# STOP LIVE
# =========================================================

@app.post("/api/live/stop")
async def stop_live(body: StopRequest):

    session = sessions.get(
        body.branchId
    )

    broadcast_id = (
        body.broadcastId
        if body.broadcastId
        else (
            session.broadcast_id
            if session
            else None
        )
    )

    # -----------------------------------------------------
    # Stop FFmpeg
    # -----------------------------------------------------

    if session and session.process:

        try:

            session.process.terminate()

        except Exception:

            pass

    # -----------------------------------------------------
    # Mark broadcast ended
    # -----------------------------------------------------

    mark_broadcast_ended(
        broadcast_id
    )

    # -----------------------------------------------------
    # Remove session
    # -----------------------------------------------------

    sessions.pop(
        body.branchId,
        None
    )

    return {
        "ok": True
    }


# =========================================================
# CONNECT BRANCH TO HQ
# =========================================================

@app.post("/api/live/connect-hq")
async def connect_hq(body: ConnectRequest):

    global hq_relay_branch

    session = sessions.get(
        body.branchId
    )

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
