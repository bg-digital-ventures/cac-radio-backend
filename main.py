import os
import secrets
import socket
from dataclasses import dataclass
from typing import Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
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

def mark_broadcast_ended(
    broadcast_id: Optional[str]
):
    """
    Mark the Firestore broadcast as ended.

    This is called when the dashboard stops a broadcast
    or when a live session is cleaned up.
    """

    if not broadcast_id:
        return

    try:
        firestore_db.collection(
            "broadcasts"
        ).document(
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
    Caster.fm Free currently uses the configured
    mount point for the radio channel.

    The branch ID is kept separate from the mount point
    because multiple CAC branches may share the same
    public radio channel.
    """

    return ICECAST_MOUNT


# =========================================================
# PUBLIC STREAM URL
# =========================================================

def public_stream_url(mount: str) -> str:
    """
    Build the public listener URL when a public base URL
    has been configured.

    If no public base is configured, return the mount only.
    """

    if PUBLIC_BASE:
        return f"{PUBLIC_BASE}{mount}"

    return mount


# =========================================================
# CASTER INFORMATION
# =========================================================

def caster_config():
    """
    Return the information needed by the external
    Caster.fm-compatible broadcaster.

    IMPORTANT:
    The Caster.fm password is NEVER returned by this API.
    """

    return {
        "host": ICECAST_HOST,
        "port": int(ICECAST_PORT),
        "username": ICECAST_SOURCE_USER,
        "mount": ICECAST_MOUNT,
        "protocol": "icecast",
        "codec": "mp3",
        "bitrate": "96k",
    }


# =========================================================
# ROOT
# =========================================================

@app.get("/")
async def root():

    return {
        "service": "CAC Radio Live Backend",
        "status": "online",
    }


# =========================================================
# HEALTH CHECK
# =========================================================

@app.get("/api/health")
async def health():

    return {
        "ok": True,
        "liveBranches": list(sessions.keys()),
        "hqRelayBranch": hq_relay_branch,
        "caster": {
            "host": ICECAST_HOST,
            "port": int(ICECAST_PORT),
            "mount": ICECAST_MOUNT,
        },
    }


# =========================================================
# TEST CASTER / ICECAST CONNECTION
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
            "ok": True,
            "message": "Render can reach Caster.fm",
            "host": ICECAST_HOST,
            "port": ICECAST_PORT,
        }

    except Exception as exc:

        return {
            "ok": False,
            "message": str(exc),
            "host": ICECAST_HOST,
            "port": ICECAST_PORT,
        }


# =========================================================
# CASTER CONFIG
# =========================================================

@app.get("/api/caster/config")
async def get_caster_config():

    return {
        "ok": True,
        "caster": caster_config(),
    }


# =========================================================
# START LIVE SESSION
# =========================================================

@app.post("/api/live/start")
async def start_live(body: StartRequest):

    # -----------------------------------------------------
    # Prevent duplicate live session
    # -----------------------------------------------------

    if body.branchId in sessions:
        raise HTTPException(
            status_code=409,
            detail="Branch is already live."
        )

    # -----------------------------------------------------
    # Create secure session token
    # -----------------------------------------------------

    token = secrets.token_urlsafe(24)

    mount = mount_for(
        body.branchId
    )

    # -----------------------------------------------------
    # Create session
    # -----------------------------------------------------

    session = Session(
        branch_id=body.branchId,
        branch_name=body.branchName,
        token=token,
        mount=mount,
    )

    sessions[body.branchId] = session

    # -----------------------------------------------------
    # Public listener URL
    # -----------------------------------------------------

    stream_url = public_stream_url(
        mount
    )

    print(
        f"Live session prepared for "
        f"{body.branchId}",
        flush=True
    )

    # -----------------------------------------------------
    # IMPORTANT
    #
    # Render does NOT start FFmpeg.
    #
    # Render does NOT receive browser microphone audio.
    #
    # Render does NOT attempt to connect to the
    # Caster.fm source port.
    #
    # A Caster.fm-compatible broadcaster on the user's
    # device is responsible for sending the microphone
    # audio to Caster.fm.
    # -----------------------------------------------------

    return {
        "ok": True,
        "sessionToken": token,
        "branchId": body.branchId,
        "branchName": body.branchName,
        "mount": mount,
        "publicStreamUrl": stream_url,
        "caster": caster_config(),
        "message": (
            "Live session prepared. "
            "Connect your broadcaster to Caster.fm."
        ),
    }


# =========================================================
# GET LIVE SESSION
# =========================================================

@app.get("/api/live/session/{branch_id}")
async def get_live_session(
    branch_id: str
):

    session = sessions.get(
        branch_id
    )

    if not session:

        return {
            "ok": True,
            "live": False,
            "branchId": branch_id,
        }

    return {
        "ok": True,
        "live": True,
        "branchId": session.branch_id,
        "branchName": session.branch_name,
        "mount": session.mount,
        "broadcastId": session.broadcast_id,
        "publicStreamUrl": public_stream_url(
            session.mount
        ),
    }


# =========================================================
# STOP LIVE
# =========================================================

@app.post("/api/live/stop")
async def stop_live(body: StopRequest):

    session = sessions.get(
        body.branchId
    )

    # -----------------------------------------------------
    # Determine broadcast ID
    # -----------------------------------------------------

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
    # Mark Firestore broadcast ended
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

    print(
        f"Live session stopped for "
        f"{body.branchId}.",
        flush=True
    )

    return {
        "ok": True,
        "branchId": body.branchId,
        "message": "Live session stopped.",
    }


# =========================================================
# CONNECT BRANCH TO HQ
# =========================================================

@app.post("/api/live/connect-hq")
async def connect_hq(body: ConnectRequest):

    global hq_relay_branch

    # -----------------------------------------------------
    # Check branch session
    # -----------------------------------------------------

    session = sessions.get(
        body.branchId
    )

    if not session:

        raise HTTPException(
            status_code=404,
            detail="That branch is not live."
        )

    # -----------------------------------------------------
    # Save HQ relay branch
    # -----------------------------------------------------

    hq_relay_branch = body.branchId

    stream_url = public_stream_url(
        session.mount
    )

    return {
        "ok": True,
        "branchId": body.branchId,
        "publicStreamUrl": stream_url,
        "mount": session.mount,
        "message": (
            "Branch connected to Headquarters output."
        ),
    }


# =========================================================
# DISCONNECT HQ
# =========================================================

@app.post("/api/live/disconnect-hq")
async def disconnect_hq():

    global hq_relay_branch

    hq_relay_branch = None

    return {
        "ok": True,
        "message": "HQ relay disconnected.",
    }
