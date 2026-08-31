import os
import secrets
import socket
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
# MOUNT POINT
# =========================================================

def mount_for(
    branch_id: str
) -> str:

    """
    All CAC branches currently use the same
    Caster.fm channel/mount.

    The branch ID remains separate because
    multiple CAC branches may broadcast through
    the same public radio channel.
    """

    return ICECAST_MOUNT


# =========================================================
# PUBLIC STREAM URL
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

    """
    Return safe Caster.fm configuration.

    NEVER return the Caster.fm password.
    """

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

    """
    Build the FFmpeg command.

    Browser:
        WebM/Opus

    FFmpeg:
        WebM/Opus -> MP3

    Caster.fm:
        Icecast MP3 source
    """

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

        "-loglevel",

        "warning",

        # -------------------------------------------------
        # Browser audio input
        # -------------------------------------------------

        "-f",

        "webm",

        "-i",

        "pipe:0",

        # -------------------------------------------------
        # Audio only
        # -------------------------------------------------

        "-vn",

        # -------------------------------------------------
        # Stereo
        # -------------------------------------------------

        "-ac",

        "2",

        # -------------------------------------------------
        # 44.1 kHz
        # -------------------------------------------------

        "-ar",

        "44100",

        # -------------------------------------------------
        # Radio bitrate
        # -------------------------------------------------

        "-b:a",

        "96k",

        # -------------------------------------------------
        # Caster / Icecast content type
        # -------------------------------------------------

        "-content_type",

        "audio/mpeg",

        # -------------------------------------------------
        # MP3 output
        # -------------------------------------------------

        "-f",

        "mp3",

        target

    ]


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
# HEALTH CHECK
# =========================================================

@app.get("/api/health")
async def health():

    return {

        "ok":
            True,

        "liveBranches":
            list(sessions.keys()),

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
# START LIVE SESSION
# =========================================================

@app.post("/api/live/start")
async def start_live(
    body: StartRequest
):

    # -----------------------------------------------------
    # Prevent duplicate live session
    # -----------------------------------------------------

    if body.branchId in sessions:

        raise HTTPException(

            status_code=409,

            detail="Branch is already live."

        )


    # -----------------------------------------------------
    # Generate secure browser session token
    # -----------------------------------------------------

    token = secrets.token_urlsafe(24)


    # -----------------------------------------------------
    # Determine Caster mount
    # -----------------------------------------------------

    mount = mount_for(
        body.branchId
    )


    # -----------------------------------------------------
    # Create session
    # -----------------------------------------------------

    session = Session(

        branch_id=
            body.branchId,

        branch_name=
            body.branchName,

        token=
            token,

        mount=
            mount

    )


    sessions[
        body.branchId
    ] = session


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
# GET LIVE SESSION
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

        "publicStreamUrl":
            public_stream_url(
                session.mount
            )

    }


# =========================================================
# LIVE MICROPHONE WEBSOCKET
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


    # -----------------------------------------------------
    # Find active session
    # -----------------------------------------------------

    session = sessions.get(
        branch_id
    )


    if session is None:

        print(

            f"WebSocket rejected: "
            f"no active session for {branch_id}",

            flush=True

        )

        # Accept first so the browser does not receive
        # an HTTP 403 handshake response.

        await websocket.accept()

        await websocket.close(
            code=4404
        )

        return


    # -----------------------------------------------------
    # Accept WebSocket
    # -----------------------------------------------------

    await websocket.accept()


    # -----------------------------------------------------
    # Validate token
    # -----------------------------------------------------

    if not token:

        print(

            f"WebSocket rejected: "
            f"missing token for {branch_id}",

            flush=True

        )

        await websocket.close(
            code=4403
        )

        return


    if not secrets.compare_digest(

        session.token,

        token

    ):

        print(

            f"WebSocket rejected: "
            f"invalid token for {branch_id}",

            flush=True

        )

        await websocket.close(
            code=4403
        )

        return


    # -----------------------------------------------------
    # Save Firestore broadcast ID
    # -----------------------------------------------------

    if broadcastId:

        session.broadcast_id = (
            broadcastId
        )


    print(

        f"WebSocket authenticated "
        f"for {branch_id}",

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

            stderr=None,

            bufsize=0

        )


        session.process = process


        print(

            f"FFmpeg started for {branch_id}",

            flush=True

        )


        # =================================================
        # RECEIVE BROWSER MICROPHONE AUDIO
        # =================================================

        while True:

            chunk = (
                await websocket.receive_bytes()
            )


            # ---------------------------------------------
            # Check FFmpeg
            # ---------------------------------------------

            if process.poll() is not None:

                raise RuntimeError(

                    "FFmpeg stopped unexpectedly."

                )


            # ---------------------------------------------
            # Make sure stdin exists
            # ---------------------------------------------

            if process.stdin is None:

                raise RuntimeError(

                    "FFmpeg input is unavailable."

                )


            # ---------------------------------------------
            # Send WebM audio to FFmpeg
            # ---------------------------------------------

            try:

                process.stdin.write(
                    chunk
                )

                process.stdin.flush()

            except BrokenPipeError:

                raise RuntimeError(

                    "FFmpeg pipe closed unexpectedly."

                )


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

        # =================================================
        # STOP FFMPEG
        # =================================================

        if process is not None:

            try:

                if process.stdin:

                    process.stdin.close()

            except Exception:

                pass


            try:

                if process.poll() is None:

                    process.terminate()

                    process.wait(
                        timeout=5
                    )

            except Exception:

                try:

                    if process.poll() is None:

                        process.kill()

                except Exception:

                    pass


        # =================================================
        # CLEAR PROCESS
        # =================================================

        session.process = None


        # =================================================
        # MARK FIRESTORE BROADCAST ENDED
        # =================================================

        mark_broadcast_ended(

            session.broadcast_id

        )


        # =================================================
        # REMOVE SESSION
        # =================================================

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
async def stop_live(
    body: StopRequest
):

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
    # Stop FFmpeg
    # -----------------------------------------------------

    if session and session.process:

        try:

            if session.process.poll() is None:

                session.process.terminate()

                try:

                    session.process.wait(
                        timeout=5
                    )

                except subprocess.TimeoutExpired:

                    session.process.kill()

        except Exception as exc:

            print(

                f"Unable to stop FFmpeg "
                f"for {body.branchId}: {exc}",

                flush=True

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

        "ok":
            True,

        "branchId":
            body.branchId,

        "message":
            "Live session stopped."

    }


# =========================================================
# CONNECT BRANCH TO HQ
# =========================================================

@app.post("/api/live/connect-hq")
async def connect_hq(
    body: ConnectRequest
):

    global hq_relay_branch


    # -----------------------------------------------------
    # Find branch session
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
    # Set HQ relay
    # -----------------------------------------------------

    hq_relay_branch = (
        body.branchId
    )


    stream_url = public_stream_url(
        session.mount
    )


    print(

        f"HQ relay connected to "
        f"{body.branchId}",

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
