import os, secrets, subprocess
from dataclasses import dataclass
from typing import Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()
app=FastAPI(title="CAC Radio Live Backend")

origins=[x.strip() for x in os.getenv("CORS_ORIGINS","http://localhost:5500").split(",") if x.strip()]
app.add_middleware(CORSMiddleware,allow_origins=origins,allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

HOST=os.getenv("ICECAST_HOST","")
PORT=os.getenv("ICECAST_PORT","8000")
USER=os.getenv("ICECAST_SOURCE_USER","source")
PASSWORD=os.getenv("ICECAST_SOURCE_PASSWORD","")
PUBLIC_BASE=os.getenv("ICECAST_PUBLIC_BASE","").rstrip("/")

@dataclass
class Session:
    branch_id:str
    branch_name:str
    token:str
    mount:str
    process:Optional[subprocess.Popen]=None

sessions:Dict[str,Session]={}
hq_relay_branch:Optional[str]=None

class StartRequest(BaseModel):
    branchId:str
    branchName:str
    title:str
    presenter:str|None=None
    programmeId:str|None=None

class StopRequest(BaseModel):
    branchId:str
    broadcastId:str|None=None

class ConnectRequest(BaseModel):
    branchId:str

def mount_for(branch_id:str):
    safe="".join(c for c in branch_id if c.isalnum() or c in "-_") or "branch"
    return f"/{safe}.mp3"

def ffmpeg_cmd(mount:str):
    if not HOST or not PASSWORD:
        raise RuntimeError("Icecast environment variables are not configured.")
    target=f"icecast://{USER}:{PASSWORD}@{HOST}:{PORT}{mount}"
    return ["ffmpeg","-hide_banner","-loglevel","warning","-f","webm","-i","pipe:0","-vn","-ac","2","-ar","44100","-b:a","96k","-content_type","audio/mpeg","-f","mp3",target]

@app.get("/api/health")
async def health():
    return {"ok":True,"liveBranches":list(sessions.keys()),"hqRelayBranch":hq_relay_branch}

@app.post("/api/live/start")
async def start_live(body:StartRequest):
    if body.branchId in sessions:
        raise HTTPException(409,"Branch is already live.")
    token=secrets.token_urlsafe(24)
    mount=mount_for(body.branchId)
    sessions[body.branchId]=Session(body.branchId,body.branchName,token,mount)
    return {
        "ok":True,
        "sessionToken":token,
        "publicStreamUrl":f"{PUBLIC_BASE}{mount}" if PUBLIC_BASE else mount
    }

@app.websocket("/ws/live/{branch_id}")
async def live_ws(websocket:WebSocket,branch_id:str,token:str):
    session=sessions.get(branch_id)
    if not session or not secrets.compare_digest(session.token,token):
        await websocket.close(code=4403)
        return
    await websocket.accept()
    try:
        process=subprocess.Popen(ffmpeg_cmd(session.mount),stdin=subprocess.PIPE)
        session.process=process
        while True:
            chunk=await websocket.receive_bytes()
            if process.poll() is not None:
                raise RuntimeError("FFmpeg stopped.")
            process.stdin.write(chunk)
            process.stdin.flush()
    except WebSocketDisconnect:
        pass
    finally:
        if session.process:
            try: session.process.stdin.close()
            except: pass
            try: session.process.terminate()
            except: pass
        sessions.pop(branch_id,None)

@app.post("/api/live/stop")
async def stop_live(body:StopRequest):
    session=sessions.get(body.branchId)
    if session and session.process:
        try: session.process.terminate()
        except: pass
    sessions.pop(body.branchId,None)
    return {"ok":True}

@app.post("/api/live/connect-hq")
async def connect_hq(body:ConnectRequest):
    global hq_relay_branch
    session=sessions.get(body.branchId)
    if not session:
        raise HTTPException(404,"That branch is not live.")
    hq_relay_branch=body.branchId
    return {"ok":True,"branchId":body.branchId,"publicStreamUrl":f"{PUBLIC_BASE}{session.mount}" if PUBLIC_BASE else session.mount}

@app.post("/api/live/disconnect-hq")
async def disconnect_hq():
    global hq_relay_branch
    hq_relay_branch=None
    return {"ok":True}
