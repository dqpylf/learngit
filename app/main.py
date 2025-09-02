from dotenv import load_dotenv

import httpx
from fastapi import FastAPI, HTTPException, Header, Depends, UploadFile, File, Form

app = FastAPI(title="Fivetran Connector Service")
load_dotenv()


@app.get("/check")
def health() -> Dict[str, str]:
    """健康检查接口"""
    return {"status": "ok", "service": "fivetran-universal-connector"}
