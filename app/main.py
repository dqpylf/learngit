import os
from pathlib import Path
from typing import Any, Dict, Optional
from dotenv import load_dotenv

import httpx
from fastapi import FastAPI, HTTPException, Header, Depends, UploadFile, File, Form

from .config import settings
from .fivetran_client import FivetranClient
from .schemas import (
    CreateGroupRequest,
    CreateUniversalConnectorRequest,
    SchemaUpdateRequest
)

app = FastAPI(title="Fivetran Connector Service")
load_dotenv()

def get_fivetran_client(authorization: Optional[str] = Header(None)) -> FivetranClient:
    """
    获取 Fivetran 客户端，使用环境变量中的配置验证fivetran
    """

    if authorization and authorization.startswith("Bearer "):
        try:
            # 获得authorization中的token
            token = authorization.split("Bearer ")[1]

            # 如果Authorization中token为空则报错
            if not token:
                raise ValueError("Auth token is required !!!")

            # 准备auth0信息
            auth0_url=settings.auth0_api_base
            headers = {
                  "Authorization": f"Bearer {token}",
                  "Content-Type": "application/json",
                  "Accept": "application/json"
            }

            # 获得auth0返回信息
            response =""
            with httpx.Client() as client:
                response=client.get(auth0_url, headers=headers)
            response_data = response.json()

            # 如果auth0返回信息中userName为空则报错
            if not response_data['data']['userName']:
                raise ValueError("You are not authorized !!!")

            # 从环境变量里取得key和secret
            api_key = os.getenv('FIVETRAN_KEY',None)
            # print(f"FIVETRAN_KEY: {api_key}")

            api_secret = os.getenv('FIVETRAN_SECRET',None)
            # print(f"FIVETRAN_SECRET: {api_secret}")

            # 如何key或secret则raise exception
            if api_key is None or api_secret is None:
                raise ValueError("Fivetran API key and secret are required !!!")

            return FivetranClient(api_key=api_key, api_secret=api_secret)

        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid Authorization header: {exc}"
            )
    return None


@app.get("/check")
def health() -> Dict[str, str]:
    """健康检查接口"""
    return {"status": "ok", "service": "fivetran-universal-connector"}

# --- 证书管理 ---
CERT_STORAGE_DIR = Path("./certificates")
CERT_STORAGE_DIR.mkdir(exist_ok=True)

@app.post("/certificates/upload_private")
async def upload_certificate(
    cert_file: UploadFile = File(..., description="Certificate file (.pem, .crt)"),
    key_file: UploadFile = File(..., description="Private key file (.pem, .key)"),
    cert_name: str = Form(..., description="Certificate name for reference")
) -> Dict[str, Any]:
    """上传证书和私钥文件"""
    try:
        # 验证文件类型
        if not cert_file.filename.lower().endswith(('.pem', '.crt')):
            raise HTTPException(status_code=400, detail="Certificate file must be .pem or .crt")
        if not key_file.filename.lower().endswith(('.pem', '.key')):
            raise HTTPException(status_code=400, detail="Private key file must be .pem or .key")
        
        # 读取文件内容
        cert_content = await cert_file.read()
        key_content = await key_file.read()
        
        # 验证 PEM 格式
        cert_text = cert_content.decode('utf-8')
        key_text = key_content.decode('utf-8')
        
        if not cert_text.strip().startswith('-----BEGIN CERTIFICATE-----'):
            raise HTTPException(status_code=400, detail="Invalid certificate format")
        if not key_text.strip().startswith('-----BEGIN PRIVATE KEY-----'):
            raise HTTPException(status_code=400, detail="Invalid private key format")
        
        # 保存文件
        cert_path = CERT_STORAGE_DIR / f"{cert_name}_cert.pem"
        key_path = CERT_STORAGE_DIR / f"{cert_name}_key.pem"
        
        with open(cert_path, 'wb') as f:
            f.write(cert_content)
        with open(key_path, 'wb') as f:
            f.write(key_content)
        
        return {
            "ok": True,
            "certificate_name": cert_name,
            "certificate_path": str(cert_path),
            "private_key_path": str(key_path),
            "message": "Certificate uploaded successfully"
        }
        
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to upload certificate: {exc}")

@app.post("/certificates/upload_ca")
async def upload_ca_certificate(
    ca_file: UploadFile = File(..., description="CA certificate file (.pem, .crt)"),
    ca_name: str = Form(..., description="CA certificate name for reference")
) -> Dict[str, Any]:
    """上传CA证书文件"""
    try:
        # 验证文件类型
        if not ca_file.filename.lower().endswith(('.pem', '.crt')):
            raise HTTPException(status_code=400, detail="CA certificate file must be .pem or .crt")
        
        # 读取文件内容
        ca_content = await ca_file.read()
        
        # 验证 PEM 格式
        ca_text = ca_content.decode('utf-8')
        
        if not ca_text.strip().startswith('-----BEGIN CERTIFICATE-----'):
            raise HTTPException(status_code=400, detail="Invalid CA certificate format")
        
        # 保存文件
        ca_path = CERT_STORAGE_DIR / f"{ca_name}_ca.pem"
        
        with open(ca_path, 'wb') as f:
            f.write(ca_content)
        
        return {
            "ok": True,
            "ca_certificate_name": ca_name,
            "ca_certificate_path": str(ca_path),
            "message": "CA certificate uploaded successfully"
        }
        
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to upload CA certificate: {exc}")

@app.get("/certificates")
def list_certificates() -> Dict[str, Any]:
    """列出所有已上传的证书"""
    try:
        certificates = []
        ca_certificates = []
        
        # 列出客户端证书
        for cert_file in CERT_STORAGE_DIR.glob("*_cert.pem"):
            cert_name = cert_file.stem.replace("_cert", "")
            key_file = CERT_STORAGE_DIR / f"{cert_name}_key.pem"
            
            if key_file.exists():
                certificates.append({
                    "name": cert_name,
                    "type": "client_certificate",
                    "certificate_file": cert_file.name,
                    "private_key_file": key_file.name,
                    "uploaded_at": cert_file.stat().st_mtime
                })
        
        # 列出CA证书
        for ca_file in CERT_STORAGE_DIR.glob("*_ca.pem"):
            ca_name = ca_file.stem.replace("_ca", "")
            ca_certificates.append({
                "name": ca_name,
                "type": "ca_certificate",
                "ca_certificate_file": ca_file.name,
                "uploaded_at": ca_file.stat().st_mtime
            })
        
        return {
            "ok": True, 
            "client_certificates": certificates,
            "ca_certificates": ca_certificates
        }
        
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list certificates: {exc}")

# --- 组管理 ---
@app.get("/fivetran/groups")
def list_groups(limit: int = 100, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """列出所有组"""
    try:
        result = client.list_groups(limit=limit)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list groups: {exc}")

@app.post("/fivetran/groups")
def create_group(req: CreateGroupRequest, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """创建组（如果不存在）"""
    try:
        existing = client.find_group_by_name(req.name)
        if existing:
            return {"group": existing, "created": False}
        created = client.create_group(req.name)
        return {"group": created, "created": True}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create group: {exc}")

@app.get("/fivetran/groups/{group_id}")
def get_group(group_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """获取组详情"""
    try:
        result = client.get_group(group_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get group: {exc}")

# --- 通用连接器管理 ---
@app.post("/fivetran/connectors", response_model=Dict[str, Any])
def create_connector(req: CreateUniversalConnectorRequest, client: FivetranClient = Depends(get_fivetran_client)) -> \
Dict[str, Any]:
    """创建通用连接器，支持所有 Fivetran 数据源"""
    try:
        # 准备配置数据
        config = req.config.model_dump(exclude_none=True)

        # 处理自定义配置
        if config.get('custom_config'):
            config.update(config.pop('custom_config'))

        # 确保 config 中有 schema_prefix（如果适用）
        # if 'schema_prefix' not in config or not config['schema_prefix']:
        config['schema_prefix'] = req.schema_name
        config['update_method'] = 'TELEPORT'
        run_setup_tests = True
        trust_certificates = True
        paused = True

        # 调用 Fivetran API 创建连接器
        group_id = os.getenv('FIVETRAN_GROUP_ID', None)

        connector = client.create_connector(
            group_id=group_id,
            service=req.service,
            config=config,
            trust_certificates=trust_certificates,
            trust_fingerprints=req.trust_fingerprints,
            run_setup_tests=run_setup_tests,
            paused=paused,
            pause_after_trial=req.pause_after_trial,
            sync_frequency=req.sync_frequency,
            daily_sync_time=req.daily_sync_time,
            schedule_type=req.schedule_type,
            data_delay_sensitivity=req.data_delay_sensitivity,
            data_delay_threshold=req.data_delay_threshold,
            networking_method=req.networking_method,
            proxy_agent_id=req.proxy_agent_id,
            private_link_id=req.private_link_id,
            hybrid_deployment_agent_id=req.hybrid_deployment_agent_id,
            schema_name = req.schema_name
        )

        return {"ok": True, "connector": connector}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create connector: {exc}")


@app.get("/fivetran/connectors")
def list_connectors(group_id: str = None, limit: int = 100, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """列出连接器"""
    try:
        result = client.list_connectors(group_id=group_id, limit=limit)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list connectors: {exc}")

@app.get("/fivetran/connectors/{connector_id}")
def get_connector(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """获取连接器详情"""
    try:
        result = client.get_connector(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get connector: {exc}")

@app.patch("/fivetran/connectors/{connector_id}")
def update_connector(connector_id: str, updates: Dict[str, Any], client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """更新连接器配置"""
    try:
        result = client.update_connector(connector_id, **updates)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update connector: {exc}")

@app.delete("/fivetran/connectors/{connector_id}")
def delete_connector(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """删除连接器"""
    try:
        result = client.delete_connector(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to delete connector: {exc}")

# --- 连接器操作 ---
@app.post("/fivetran/connectors/{connector_id}/pause")
def pause_connector(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """暂停连接器"""
    try:
        result = client.pause_connector(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to pause connector: {exc}")

@app.post("/fivetran/connectors/{connector_id}/resume")
def resume_connector(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """恢复连接器"""
    try:
        result = client.resume_connector(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to resume connector: {exc}")

@app.post("/fivetran/connectors/{connector_id}/test")
def run_setup_tests(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """运行连接器设置测试"""
    try:
        result = client.run_setup_tests(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to run setup tests: {exc}")

@app.post("/fivetran/connectors/{connector_id}/sync")
def force_sync(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """强制同步连接器数据"""
    try:
        result = client.force_sync(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to force sync: {exc}")

@app.post("/fivetran/connectors/{connector_id}/resync")
def resync_connector(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """重新同步连接器数据（历史同步）"""
    try:
        result = client.resync_connector(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to resync connector: {exc}")

@app.get("/fivetran/connectors/{connector_id}/state")
def get_connector_state(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """获取连接器状态"""
    try:
        result = client.get_connector_state(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get connector state: {exc}")

# --- Schema 管理 ---
@app.get("/fivetran/connectors/{connector_id}/schemas")
def get_connector_schemas(connector_id: str, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """获取连接器的schema信息，包括所有表"""
    try:
        result = client.get_schema(connector_id)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get connector schemas: {exc}")

@app.patch("/fivetran/connectors/{connector_id}/schemas")
def update_connector_schemas(connector_id: str, req: SchemaUpdateRequest, client: FivetranClient = Depends(get_fivetran_client)) -> Dict[str, Any]:
    """更新连接器的schema配置，选择特定表进行同步"""
    try:
        # 将Pydantic模型转换为字典
        schema_updates = req.model_dump(exclude_none=True)
        result = client.update_schema(connector_id, schema_updates)
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update connector schemas: {exc}")

