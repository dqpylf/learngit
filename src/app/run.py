# run.py
from app.main import app  # 假设 main.py 中定义了 app 实例

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)