FROM python:3.9-slim
WORKDIR /app
COPY src/main/python/ /app/
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5001
CMD ["python", "run.py"]