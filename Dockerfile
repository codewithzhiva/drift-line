FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY app app
RUN mkdir -p /data && chown 65532:65532 /data
USER 65532:65532
ENV DATABASE_PATH=/data/driftline.db
EXPOSE 8000
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
