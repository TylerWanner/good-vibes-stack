# Use the official Scrapling image — has all browsers + system deps pre-installed via uv
FROM pyd4vinci/scrapling:latest

# Add fastapi/uvicorn into uv's venv (same env scrapling lives in)
RUN uv pip install fastapi uvicorn

WORKDIR /app
COPY server.py .

EXPOSE 8002
# Clear the scrapling entrypoint so we can run uvicorn directly
ENTRYPOINT []
CMD ["uv", "run", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8002"]
