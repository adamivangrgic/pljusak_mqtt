FROM python:3.12-slim

# Non-root user
RUN useradd -m -u 1000 app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

USER app

# Set to run once as a smoke test; override with CMD for production
ENTRYPOINT ["python", "-u", "main.py"]
