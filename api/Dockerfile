FROM python:3.12-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml .
COPY . .

RUN uv pip install --system .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
