# ---- build stage ----
FROM python:3.11-slim AS build
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install ".[serving]"

# ---- runtime stage ----
FROM python:3.11-slim
RUN useradd --create-home appuser
WORKDIR /app
COPY --from=build /install /usr/local
COPY conf ./conf
ENV SERVING_MODEL_NAME=lakehouse-sentiment-classical \
    MLFLOW_TRACKING_URI=http://mlflow:5000
USER appuser
EXPOSE 8000
HEALTHCHECK CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "lakehouse.ml.serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
