"""FastAPI inference service backed by the MLflow 'champion' alias."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import mlflow.pyfunc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

MODEL_NAME = os.environ.get("SERVING_MODEL_NAME", "lakehouse-sentiment-classical")
MODEL_URI = os.environ.get("SERVING_MODEL_URI", f"models:/{MODEL_NAME}@champion")

_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    _state["model"] = mlflow.pyfunc.load_model(MODEL_URI)
    yield
    _state.clear()


app = FastAPI(title="lakehouse-sentiment", version="1.0.0", lifespan=lifespan)


class PredictRequest(BaseModel):
    texts: list[str] = Field(..., min_length=1, max_length=256)


class Prediction(BaseModel):
    text: str
    label: int
    sentiment: str


class PredictResponse(BaseModel):
    model: str
    predictions: list[Prediction]


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model_uri": MODEL_URI, "loaded": "model" in _state}


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    model = _state.get("model")
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")
    labels = [int(x) for x in model.predict(request.texts)]
    return PredictResponse(
        model=MODEL_URI,
        predictions=[
            Prediction(text=t[:120], label=y, sentiment="positive" if y == 1 else "negative")
            for t, y in zip(request.texts, labels, strict=False)
        ],
    )
