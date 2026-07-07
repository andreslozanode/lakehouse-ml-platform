"""DistilBERT fine-tuning in PyTorch with full CUDA optimization.

GPU optimizations applied when CUDA is available:
  * Mixed precision (torch.autocast + GradScaler) - ~2x throughput on Ampere+.
  * TF32 matmuls enabled for additional speed at negligible accuracy cost.
  * Pinned memory + multi-worker DataLoaders for host->device overlap.
  * Gradient clipping + linear warmup/decay schedule for stable convergence.
Early stopping on validation F1-macro; best checkpoint restored before the
final test evaluation and MLflow registration.
"""

from __future__ import annotations

import copy

import mlflow
import numpy as np
import torch
from torch.utils.data import DataLoader, Dataset
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    get_linear_schedule_with_warmup,
)

from lakehouse.common.config import load_config
from lakehouse.common.logging import get_logger
from lakehouse.ml.evaluate import compute_metrics, log_report
from lakehouse.ml.features import load_splits
from lakehouse.ml.tracking import init_mlflow, registered_model_name, standard_tags

log = get_logger(__name__)


class TextDataset(Dataset):
    def __init__(self, texts: list[str], labels: list[int], tokenizer, max_length: int):
        self.encodings = tokenizer(
            texts,
            truncation=True,
            padding="max_length",
            max_length=max_length,
            return_tensors="pt",
        )
        self.labels = torch.tensor(labels, dtype=torch.long)

    def __len__(self) -> int:
        return len(self.labels)

    def __getitem__(self, idx: int) -> dict:
        return {
            "input_ids": self.encodings["input_ids"][idx],
            "attention_mask": self.encodings["attention_mask"][idx],
            "labels": self.labels[idx],
        }


def _device() -> torch.device:
    if torch.cuda.is_available():
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        torch.backends.cudnn.benchmark = True
        log.info("CUDA device: %s", torch.cuda.get_device_name(0))
        return torch.device("cuda")
    log.warning("CUDA not available; training on CPU (expect it to be slow).")
    return torch.device("cpu")


@torch.no_grad()
def _evaluate(model, loader: DataLoader, device: torch.device) -> tuple[np.ndarray, np.ndarray]:
    model.eval()
    preds, labels = [], []
    for batch in loader:
        batch = {k: v.to(device, non_blocking=True) for k, v in batch.items()}
        with torch.autocast(device_type=device.type, enabled=device.type == "cuda"):
            logits = model(
                input_ids=batch["input_ids"], attention_mask=batch["attention_mask"]
            ).logits
        preds.append(torch.argmax(logits, dim=-1).cpu().numpy())
        labels.append(batch["labels"].cpu().numpy())
    return np.concatenate(preds), np.concatenate(labels)


def run() -> None:
    cfg = load_config()
    bert_cfg = cfg.ml.bert
    device = _device()
    use_amp = device.type == "cuda"

    splits = load_splits(cfg)
    text_col, label_col = cfg.ml.text_col, cfg.ml.label_col
    tokenizer = AutoTokenizer.from_pretrained(bert_cfg.model_name)

    loaders: dict[str, DataLoader] = {}
    for name, frame in splits.items():
        dataset = TextDataset(
            frame[text_col].tolist(),
            frame[label_col].tolist(),
            tokenizer,
            int(bert_cfg.max_length),
        )
        loaders[name] = DataLoader(
            dataset,
            batch_size=int(bert_cfg.batch_size),
            shuffle=(name == "train"),
            num_workers=2,
            pin_memory=use_amp,
        )

    model = AutoModelForSequenceClassification.from_pretrained(
        bert_cfg.model_name, num_labels=2
    ).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=float(bert_cfg.lr), weight_decay=0.01)
    total_steps = len(loaders["train"]) * int(bert_cfg.epochs)
    scheduler = get_linear_schedule_with_warmup(
        optimizer, int(total_steps * float(bert_cfg.warmup_ratio)), total_steps
    )
    scaler = torch.cuda.amp.GradScaler(enabled=use_amp)

    init_mlflow(cfg)
    with mlflow.start_run(run_name="bert-distilbert", tags=standard_tags(cfg)) as run:
        mlflow.log_params(
            {
                "model_name": bert_cfg.model_name,
                "max_length": bert_cfg.max_length,
                "batch_size": bert_cfg.batch_size,
                "lr": bert_cfg.lr,
                "epochs": bert_cfg.epochs,
                "device": device.type,
                "amp": use_amp,
            }
        )

        best_f1, patience_left = -1.0, int(bert_cfg.patience)
        best_state: dict | None = None

        for epoch in range(int(bert_cfg.epochs)):
            model.train()
            running_loss = 0.0
            for batch in loaders["train"]:
                batch = {k: v.to(device, non_blocking=True) for k, v in batch.items()}
                optimizer.zero_grad(set_to_none=True)
                with torch.autocast(device_type=device.type, enabled=use_amp):
                    loss = model(**batch).loss
                scaler.scale(loss).backward()
                scaler.unscale_(optimizer)
                torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                scaler.step(optimizer)
                scaler.update()
                scheduler.step()
                running_loss += loss.item()

            val_pred, val_true = _evaluate(model, loaders["val"], device)
            val_metrics = compute_metrics(val_true, val_pred)
            mlflow.log_metrics(
                {
                    "train_loss": running_loss / len(loaders["train"]),
                    **{f"val_{k}": v for k, v in val_metrics.items()},
                },
                step=epoch,
            )
            log.info(
                "epoch %d: loss=%.4f val_f1=%.4f",
                epoch,
                running_loss,
                val_metrics["f1_macro"],
            )

            if val_metrics["f1_macro"] > best_f1:
                best_f1 = val_metrics["f1_macro"]
                best_state = copy.deepcopy(model.state_dict())
                patience_left = int(bert_cfg.patience)
            else:
                patience_left -= 1
                if patience_left <= 0:
                    log.info("Early stopping at epoch %d.", epoch)
                    break

        if best_state is not None:
            model.load_state_dict(best_state)
        test_pred, test_true = _evaluate(model, loaders["test"], device)
        test_metrics = compute_metrics(test_true, test_pred)
        mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})
        log_report("distilbert", test_true, test_pred)

        components = {"model": model.cpu(), "tokenizer": tokenizer}
        mlflow.transformers.log_model(
            transformers_model=components, artifact_path="model", task="text-classification"
        )
        mlflow.register_model(f"runs:/{run.info.run_id}/model", registered_model_name(cfg, "bert"))


if __name__ == "__main__":
    run()
