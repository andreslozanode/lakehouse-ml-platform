"""TensorFlow/Keras baseline: TextVectorization -> Embedding -> BiLSTM head.

Uses mixed_float16 policy automatically when a GPU is present, tf.data
pipelines with AUTOTUNE prefetching, and EarlyStopping/ReduceLROnPlateau.
"""

from __future__ import annotations

import mlflow
import numpy as np
import tensorflow as tf

from lakehouse.common.config import load_config
from lakehouse.common.logging import get_logger
from lakehouse.ml.evaluate import compute_metrics, log_report
from lakehouse.ml.features import load_splits
from lakehouse.ml.tracking import init_mlflow, registered_model_name, standard_tags

log = get_logger(__name__)
VOCAB_SIZE = 30_000
SEQ_LEN = 128


def _dataset(texts, labels, batch_size: int, shuffle: bool) -> tf.data.Dataset:
    ds = tf.data.Dataset.from_tensor_slices((texts.to_numpy(), labels.to_numpy()))
    if shuffle:
        ds = ds.shuffle(10_000, seed=42)
    return ds.batch(batch_size).prefetch(tf.data.AUTOTUNE)


def _build_model(vectorizer: tf.keras.layers.TextVectorization) -> tf.keras.Model:
    inputs = tf.keras.Input(shape=(1,), dtype=tf.string)
    x = vectorizer(inputs)
    x = tf.keras.layers.Embedding(VOCAB_SIZE, 128, mask_zero=True)(x)
    x = tf.keras.layers.Bidirectional(tf.keras.layers.LSTM(64))(x)
    x = tf.keras.layers.Dropout(0.3)(x)
    x = tf.keras.layers.Dense(64, activation="relu")(x)
    outputs = tf.keras.layers.Dense(1, activation="sigmoid", dtype="float32")(x)
    model = tf.keras.Model(inputs, outputs)
    model.compile(
        optimizer=tf.keras.optimizers.Adam(1e-3),
        loss="binary_crossentropy",
        metrics=[tf.keras.metrics.AUC(name="auc"), "accuracy"],
    )
    return model


def run() -> None:
    cfg = load_config()
    if tf.config.list_physical_devices("GPU"):
        tf.keras.mixed_precision.set_global_policy("mixed_float16")
        log.info("GPU detected; mixed_float16 policy enabled.")

    splits = load_splits(cfg)
    text_col, label_col = cfg.ml.text_col, cfg.ml.label_col

    vectorizer = tf.keras.layers.TextVectorization(
        max_tokens=VOCAB_SIZE, output_sequence_length=SEQ_LEN
    )
    vectorizer.adapt(splits["train"][text_col].to_numpy())

    train_ds = _dataset(splits["train"][text_col], splits["train"][label_col], 64, True)
    val_ds = _dataset(splits["val"][text_col], splits["val"][label_col], 64, False)
    test_ds = _dataset(splits["test"][text_col], splits["test"][label_col], 64, False)

    model = _build_model(vectorizer)
    init_mlflow(cfg)
    with mlflow.start_run(run_name="tensorflow-bilstm", tags=standard_tags(cfg)) as run:
        mlflow.log_params({"vocab_size": VOCAB_SIZE, "seq_len": SEQ_LEN, "arch": "bilstm"})
        model.fit(
            train_ds,
            validation_data=val_ds,
            epochs=8,
            callbacks=[
                tf.keras.callbacks.EarlyStopping(
                    monitor="val_auc", mode="max", patience=2, restore_best_weights=True
                ),
                tf.keras.callbacks.ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=1),
            ],
            verbose=2,
        )

        proba = model.predict(test_ds, verbose=0).ravel()
        pred = (proba >= 0.5).astype(int)
        y_true = np.concatenate([y.numpy() for _, y in test_ds])
        metrics = compute_metrics(y_true, pred, proba)
        mlflow.log_metrics({f"test_{k}": v for k, v in metrics.items()})
        log_report("tensorflow-bilstm", y_true, pred)

        mlflow.tensorflow.log_model(model, artifact_path="model")
        mlflow.register_model(
            f"runs:/{run.info.run_id}/model", registered_model_name(cfg, "tensorflow")
        )


if __name__ == "__main__":
    run()
