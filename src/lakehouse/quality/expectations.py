"""Lightweight declarative data-quality engine for Spark DataFrames.

Semantics per rule:
  WARN  - log violation metrics, keep all rows.
  DROP  - remove violating rows, log how many were dropped.
  FAIL  - abort the pipeline if the violation ratio exceeds the configured
          threshold (fail-fast to protect downstream consumers).

All results are appended to a Delta audit table (`_meta/dq_audit`) so quality
can be tracked over time and surfaced in BI.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.common.config import Config
from lakehouse.common.logging import get_logger

log = get_logger(__name__)


class Action(str, Enum):
    WARN = "warn"
    DROP = "drop"
    FAIL = "fail"


class DataQualityError(RuntimeError):
    """Raised when a FAIL expectation exceeds the allowed violation threshold."""


@dataclass(frozen=True)
class Expectation:
    name: str
    condition: str  # SQL expression that must evaluate to TRUE for valid rows
    action: Action = Action.WARN


@dataclass(frozen=True)
class ExpectationResult:
    name: str
    action: str
    total_rows: int
    violations: int

    @property
    def violation_pct(self) -> float:
        return 0.0 if self.total_rows == 0 else 100.0 * self.violations / self.total_rows


class ExpectationEngine:
    def __init__(self, cfg: Config, table: str):
        self.cfg = cfg
        self.table = table
        self.fail_threshold_pct = float(cfg.quality.fail_threshold_pct)

    def apply(self, df: DataFrame, rules: list[Expectation]) -> DataFrame:
        results: list[ExpectationResult] = []
        total = df.count()

        for rule in rules:
            violations = df.filter(~F.expr(rule.condition)).count()
            result = ExpectationResult(rule.name, rule.action.value, total, violations)
            results.append(result)
            log.info(
                "[DQ][%s] %s: %d/%d violations (%.2f%%) action=%s",
                self.table,
                rule.name,
                violations,
                total,
                result.violation_pct,
                rule.action.value,
            )
            if rule.action is Action.DROP and violations:
                df = df.filter(F.expr(rule.condition))
            elif rule.action is Action.FAIL and result.violation_pct > self.fail_threshold_pct:
                self._audit(df.sparkSession, results)
                raise DataQualityError(
                    f"[{self.table}] '{rule.name}' violated {result.violation_pct:.2f}% "
                    f"> threshold {self.fail_threshold_pct}%"
                )

        self._audit(df.sparkSession, results)
        return df

    def _audit(self, spark: SparkSession, results: list[ExpectationResult]) -> None:
        if not results:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            (
                now,
                self.cfg.environment,
                self.table,
                r.name,
                r.action,
                r.total_rows,
                r.violations,
                r.violation_pct,
            )
            for r in results
        ]
        audit_df = spark.createDataFrame(
            rows,
            "audit_ts string, environment string, table string, rule string, action string, "
            "total_rows long, violations long, violation_pct double",
        )
        path = f"{self.cfg.paths.meta}/dq_audit"
        audit_df.write.format("delta").mode("append").save(path)
