from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class AppConfig:
    """Parameters shared by the notebook and the Databricks App."""

    catalogs: List[str]
    output_schema: str = "governance.trust"
    domain_tag: str = "domain"
    certified_tag: str = "certified"
    pii_tag: str = "pii"
    glossary_table: Optional[str] = None
    freshness_days: int = 7
    lookback_days: int = 30
    run_mode: str = "manual"

    def validate(self) -> None:
        if not self.catalogs:
            raise ValueError("At least one catalog must be provided.")
        output_schema_parts = self.output_schema.split(".") if self.output_schema else []
        if len(output_schema_parts) != 2 or any(not part for part in output_schema_parts):
            raise ValueError("output_schema must be a catalog.schema value, e.g. governance.trust")
        if self.freshness_days <= 0:
            raise ValueError("freshness_days must be greater than zero.")
        if self.lookback_days <= 0:
            raise ValueError("lookback_days must be greater than zero.")

    @classmethod
    def from_dict(cls, data: dict) -> "AppConfig":
        return cls(
            catalogs=[c.strip() for c in data.get("catalogs", "").split(",") if c.strip()],
            output_schema=data.get("output_schema", "governance.trust"),
            domain_tag=data.get("domain_tag", "domain"),
            certified_tag=data.get("certified_tag", "certified"),
            pii_tag=data.get("pii_tag", "pii"),
            glossary_table=data.get("glossary_table") or None,
            freshness_days=int(data.get("freshness_days", 7)),
            lookback_days=int(data.get("lookback_days", 30)),
            run_mode=data.get("run_mode", "manual"),
        )

    def to_dict(self) -> dict:
        return {
            "catalogs": ", ".join(self.catalogs),
            "output_schema": self.output_schema,
            "domain_tag": self.domain_tag,
            "certified_tag": self.certified_tag,
            "pii_tag": self.pii_tag,
            "glossary_table": self.glossary_table,
            "freshness_days": self.freshness_days,
            "lookback_days": self.lookback_days,
            "run_mode": self.run_mode,
        }
