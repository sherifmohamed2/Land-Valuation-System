"""
benchmark_validation_service.py

Post-selection validation of the 5 selected benchmarks.

Checks:
  1. District uniqueness — resolve_duplicate_districts() re-picks lower-priority benchmarks when possible
  2. Remaining duplicate districts → DISTRICT_OVERLAP_UNAVOIDABLE warning
  3. Minimum area spread (VALID_MIN_AREA_DIFF_PCT)
  4. Minimum price/sqm spread (VALID_MIN_PRICE_DIFF_PCT)

All thresholds from settings.
"""

import logging
import copy
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from app.core.config import Settings

logger = logging.getLogger(__name__)

BENCHMARK_TYPES = ["market_average", "prime", "secondary", "large_dev", "emerging"]


def _call_selector(
    btype: str,
    selector: Any,
    df: pd.DataFrame,
    rising_districts: set,
    exclude_ids: Set[int],
    forbidden_districts: Set[str],
) -> Optional[dict]:
    """Invoke a selector with optional re-pick constraints (district de-duplication)."""
    if btype == "emerging":
        return selector.select(
            df,
            rising_districts=rising_districts,
            exclude_ids=exclude_ids,
            forbidden_districts=forbidden_districts,
        )
    return selector.select(df, exclude_ids=exclude_ids, forbidden_districts=forbidden_districts)


class BenchmarkValidationService:
    """Post-selection checks and deterministic district conflict resolution."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def resolve_duplicate_districts(
        self,
        benchmarks: dict[str, Optional[dict]],
        df: pd.DataFrame,
        selectors: dict[str, Any],
        rising_districts: set,
    ) -> Tuple[Dict[str, Optional[dict]], List[str]]:
        """
        Deterministic conflict resolution: when two benchmarks share the same non-null district,
        try replacing the lower-priority benchmark (higher index in BENCHMARK_TYPES) using the
        next-ranked eligible candidate whose district is not used by any other selected benchmark.

        Returns:
            Tuple of (possibly mutated benchmarks dict, human-readable log lines for warnings).

        Note:
            Only non-null ``district`` strings participate in duplicate detection.
        """
        bms: Dict[str, Optional[dict]] = {
            k: (copy.deepcopy(v) if v else None) for k, v in benchmarks.items()
        }
        resolution_warnings: List[str] = []
        exhausted: Set[Tuple[str, str]] = set()

        max_passes = 100
        for _ in range(max_passes):
            non_null = {k: v for k, v in bms.items() if v is not None}
            by_district: dict[str, list[str]] = {}
            for btype, v in non_null.items():
                d = v.get("district")
                if d:
                    by_district.setdefault(d, []).append(btype)
            dup_entries = {d: ts for d, ts in by_district.items() if len(ts) > 1}
            if not dup_entries:
                break

            progressed = False
            for d in sorted(dup_entries.keys()):
                types_at = dup_entries[d]
                for replace_type in sorted(
                    types_at,
                    key=lambda t: BENCHMARK_TYPES.index(t),
                    reverse=True,
                ):
                    key = (d, replace_type)
                    if key in exhausted:
                        continue

                    other_districts: Set[str] = set()
                    exclude_ids: Set[int] = set()
                    for bt, v in non_null.items():
                        if v.get("land_transaction_id") is not None:
                            exclude_ids.add(int(v["land_transaction_id"]))
                        if bt == replace_type:
                            continue
                        od = v.get("district")
                        if od:
                            other_districts.add(od)

                    sel = selectors.get(replace_type)
                    if sel is None:
                        exhausted.add(key)
                        continue

                    alt = _call_selector(
                        replace_type,
                        sel,
                        df,
                        rising_districts,
                        exclude_ids,
                        other_districts,
                    )
                    if alt is not None:
                        bms[replace_type] = alt
                        resolution_warnings.append(
                            f"DISTRICT_RESOLVED: re-selected benchmark {replace_type!r} "
                            f"to avoid duplicate district {d!r}."
                        )
                        exhausted.clear()
                        progressed = True
                        break
                    exhausted.add(key)
                if progressed:
                    break

            if not progressed:
                for dup_d in sorted(dup_entries.keys()):
                    resolution_warnings.append(
                        f"DISTRICT_DUPLICATE_UNRESOLVED: benchmarks share district {dup_d!r}; "
                        "no alternate candidate satisfied uniqueness."
                    )
                break

        return bms, resolution_warnings

    def validate(self, benchmarks: dict[str, Optional[dict]]) -> dict:
        """
        Validate counts, remaining district overlap, and min area/price spread.

        Returns:
            ``is_valid`` is False if hard flags (e.g. zero benchmarks) exist; warnings cover
            soft issues (overlap after resolution, low spread).
        """
        flags = []
        warnings = []

        non_null = {k: v for k, v in benchmarks.items() if v is not None}
        benchmarks_found = len(non_null)

        if benchmarks_found == 0:
            flags.append("NO_BENCHMARKS_SELECTED: zero benchmarks found in this run.")
        elif benchmarks_found < 3:
            flags.append(
                f"LOW_BENCHMARK_COUNT: only {benchmarks_found}/5 benchmarks selected — "
                "valuation confidence is reduced."
            )

        districts = [v["district"] for v in non_null.values() if v.get("district")]
        if len(districts) != len(set(districts)):
            warnings.append(
                "DISTRICT_OVERLAP_UNAVOIDABLE: two or more benchmarks still share the same district "
                "after conflict resolution."
            )

        areas = [v["area_sqm"] for v in non_null.values() if v.get("area_sqm")]
        if len(areas) >= 2:
            area_spread = (max(areas) - min(areas)) / max(areas)
            if area_spread < self.settings.VALID_MIN_AREA_DIFF_PCT:
                warnings.append(
                    f"LOW_AREA_SPREAD: area spread is {area_spread*100:.1f}% "
                    f"(min required: {self.settings.VALID_MIN_AREA_DIFF_PCT*100:.0f}%)."
                )

        prices = [v["price_per_sqm"] for v in non_null.values() if v.get("price_per_sqm")]
        if len(prices) >= 2:
            price_spread = (max(prices) - min(prices)) / max(prices)
            if price_spread < self.settings.VALID_MIN_PRICE_DIFF_PCT:
                warnings.append(
                    f"LOW_PRICE_SPREAD: price/sqm spread is {price_spread*100:.1f}% "
                    f"(min required: {self.settings.VALID_MIN_PRICE_DIFF_PCT*100:.0f}%)."
                )

        is_valid = len(flags) == 0

        logger.info(
            f"Validation: is_valid={is_valid}, found={benchmarks_found}/5, "
            f"flags={flags}, warnings={warnings}"
        )

        return {
            "is_valid": is_valid,
            "benchmarks_found": benchmarks_found,
            "flags": flags,
            "warnings": warnings,
        }
