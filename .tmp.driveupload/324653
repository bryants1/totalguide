#!/usr/bin/env python3
"""
process_state_courses.py
Usage:
  python process_state_courses.py --base ../states/ma

What it does:
- Loads Excel at <BASE>/course_list/USGolfData-WithPlaceDetails_with_urls.xlsx
- For each COURSE_ID in Excel (cCourseNumber), expects:
    reviews/scores/{COURSE_ID}_*_reviews_summary.json
    course_scores/{COURSE_ID}_rubric_output/{COURSE_ID}_rubric.json
    website_data/general/{COURSE_ID}_*coursescrape_structured_formatted.json
    images_elevation/{COURSE_ID}_*/(comprehensive_analysis.json OR analysis_summary.json)
- Builds **technical**, **experience**, and **combined** 5D vectors
- Writes to <BASE>/vectors_golfer_matching/{COURSE_ID}_vectors.json
- If a course is missing a required file, logs an error and continues to the next.
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from datetime import datetime
import argparse
import sys

from golf_vector_system_pinned import MAStateLoader, DualVectorGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("runner-pinned")


def process_all(base: Path) -> None:
    base = Path(base)
    outdir = base / "vectors_golfer_matching"
    outdir.mkdir(parents=True, exist_ok=True)

    loader = MAStateLoader(base)
    df = loader.df  # already validated

    # Iterate EXACT course IDs as listed in Excel
    ids = [str(v).strip().upper() for v in df["cCourseNumber"].tolist()]
    ids = [cid for cid in ids if cid.startswith("MA-")]

    gen = DualVectorGenerator(log_missing=True)

    for cid in ids:
        try:
            course_data, rubric = loader.load_all_inputs(cid)
            vectors = gen.generate_complete_vectors(course_data, rubric)
            vectors["course_id"] = cid
            vectors["course_name"] = course_data.get("course_name")
            vectors["timestamp"] = datetime.now().isoformat()

            # ⬇️ add core facts and a few helpful metrics into the output
            core_keys = [
                "total_length_yards", "par", "slope_rating", "course_rating", "holes",
                "difficulty_score", "avg_elevation_change_m", "avg_wind_kph",
                "pricing_level", "typical_rate", "has_driving_range", "has_practice_green", "has_food_beverage", "course_url", "latitude", "longitude"
            ]
            vectors.update({k: course_data[k] for k in core_keys if k in course_data})

            out_path = outdir / f"{cid}_vectors.json"
            with open(out_path, "w") as f:
                json.dump(vectors, f, indent=2)
            log.info("Wrote %s", out_path)

        except Exception as e:
            log.error("Skipping %s: %s", cid, e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="State base path (e.g., ../states/ma)")
    args = ap.parse_args()

    process_all(Path(args.base))


if __name__ == "__main__":
    main()
