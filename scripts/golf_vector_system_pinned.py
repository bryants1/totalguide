#!/usr/bin/env python3
"""
golf_vector_system_pinned.py
Deterministic vector system for one state folder (e.g., ../states/ma)

Folder layout assumed under <BASE>:
  course_list/USGolfData-WithPlaceDetails_with_urls.xlsx
  reviews/scores/{COURSE_ID}_*_reviews_summary.json
  course_scores/{COURSE_ID}_rubric_output/{COURSE_ID}_rubric.json
  website_data/general/{COURSE_ID}_*coursescrape_structured_formatted.json
  images_elevation/{COURSE_ID}_*/analysis_summary.json

Rules:
- Excel REQUIRED; course row matched by exact cCourseNumber == COURSE_ID
- No recursion; only list() the known directory and pick the deterministic
  file that matches the naming rule (start-with COURSE_ID and required suffix).
- If any file for a course is missing, raise for that course (the runner may
  catch and continue to next course).
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List
from datetime import datetime
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("gvs-pinned")

# ===== Excel hard requirement =====
try:
    import pandas as pd
except Exception as e:
    raise RuntimeError("pandas/openpyxl required. Install: pip install pandas openpyxl") from e


# =========================
# Vector math (kept slim, deterministic)
# =========================
RECOVERY_WEIGHTS = {'terminal': 0.35, 'rough': 0.25, 'bunker': 0.20, 'tree': 0.20}
SHAPE_WEIGHTS     = {'doglegs': 0.30, 'elevation': 0.25, 'wind': 0.20, 'approaches': 0.25}
WALK_WEIGHTS      = {'elevation': 0.40, 'terrain': 0.25, 'distances': 0.20, 'routing': 0.15}
COND_WEIGHTS      = {'fairways': 0.30, 'greens': 0.40, 'bunkers': 0.15, 'tees': 0.15}
AMEN_WEIGHTS      = {'range': 0.25, 'practice': 0.20, 'food': 0.25, 'facilities': 0.15, 'shop': 0.15}

class TechnicalVector:
    def __init__(self, log_missing: bool = False):
        self.log_missing = log_missing
        self.missing_fields: set[str] = set()
        self.version = "3.3.0"

    def _get(self, d: Dict, k: str, default):
        if k not in d and self.log_missing:
            self.missing_fields.add(k)
        return d.get(k, default)

    def _clamp(self, v: float, a=0.0, b=1.0) -> float:
        return max(a, min(b, v))

    def _calc_effective_length(self, data: Dict) -> Dict:
        base = self._get(data, "total_length_yards", 0)
        if base == 0:
            base = self._get(data, "par", 72) * 87
        wind = self._get(data, "avg_wind_kph", 15)
        wind_factor = 1.10 if wind > 25 else 1.05 if wind > 20 else 1.0
        ctype = str(self._get(data, "course_type", "parkland")).lower()
        roll = {"links":0.92,"desert":0.95,"parkland":1.0,"forest":1.02,"mountain":1.03,"heathland":0.96}.get(ctype,1.0)
        forced = self._get(data, "forced_carry_count", 0)
        waterh = self._get(data, "water_holes_count", 0)
        eff = base * wind_factor * roll + forced*15 + waterh*10
        if eff < 3500: s=0.10
        elif eff < 5000: s=0.10+(eff-3500)/1500*0.15
        elif eff < 5500: s=0.25+(eff-5000)/500*0.10
        elif eff < 6000: s=0.35+(eff-5500)/500*0.15
        elif eff < 6500: s=0.50+(eff-6000)/500*0.20
        elif eff < 7000: s=0.70+(eff-6500)/500*0.15
        else: s=0.85+np.tanh((eff-7000)/1000)*0.10
        return {"value": self._clamp(s,0.10,0.95), "effective_yards": eff, "base_yards": base}

    def _calc_difficulty(self, d: Dict, holes: int) -> Dict:
        slope = self._get(d,"slope_rating",113); rating = self._get(d,"course_rating",72); par = self._get(d,"par",72)
        slope_n = self._clamp((slope-55)/100)
        rvp = self._clamp(((rating-par)+5)/10) if rating>10 else 0.5
        player = (self._get(d,"difficulty_rating",5.0)/10) if d.get("difficulty_rating") else 0.5
        analysis = self._get(d,"difficulty_score",50)/100
        ch = self._get(d,"challenging_holes",3) + 2*self._get(d,"extreme_difficulty_holes",0)
        challenge = self._clamp(ch/8)
        wind = self._clamp(self._get(d,"avg_wind_kph",15)/40)
        val = slope_n*0.30 + rvp*0.20 + player*0.20 + analysis*0.15 + challenge*0.10 + wind*0.05
        return {"value": self._clamp(val,0.10,0.95)}

    def _calc_recovery(self, d: Dict, holes: int) -> Dict:
        holes = max(1, holes)
        water = self._get(d,"water_holes_count",4); oob=self._get(d,"out_of_bounds_holes",2); carries=self._get(d,"forced_carry_count",2)
        term = self._clamp(np.tanh(((water+oob+carries*0.5)/holes)*2.5))*RECOVERY_WEIGHTS['terminal']
        rough = {"light":0.20,"medium":0.50,"heavy":0.80,"extreme":0.95,"native":0.90,"fescue":0.85}.get(str(self._get(d,"rough_difficulty","medium")).lower(),0.50)
        rough *= RECOVERY_WEIGHTS['rough']
        depth = {"shallow":0.3,"medium":0.5,"deep":0.8}.get(str(self._get(d,"bunker_depth","medium")).lower(),0.5)
        pot   = np.tanh(self._get(d,"pot_bunkers_count",0)/holes*3)
        bunk  = (depth*0.7 + pot*0.3)*RECOVERY_WEIGHTS['bunker']
        tdens = {"sparse":0.30,"moderate":0.60,"dense":0.85,"forest":0.95}.get(str(self._get(d,"tree_density","moderate")).lower(),0.60)
        lanes = {"good":0.7,"mixed":1.0,"bad":1.3,"none":1.5}.get(str(self._get(d,"tree_play_lanes","mixed")).lower(),1.0)
        tree  = self._clamp(tdens*lanes)*RECOVERY_WEIGHTS['tree']
        val = term+rough+bunk+tree
        return {"value": self._clamp(val,0.10,0.95)}

    def _calc_shape(self, d: Dict, holes: int) -> Dict:
        holes = max(1, holes)
        tot=self._get(d,"total_doglegs",4); left=self._get(d,"left_doglegs",2); right=self._get(d,"right_doglegs",2); hard=self._get(d,"hard_doglegs",1)
        dog = self._clamp(np.tanh((tot/holes)*2) + np.tanh(hard/6)*0.3)*SHAPE_WEIGHTS['doglegs']
        if "elevation_variety_score" in d:
            elev_var = d["elevation_variety_score"]
        else:
            up=self._get(d,"uphill_holes",3); dn=self._get(d,"downhill_holes",3); avg=self._get(d,"avg_elevation_change_m",5); eg=self._get(d,"elevated_greens_count",4)
            count = (up+dn+eg*0.5)/holes; elev_var = (np.tanh(count*1.5)*0.6 + np.tanh(avg/12)*0.4)
        elev = self._clamp(elev_var)*SHAPE_WEIGHTS['elevation']
        wind = {"low":0.20,"moderate":0.50,"high":0.80,"extreme":0.95}.get(str(self._get(d,"wind_exposure","moderate")).lower(),0.50)*SHAPE_WEIGHTS['wind']
        ff=self._get(d,"false_fronts_count",2); mt=self._get(d,"multi_tier_greens_count",4); cr=self._get(d,"crowned_greens_count",2)
        app = self._clamp(np.tanh(((ff+mt+cr)/holes)*1.5))*SHAPE_WEIGHTS['approaches']
        return {"value": self._clamp(dog+elev+wind+app,0.10,0.95)}

    def _calc_walk(self, d: Dict, holes: int) -> Dict:
        holes = max(1, holes)
        if d.get("walkability_elevation_score") is not None:
            elev_score = float(d["walkability_elevation_score"])
        else:
            total = self._get(d,"total_elevation_change_m",0); per = total/holes
            if   per < 1.5: elev_score=0.95
            elif per < 3.0: elev_score=0.85
            elif per < 4.5: elev_score=0.70
            elif per < 6.0: elev_score=0.55
            elif per < 8.0: elev_score=0.40
            elif per <10.0: elev_score=0.25
            else:           elev_score=0.15
        elev_score = max(0.10, elev_score - min(0.20, self._get(d,"extreme_difficulty_holes",0)*0.05))
        elev = elev_score*WALK_WEIGHTS['elevation']
        terr = {"links":0.90,"heathland":0.85,"parkland":0.70,"meadow":0.75,"forest":0.60,"desert":0.50,"mountain":0.30,"canyon":0.20}.get(str(self._get(d,"terrain_type","parkland")).lower(),0.60)
        terr *= WALK_WEIGHTS['terrain']
        g2t=self._get(d,"avg_green_to_tee_distance",50)
        dist = (1.00 if g2t<50 else 0.90 if g2t<100 else 0.75 if g2t<150 else 0.60 if g2t<200 else 0.40)*WALK_WEIGHTS['distances']
        rout = {"returning":1.00,"semi-returning":0.90,"continuous":0.85,"spread":0.70}.get(str(self._get(d,"routing_type","returning")).lower(),0.85)*WALK_WEIGHTS['routing']
        base = self._get(d,"total_length_yards",6200)
        penalty = 0.20 if base>7200 else 0.10 if base>6800 else 0.05 if base>6400 else 0.0
        val = max(0.10, elev+terr+dist+rout - penalty)
        return {"value": self._clamp(val,0.10,0.95)}

    def calculate(self, data: Dict) -> Dict:
        self.missing_fields.clear()
        holes = max(1, self._get(data,"holes",18))
        t1=self._calc_effective_length(data)
        t2=self._calc_difficulty(data, holes)
        t3=self._calc_recovery(data, holes)
        t4=self._calc_shape(data, holes)
        t5=self._calc_walk(data, holes)
        raw = np.array([t1["value"],t2["value"],t3["value"],t4["value"],t5["value"]])
        norm = raw/np.linalg.norm(raw) if np.linalg.norm(raw)>0 else raw
        return {
            "vector_5d": norm.tolist(),
            "vector_5d_raw": raw.tolist(),
            "traits": {"effective_length":t1,"difficulty_level":t2,"recovery_difficulty":t3,"shot_shape_demand":t4,"walkability":t5},
            "trait_values_normalized": {
                "effective_length": float(norm[0]), "difficulty_level": float(norm[1]),
                "recovery_difficulty": float(norm[2]), "shot_shape_demand": float(norm[3]),
                "walkability": float(norm[4])
            },
            "missing_fields": sorted(self.missing_fields) or None,
            "metadata": {"type":"technical","version":self.version,"dimensions":5}
        }

class ExperienceVector:
    def __init__(self, log_missing: bool = False):
        self.log_missing = log_missing
        self.missing_fields: set[str] = set()
        self.version = "3.3.0"

    def _get(self, d,k,default=None):
        if k not in d and self.log_missing:
            self.missing_fields.add(k)
        return d.get(k, default)

    def _clamp(self,v,a=0.0,b=1.0): return max(a,min(b,v))

    def _conditions(self, d: Dict, r: Optional[Dict]) -> Dict:
        score = 0.5
        if r and "scores" in r:
            s = r["scores"].get("Course Conditions", {})
            fair=s.get("Fairways",5)/10; gre=s.get("Greens",5)/10; bun=s.get("Bunkers",5)/10; tee=s.get("Tee Boxes",5)/10
            score = fair*COND_WEIGHTS['fairways'] + gre*COND_WEIGHTS['greens'] + bun*COND_WEIGHTS['bunkers'] + tee*COND_WEIGHTS['tees']
        rc = self._get(d,"conditions_rating",None)
        if rc:
            score = score*0.6 + (rc/5)*0.4 if r else (rc/5)
        return {"value": self._clamp(score,0.10,0.95)}

    def _amenities(self, d: Dict, r: Optional[Dict]) -> Dict:
        score = 0.5
        if r and "scores" in r:
            s = r["scores"].get("Amenities", {})
            rng=s.get("Driving Range",3)/5; prac=s.get("Putting & Short Game Areas",3)/5
            food=s.get("Snack Bar-1, Snack Bar w/ Alcohol-2, Grill w/ Alcohol-3, Full Bar & Lounge-4, Full Service Restaurant-5",3)/5
            fac=s.get("Locker room & Showers",3)/5; shop=s.get("Pro-shop",3)/5
            score = rng*AMEN_WEIGHTS['range'] + prac*AMEN_WEIGHTS['practice'] + food*AMEN_WEIGHTS['food'] + fac*AMEN_WEIGHTS['facilities'] + shop*AMEN_WEIGHTS['shop']
        if not r:
            br = 1.0 if d.get("has_driving_range") else 0.3
            bp = 1.0 if d.get("has_practice_green") else 0.3
            bf = 1.0 if d.get("has_food_beverage") else 0.3
            score = br*0.33 + bp*0.33 + bf*0.34
        return {"value": self._clamp(score,0.10,0.95)}

    def _service(self, d: Dict, r: Optional[Dict]) -> Dict:
        score = 0.5
        if r and "scores" in r:
            s=r["scores"].get("Player Experience",{})
            staff=s.get("Staff Friendliness, After-Round Experience",3)/5
            pace =s.get("Pace of Play",3)/5
            score = staff*0.7 + pace*0.3
        fr = self._get(d,"friendliness_rating",None)
        pr = self._get(d,"pace_rating",None)
        if fr: score = score*0.6 + (fr/5)*0.4 if r else (fr/5)
        if pr: score = score*0.8 + (pr/5)*0.2
        return {"value": self._clamp(score,0.10,0.95)}

    def _value(self, d: Dict, r: Optional[Dict]) -> Dict:
        score = 0.5
        if r and "scores" in r:
            v = r["scores"].get("Player Experience",{}).get("Green Fees vs. Quality",3)/5
            score = v
        vr = self._get(d,"value_rating",None)
        if vr: score = score*0.6 + (vr/5)*0.4 if r else (vr/5)
        price = self._get(d,"pricing_level",3)
        if price==5: score *= 0.7
        elif price==4: score *= 0.85
        elif price==2: score = min(score*1.15,0.95)
        elif price==1: score = min(score*1.30,0.95)
        return {"value": self._clamp(score,0.10,0.95)}

    def _ambiance(self, d: Dict, r: Optional[Dict]) -> Dict:
        score = 0.5
        if r and "scores" in r:
            s = r["scores"].get("Course Layout & Design",{})
            score = (s.get("Overall feel / Scenery",3)/5)*0.4 + (s.get("Signature Holes / Quirky/Fun Design Features",3)/5)*0.3 + (s.get("Shot Variety / Hole Uniqueness",3)/5)*0.3
        if d.get("ocean_views"): score = min(score+0.15,0.95)
        elif d.get("scenic_views"): score = min(score+0.10,0.95)
        if (d.get("signature_holes_count") or 0) > 3: score = min(score+0.10,0.95)
        return {"value": self._clamp(score,0.10,0.95)}

    def calculate(self, data: Dict, rubric: Optional[Dict]) -> Dict:
        self.missing_fields.clear()
        t1=self._conditions(data,rubric)
        t2=self._amenities(data,rubric)
        t3=self._service(data,rubric)
        t4=self._value(data,rubric)
        t5=self._ambiance(data,rubric)
        raw=np.array([t1["value"],t2["value"],t3["value"],t4["value"],t5["value"]])
        norm=raw/np.linalg.norm(raw) if np.linalg.norm(raw)>0 else raw
        return {
            "vector_5d": norm.tolist(),
            "vector_5d_raw": raw.tolist(),
            "traits": {"conditions_quality":t1,"amenities_quality":t2,"service_quality":t3,"value_rating":t4,"ambiance_scenery":t5},
            "trait_values_normalized": {
                "conditions_quality":float(norm[0]),"amenities_quality":float(norm[1]),
                "service_quality":float(norm[2]),"value_rating":float(norm[3]),"ambiance_scenery":float(norm[4])
            },
            "missing_fields": sorted(self.missing_fields) or None,
            "metadata": {"type":"experience","version":self.version,"dimensions":5,"has_rubric": rubric is not None}
        }

class DualVectorGenerator:
    def __init__(self, log_missing: bool=False):
        self.technical_gen = TechnicalVector(log_missing)
        self.experience_gen = ExperienceVector(log_missing)

    def _combine(self, tech: Dict, exp: Dict, tw: float = 0.6) -> Dict:
        t=np.array(tech["vector_5d"]); e=np.array(exp["vector_5d"])
        c=t*tw + e*(1-tw)
        c=c/np.linalg.norm(c) if np.linalg.norm(c)>0 else c
        return {"vector_5d": c.tolist(), "tech_weight": tw, "exp_weight": 1-tw}

    def generate_complete_vectors(self, course_data: Dict, rubric_data: Optional[Dict]) -> Dict:
        tech=self.technical_gen.calculate(course_data)
        exp =self.experience_gen.calculate(course_data, rubric_data)
        return {"technical": tech, "experience": exp, "combined": self._combine(tech,exp)}


# =========================
# Deterministic per-course loader for MA folder layout
# =========================
class MAStateLoader:
    """Given a base path (e.g., ../states/ma), load the five inputs for a COURSE_ID."""
    def __init__(self, base_path: Path):
        self.base = Path(base_path)
        self.excel_path = self.base / "course_list" / "USGolfData-WithPlaceDetails_with_urls.xlsx"
        if not self.excel_path.exists():
            raise FileNotFoundError(f"Excel not found: {self.excel_path}")
        self.df = pd.read_excel(self.excel_path)
        if "cCourseNumber" not in self.df.columns:
            raise ValueError("Excel missing cCourseNumber")

    def _excel_row(self, course_id: str):
        ser = self.df["cCourseNumber"].astype(str).str.strip().str.upper()
        mask = ser == course_id.upper()
        if not mask.any():
            raise KeyError(f"Excel row not found for {course_id}")
        return self.df[mask].iloc[0]

    def _extract_excel_core(self, row) -> Dict[str, Any]:
        """Extract core facts from an Excel row (pandas Series)."""
        out: Dict[str, Any] = {}

        def _has(col: str) -> bool:
            return col in row and pd.notna(row[col]) and str(row[col]).strip() != ""

        # ---- Length (e.g., "6200-6600" -> 6600) ----
        if _has("Length"):
            s = str(row["Length"]).replace(",", "").strip()
            if "-" in s:
                s = s.split("-")[-1].strip()
            try:
                out["total_length_yards"] = int(float(s))
            except Exception:
                pass

        # ---- Par ----
        if _has("Par"):
            try:
                out["par"] = int(row["Par"])
            except Exception:
                pass

        # ---- Slope (supports "130/125") ----
        if _has("Slope"):
            s = str(row["Slope"]).strip()
            try:
                out["slope_rating"] = (
                    max(float(x) for x in s.split("/") if x.strip())
                    if "/" in s else float(s)
                )
            except Exception:
                pass

        # ---- Course Rating (supports "72.1/70.5") ----
        if _has("Rating"):
            s = str(row["Rating"]).strip()
            try:
                out["course_rating"] = (
                    max(float(x) for x in s.split("/") if x.strip())
                    if "/" in s else float(s)
                )
            except Exception:
                pass

        # ---- Holes ----
        if _has("CoursesMasterT::TotalHoles"):
            try:
                out["holes"] = int(row["CoursesMasterT::TotalHoles"])
            except Exception:
                pass

        # ---- Course name ----
        if _has("CoursesMasterT::CourseName"):
            out["course_name"] = str(row["CoursesMasterT::CourseName"]).strip()

        # ---- URL (pick first populated; ensure scheme) ----
        url_columns = [
            "Website", "URL", "CourseURL", "WebsiteURL", "PublicWebsite",
            "CoursesMasterT::Website", "CoursesMasterT::URL",
            "GoogleMapsLink",  # last resort
            "GolfNow URL", "GolfPassURL"
        ]
        for col in url_columns:
            if _has(col):
                url = str(row[col]).strip()
                # Normalize simple domains to https://
                if not url.lower().startswith(("http://", "https://")):
                    url = "https://" + url
                out["course_url"] = url
                break

        # ---- Latitude / Longitude (from Excel) ----
        # Accept common header variants
        def _first_float(cols: List[str]) -> Optional[float]:
            for c in cols:
                if _has(c):
                    try:
                        return float(row[c])
                    except Exception:
                        pass
            return None

        lat = _first_float(["Latitude", "Lat", "CoursesMasterT::Latitude"])
        lon = _first_float(["Longitude", "Lon", "Lng", "CoursesMasterT::Longitude"])

        # Swap correction if obviously flipped (lat should be in [-90,90])
        if lat is not None and lon is not None:
            if abs(lat) > 90 and abs(lon) <= 90:
                lat, lon = lon, lat
            if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                out["latitude"] = lat
                out["longitude"] = lon

        return out

    def _find_single_by_rule(self, folder: Path, course_id: str, must_end: str) -> Path:
        """List folder and pick the file that starts with '<COURSE_ID>_' and endswith must_end."""
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder}")
        cprefix = f"{course_id}_"
        candidates = [p for p in folder.iterdir() if p.is_file() and p.name.startswith(cprefix) and p.name.endswith(must_end)]
        if len(candidates) != 1:
            raise FileNotFoundError(f"Expected exactly one file in {folder} with prefix {cprefix} and suffix {must_end}; found {len(candidates)}")
        return candidates[0]

    def _find_subdir_by_rule(self, parent: Path, course_id: str) -> Path:
        """List parent and pick the subdir that starts with '<COURSE_ID>_'."""
        if not parent.exists():
            raise FileNotFoundError(f"Folder not found: {parent}")
        cprefix = f"{course_id}_"
        candidates = [p for p in parent.iterdir() if p.is_dir() and p.name.startswith(cprefix)]
        if len(candidates) != 1:
            raise FileNotFoundError(f"Expected exactly one subdir in {parent} starting {cprefix}; found {len(candidates)}")
        return candidates[0]

    def load_all_inputs(self, course_id: str) -> Dict[str, Any]:
        # ---------- Excel (REQUIRED) ----------
        row = self._excel_row(course_id)  # raises if not found
        core = self._extract_excel_core(row)

        # ---------- Reviews (REQUIRED) ----------
        reviews_dir = self.base / "reviews" / "scores"
        reviews_path = self._find_single_by_rule(reviews_dir, course_id, "_reviews_summary.json")
        with open(reviews_path, "r") as f:
            reviews = json.load(f)

        # ---------- Rubric (REQUIRED) ----------
        rubric_path = self.base / "course_scores" / f"{course_id}_rubric_output" / f"{course_id}_rubric.json"
        if not rubric_path.exists():
            raise FileNotFoundError(f"Rubric not found: {rubric_path}")
        with open(rubric_path, "r") as f:
            rubric = json.load(f)

        # ---------- Coursescrape (REQUIRED) ----------
        # Using your suffix exactly as requested
        scrape_dir = self.base / "website_data" / "general"
        scrape_path = self._find_single_by_rule(scrape_dir, course_id, "_coursescrape_structured.json")
        with open(scrape_path, "r") as f:
            coursescrape = json.load(f)

        # ---------- analysis_summary.json (REQUIRED) ----------
        images_root = self.base / "images_elevation"
        course_dir = self._find_subdir_by_rule(images_root, course_id)
        anal_path = course_dir / "analysis_summary.json"
        if not anal_path.exists():
            raise FileNotFoundError(f"Missing required file: {anal_path}")
        with open(anal_path, "r") as f:
            analysis_summary = json.load(f)

        # ---------- Seed course_data with Excel core ----------
        course_data: Dict[str, Any] = {
            "course_id": course_id,
            "timestamp": datetime.now().isoformat()
        }
        course_data.update(core)

        # ---------- Coursescrape: light, non-overriding fields ----------
        if isinstance(coursescrape, dict):
            gi = coursescrape.get("general_info", {})
            if isinstance(gi, dict):
                # Course name
                nm = gi.get("name")
                if isinstance(nm, dict): nm = nm.get("value")
                if isinstance(nm, str) and nm.strip():
                    course_data["course_name"] = nm.strip()

                # Course type
                ct = gi.get("course_type")
                if isinstance(ct, dict): ct = ct.get("value")
                if ct:
                    course_data["course_type"] = str(ct).lower()

                # PRICING LEVEL  ⬅️  (this is what was missing)
                pl = gi.get("pricing_level")
                if pl is not None:
                    try:
                        if isinstance(pl, dict):
                            # common shape: {"value": 3, "typical_18_hole_rate": 65}
                            val = pl.get("value", None)
                            if val is not None:
                                course_data["pricing_level"] = int(val)
                            if "typical_18_hole_rate" in pl:
                                course_data["typical_rate"] = pl["typical_18_hole_rate"]
                        else:
                            # numeric or numeric string
                            course_data["pricing_level"] = int(float(pl))
                    except Exception:
                        # be tolerant if source is messy
                        pass

        # ---------- Reviews ----------
        if isinstance(reviews, dict):
            cats = reviews.get("form_category_averages", {})
            if isinstance(cats, dict):
                course_data["conditions_rating"]   = cats.get("Conditions")
                course_data["value_rating"]        = cats.get("Value")
                course_data["friendliness_rating"] = cats.get("Friendliness")
                course_data["pace_rating"]         = cats.get("Pace")
                course_data["amenities_rating"]    = cats.get("Amenities")
                if "Difficulty" in cats:
                    course_data["difficulty_rating"] = cats["Difficulty"]
            course_data["overall_rating"]    = reviews.get("overall_rating")
            course_data["recommend_percent"] = reviews.get("recommend_percent")

        # ---------- analysis_summary.json  → map to expected fields ----------
        if isinstance(analysis_summary, dict):
            # 1) Key metrics
            km = analysis_summary.get("key_metrics", {})
            if isinstance(km, dict):
                if "difficulty_score" in km:
                    course_data["difficulty_score"] = km["difficulty_score"]
                if "avg_elevation_change_m" in km:
                    course_data["avg_elevation_change_m"] = km["avg_elevation_change_m"]
                if "total_holes" in km and "holes" not in course_data:
                    course_data["holes"] = km["total_holes"]

            # 2) Terrain / elevation (terrain_profile)
            tp = analysis_summary.get("terrain_profile", {})
            if isinstance(tp, dict):
                if "total_elevation_change_m" in tp:
                    course_data["total_elevation_change_m"] = tp["total_elevation_change_m"]
                if "avg_elevation_change_m" in tp and "avg_elevation_change_m" not in course_data:
                    course_data["avg_elevation_change_m"] = tp["avg_elevation_change_m"]
                # rename/forward known counters
                for src, dst in [
                    ("uphill_holes", "uphill_holes"),
                    ("downhill_holes", "downhill_holes"),
                    ("flat_holes", "flat_holes"),
                    ("extreme_difficulty_holes", "extreme_difficulty_holes"),
                    ("challenging_difficulty_holes", "challenging_holes"),
                ]:
                    if src in tp and tp[src] is not None:
                        course_data[dst] = tp[src]

            # 3) Course strategy (doglegs & bunker bias)
            cs = analysis_summary.get("course_strategy", {})
            if isinstance(cs, dict):
                dog = cs.get("dogleg_analysis", {})
                if isinstance(dog, dict):
                    if "left_doglegs"  in dog: course_data["left_doglegs"]  = dog["left_doglegs"]
                    if "right_doglegs" in dog: course_data["right_doglegs"] = dog["right_doglegs"]
                    if "total_doglegs" in dog: course_data["total_doglegs"] = dog["total_doglegs"]
                bunk = cs.get("bunker_positioning", {})
                if isinstance(bunk, dict) and "course_bunker_bias" in bunk:
                    course_data["bunker_bias"] = bunk["course_bunker_bias"]

            # 4) Weather (weather_profile)
            wp = analysis_summary.get("weather_profile", {})
            if isinstance(wp, dict):
                if "avg_wind_kph" in wp:
                    course_data["avg_wind_kph"] = wp["avg_wind_kph"]
                    k = wp["avg_wind_kph"]
                    if   k < 12: course_data["wind_exposure"] = "low"
                    elif k < 20: course_data["wind_exposure"] = "moderate"
                    elif k < 28: course_data["wind_exposure"] = "high"
                    else:        course_data["wind_exposure"] = "extreme"

            # 5) Course vectors (translate to expected fields)
            cv = analysis_summary.get("course_vectors", {})
            if isinstance(cv, dict):
                # Tree density from coverage %
                tc = cv.get("tree_coverage")
                if tc is not None:
                    if   tc < 30: course_data["tree_density"] = "sparse"
                    elif tc < 50: course_data["tree_density"] = "moderate"
                    elif tc < 70: course_data["tree_density"] = "dense"
                    else:         course_data["tree_density"] = "forest"

                # Openness → tree play lanes
                opn = cv.get("course_openness")
                if opn is not None:
                    if   opn > 70: course_data["tree_play_lanes"] = "good"
                    elif opn > 50: course_data["tree_play_lanes"] = "mixed"
                    elif opn > 30: course_data["tree_play_lanes"] = "bad"
                    else:          course_data["tree_play_lanes"] = "none"

                # Design/routing styles
                ds = cv.get("design_style")
                if ds: course_data["terrain_type"] = str(ds).lower()
                rs = cv.get("routing_style")
                if rs:
                    rs_l = str(rs).lower()
                    if "out_back" in rs_l or "return" in rs_l:
                        course_data["routing_type"] = "returning"
                    elif "continuous" in rs_l:
                        course_data["routing_type"] = "continuous"
                    else:
                        course_data["routing_type"] = "spread"

                # Ball findability → rough difficulty
                bf = cv.get("ball_findability")
                if bf is not None:
                    if   bf > 80: course_data["rough_difficulty"] = "light"
                    elif bf > 60: course_data["rough_difficulty"] = "medium"
                    elif bf > 40: course_data["rough_difficulty"] = "heavy"
                    else:         course_data["rough_difficulty"] = "extreme"

                # Water prominence → rough water-hole estimate (only if not set)
                if "water_holes_count" not in course_data:
                    wprom = cv.get("water_prominence")
                    if wprom is not None:
                        if   wprom < 10: course_data["water_holes_count"] = 0
                        elif wprom < 20: course_data["water_holes_count"] = 2
                        elif wprom < 40: course_data["water_holes_count"] = 4
                        elif wprom < 60: course_data["water_holes_count"] = 6
                        else:            course_data["water_holes_count"] = 9

        return course_data, rubric
