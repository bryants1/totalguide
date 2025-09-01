import json

TEXT_TO_NUM = {
    "Excellent": 5, "Good": 4, "Average": 3, "Fair": 2, "Poor": 2, "Very Poor": 1,
    "Moderate": 3, "Somewhat Challenging": 4, "Extremely Challenging": 5,
    None: None
}
CATEGORIES = ["Conditions", "Value", "Layout", "Friendliness", "Pace", "Amenities", "Difficulty"]

def aggregate_numeric_summary(reviews):
    sums = {cat: 0.0 for cat in CATEGORIES}
    counts = {cat: 0.0 for cat in CATEGORIES}
    recs = 0
    overall_ratings_sum = 0
    overall_ratings_count = 0

    for r in reviews:
        if r.get("recommend") is True:
            recs += 1
        orating = r.get("overall_rating")
        if orating is not None:
            try:
                overall_ratings_sum += float(orating)
                overall_ratings_count += 1
            except:
                pass
        cats = r.get("category_ratings", {})
        for cat in CATEGORIES:
            val = cats.get(cat)
            num = TEXT_TO_NUM.get(val, None)
            if num is not None:
                sums[cat] += num
                counts[cat] += 1

    summary = {
        "overall_rating": round(overall_ratings_sum / overall_ratings_count, 2) if overall_ratings_count else None,
        "recommend": round(100 * recs / len(reviews), 1) if reviews else None,
        "category_ratings": {}
    }
    for cat in CATEGORIES:
        avg = sums[cat] / counts[cat] if counts[cat] else None
        summary["category_ratings"][cat] = round(avg, 2) if avg is not None else None
    return summary

if __name__ == "__main__":
    with open("unified_reviews.json") as f:
        all_reviews = json.load(f)["reviews"]
    summary = aggregate_numeric_summary(all_reviews)
    with open("course_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("Saved summary as course_summary.json")
