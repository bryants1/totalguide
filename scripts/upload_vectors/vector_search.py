import os
import json
import pandas as pd
import streamlit as st
import warnings
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

warnings.filterwarnings("ignore", category=DeprecationWarning)

# === CONFIG ===
QDRANT_URL = "https://18dc1c0d-e170-4ffc-b546-f02e7c59172a.us-west-1-0.aws.cloud.qdrant.io"
QDRANT_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.BAxtyJ-tgho1j6JqzHTRtI-SCNsKzzmt6vN-pZcbTg4"
COLLECTION_NAME = "golf_totalguide"
VECTOR_DIR = "../states/ma/vectors"
TOP_K = 5
VECTOR_SIZE = 107

def extract_numeric_features(d):
    """Extract numeric features recursively from nested dict"""
    features = []
    for key, value in d.items():
        if isinstance(value, dict):
            features.extend(extract_numeric_features(value))
        elif isinstance(value, (int, float, bool)):
            features.append(float(value))
    return features

def safe_get_nested(data, keys, default="N/A"):
    """Safely get nested dictionary values"""
    try:
        result = data
        for key in keys:
            result = result[key]
        return result
    except (KeyError, TypeError):
        return default

def format_value(value):
    """Format values for display"""
    if isinstance(value, bool):
        return "✅" if value else "❌"
    elif isinstance(value, float):
        return f"{value:.2f}"
    else:
        return str(value)

def main():
    st.set_page_config(page_title="Golf Course Matcher", layout="wide")
    st.title("⛳️ Golf Course Matcher")
    st.markdown("Find golf courses similar to your selected course using AI vector similarity.")

    # Initialize connection
    try:
        client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
        # Test connection
        collection_info = client.get_collection(COLLECTION_NAME)
        st.success(f"✅ Connected to Qdrant. Collection has {collection_info.vectors_count} courses.")
    except Exception as e:
        st.error(f"❌ Failed to connect to Qdrant: {str(e)}")
        st.stop()

    # Load local course files for reference
    if not os.path.exists(VECTOR_DIR):
        st.error(f"❌ Vector directory not found: {VECTOR_DIR}")
        st.stop()

    valid_files = []
    vector_map = {}
    course_map = {}

    for f in sorted(os.listdir(VECTOR_DIR)):
        if f.endswith(".json"):
            try:
                file_path = os.path.join(VECTOR_DIR, f)
                with open(file_path, "r") as j:
                    data = json.load(j)
                    vector = extract_numeric_features(data)
                    if len(vector) == VECTOR_SIZE:
                        valid_files.append(f)
                        vector_map[f] = vector
                        course_map[f] = data
            except Exception as e:
                st.warning(f"⚠️ Skipped {f}: {str(e)}")
                continue

    if not valid_files:
        st.error("❌ No valid 107-dimension vector files found in directory.")
        st.stop()

    # Course selection
    st.subheader("🎯 Select Reference Course")
    selected_file = st.selectbox(
        "Choose a course to find similar matches:",
        valid_files,
        format_func=lambda x: course_map[x].get("course_name", x.replace(".json", "").replace("_", " ").title())
    )

    course_data = course_map[selected_file]
    query_vector = vector_map[selected_file]
    course_name = course_data.get("course_name", "Unknown Course")

    # Display selected course info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Course", course_name)
    with col2:
        par = safe_get_nested(course_data, ["course_fundamentals", "total_par"], "N/A")
        st.metric("Par", par)
    with col3:
        length = safe_get_nested(course_data, ["course_fundamentals", "total_length_yards"], "N/A")
        st.metric("Length (yards)", length)

    # Filters section
    st.subheader("🔍 Search Filters")
    col1, col2 = st.columns(2)

    with col1:
        filter_by_state = st.checkbox("Filter by state")
        selected_state = None
        if filter_by_state:
            selected_state = st.selectbox("State:", ["MA", "CT", "NH", "VT", "RI", "ME"])

    with col2:
        filter_by_type = st.checkbox("Filter by course type")
        selected_type = None
        if filter_by_type:
            selected_type = st.selectbox("Course Type:", ["Public", "Private", "Resort"])

    # Build query filter
    query_filter = None
    filter_conditions = []

    if filter_by_state and selected_state:
        filter_conditions.append(
            FieldCondition(key="location_economics_state", match=MatchValue(value=selected_state))
        )

    if filter_by_type and selected_type:
        type_key = f"location_economics_course_type_{selected_type.lower()}"
        filter_conditions.append(
            FieldCondition(key=type_key, match=MatchValue(value=True))
        )

    if filter_conditions:
        query_filter = Filter(must=filter_conditions)

    # Perform search
    try:
        with st.spinner("🔍 Searching for similar courses..."):
            results = client.search(
                collection_name=COLLECTION_NAME,
                query_vector=query_vector,
                limit=TOP_K,
                with_payload=True,
                query_filter=query_filter
            )
    except Exception as e:
        st.error(f"❌ Search failed: {str(e)}")
        st.stop()

    # Display results
    st.subheader(f"🏆 Top {len(results)} Most Similar Courses to {course_name}")

    if not results:
        st.warning("No similar courses found with the current filters.")
        return

    # Results overview table
    overview_data = []
    for i, hit in enumerate(results, 1):
        payload = hit.payload or {}
        match_name = payload.get("course_name", "Unknown Course")
        match_state = payload.get("location_economics_state", "Unknown")
        score = hit.score

        overview_data.append({
            "Rank": i,
            "Course Name": match_name,
            "State": match_state,
            "Similarity Score": f"{score:.4f}"
        })

    st.table(pd.DataFrame(overview_data))

    # Detailed comparisons
    st.subheader("📊 Detailed Course Comparisons")

    # Define comparison features with display names
    comparison_features = [
        ("course_fundamentals_course_rating", "Course Rating", ["course_fundamentals", "course_rating"]),
        ("course_fundamentals_slope_rating", "Slope Rating", ["course_fundamentals", "slope_rating"]),
        ("course_fundamentals_total_length_yards", "Total Length", ["course_fundamentals", "total_length_yards"]),
        ("strategic_complexity_total_bunkers", "Total Bunkers", ["strategic_complexity", "total_bunkers"]),
        ("landing_zone_difficulty_avg_fairway_width", "Avg Fairway Width", ["landing_zone_difficulty", "avg_fairway_width"]),
        ("player_experience_ratings_overall_rating", "Overall Rating", ["player_experience_ratings", "overall_rating"]),
        ("player_experience_ratings_difficulty_rating", "Difficulty Rating", ["player_experience_ratings", "difficulty_rating"]),
        ("weather_characteristics_windy_days_percent", "Windy Days %", ["weather_characteristics", "windy_days_percent"]),
        ("location_economics_pricing_level", "Pricing Level", ["location_economics", "pricing_level"])
    ]

    for i, hit in enumerate(results, 1):
        payload = hit.payload or {}
        match_name = payload.get("course_name", f"Course #{i}")
        match_state = payload.get("location_economics_state", "Unknown")
        score = hit.score

        with st.expander(f"🏌️ #{i}: {match_name} ({match_state}) - Score: {score:.4f}"):
            # Create comparison table
            comparison_data = []

            for flat_key, display_name, nested_keys in comparison_features:
                selected_value = safe_get_nested(course_data, nested_keys)
                match_value = payload.get(flat_key, "N/A")

                comparison_data.append({
                    "Feature": display_name,
                    f"{course_name}": format_value(selected_value),
                    f"{match_name}": format_value(match_value)
                })

            comparison_df = pd.DataFrame(comparison_data)
            st.table(comparison_df)

    # Download results
    st.subheader("💾 Export Results")
    if st.button("📥 Download Results as CSV"):
        df = pd.DataFrame(overview_data)
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"similar_courses_to_{course_name.replace(' ', '_')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
