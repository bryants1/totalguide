import os
import json
import uuid
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance

QDRANT_URL = "https://18dc1c0d-e170-4ffc-b546-f02e7c59172a.us-west-1-0.aws.cloud.qdrant.io"
QDRANT_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.BAxtyJ-tgho1j6JqzHTRtI-SCNsKzzmt6vN-pZcbTg4"
COLLECTION_NAME = "golf_totalguide"
VECTOR_DIR = "../states/ma/vectors"
VECTOR_SIZE = 107
DISTANCE_METRIC = Distance.COSINE

# Connect
client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
client.recreate_collection(
    collection_name=COLLECTION_NAME,
    vectors_config=VectorParams(size=VECTOR_SIZE, distance=DISTANCE_METRIC)
)

# Feature extraction
def extract_numeric_features(d):
    features = []
    for key, value in d.items():
        if isinstance(value, dict):
            features.extend(extract_numeric_features(value))
        elif isinstance(value, (int, float, bool)):
            features.append(float(value))
    return features

# Payload flattening
def flatten_json(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, (int, float, bool, str)):
            items.append((new_key, v))
    return dict(items)

# Load files
points = []
for filename in os.listdir(VECTOR_DIR):
    if filename.endswith(".json"):
        path = os.path.join(VECTOR_DIR, filename)
        with open(path, "r") as f:
            data = json.load(f)

        vector = extract_numeric_features(data)
        if len(vector) != VECTOR_SIZE:
            print(f"❌ Skipping {filename}: vector size {len(vector)} != {VECTOR_SIZE}")
            continue

        # Flatten entire file to payload
        flat_payload = flatten_json(data)

        point = PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_DNS, data.get("course_id", filename))),
            vector=vector,
            payload=flat_payload
        )
        points.append(point)

print(f"✅ Loaded {len(points)} valid vectors.")

if points:
    print(f"Uploading to Qdrant collection '{COLLECTION_NAME}'...")
    client.upsert(collection_name=COLLECTION_NAME, points=points)
    print("✅ Upload complete.")
else:
    print("⚠️ No valid vector files found.")
