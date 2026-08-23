"""Small in-memory stand-in for the Tater-owned Face ID API in core unit tests."""

import json
import math
import uuid


SHARED_IDENTITIES_KEY = "tater:face_identities:v1"


def _text(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace").strip()
    return str(value or "").strip()


def identity_rows(client):
    rows = {}
    for raw_id, raw_payload in (client.hgetall(SHARED_IDENTITIES_KEY) or {}).items():
        identity_id = _text(raw_id)
        payload = json.loads(raw_payload) if isinstance(raw_payload, (str, bytes, bytearray)) else dict(raw_payload)
        rows[identity_id] = {**payload, "id": identity_id}
    return rows


def save_identity(identity, client):
    payload = dict(identity)
    identity_id = _text(payload.get("id"))
    payload["id"] = identity_id
    client.hset(SHARED_IDENTITIES_KEY, identity_id, json.dumps(payload))
    return payload


def valid_embedding(raw, dimensions=0):
    if not isinstance(raw, list) or not raw:
        return []
    values = [float(value) for value in raw]
    return values if not dimensions or len(values) == dimensions else []


def cosine_distance(left, right):
    if not left or not right or len(left) != len(right):
        return float("inf")
    dot = sum(float(a) * float(b) for a, b in zip(left, right))
    left_norm = math.sqrt(sum(float(value) ** 2 for value in left))
    right_norm = math.sqrt(sum(float(value) ** 2 for value in right))
    return 1.0 - dot / (left_norm * right_norm) if left_norm and right_norm else float("inf")


def reference_embeddings(identity):
    rows = identity.get("reference_centroids") or []
    if rows:
        return [valid_embedding(row) for row in rows if valid_embedding(row)]
    centroid = valid_embedding(identity.get("centroid"))
    return [centroid] if centroid else []


def display_name(identity, _client=None):
    return _text(identity.get("person_name") or identity.get("name"))


def person_name(_person_id, _client=None):
    return ""


def runtime_status(_client=None):
    return {"enabled": False, "loaded": False, "state": "disabled"}


def record_detection(detection, *, event_id, seen_at="", source=None, redis_client=None):
    del source
    embedding = valid_embedding(detection.get("embedding"))
    identities = identity_rows(redis_client)
    matched_id = ""
    best_distance = float("inf")
    for identity_id, identity in identities.items():
        distance = min((cosine_distance(embedding, row) for row in reference_embeddings(identity)), default=float("inf"))
        if distance < best_distance:
            matched_id, best_distance = identity_id, distance
    if best_distance > 0.30:
        matched_id = ""
    identity = dict(identities.get(matched_id) or {})
    if not matched_id:
        matched_id = f"face_{uuid.uuid4().hex[:16]}"
        identity = {"id": matched_id, "name": "", "observations": [], "event_count": 0}
    observations = list(identity.get("observations") or [])
    observations.insert(
        0,
        {
            "id": f"observation_{uuid.uuid4().hex[:20]}",
            "event_id": _text(event_id),
            "seen_at": _text(seen_at),
            "embedding": embedding,
            "face_b64": _text(detection.get("crop_b64")),
            "face_content_type": _text(detection.get("crop_content_type")) or "image/jpeg",
            "quality": float(detection.get("confidence") or 0),
        },
    )
    identity.update(
        {
            "id": matched_id,
            "centroid": embedding,
            "centroid_count": int(identity.get("centroid_count") or 0) + 1,
            "reference_centroids": [embedding],
            "observations": observations,
            "observation_count": len(observations),
            "event_count": len({_text(row.get("event_id")) for row in observations if _text(row.get("event_id"))}),
            "last_event_id": _text(event_id),
            "last_seen": _text(seen_at),
            "face_b64": _text(detection.get("crop_b64")),
            "face_content_type": _text(detection.get("crop_content_type")) or "image/jpeg",
        }
    )
    return save_identity(identity, redis_client)


def save_profile(identity_id, *, name="", person_id="", person_link_supplied=True, redis_client=None):
    identity = dict(identity_rows(redis_client).get(_text(identity_id)) or {})
    if not identity:
        raise KeyError("Face identity not found.")
    identity["name"] = _text(name)
    if person_link_supplied and person_id:
        identity["person_id"] = _text(person_id)
        identity["person_name"] = _text(name)
    return save_identity(identity, redis_client)


def identity_ids_for_event(_event_id, fallback_identity_ids=None, _client=None):
    return list(dict.fromkeys(_text(value) for value in (fallback_identity_ids or []) if _text(value)))


def recognized_people(identity_ids, client=None):
    identities = identity_rows(client)
    rows = []
    for identity_id in identity_ids:
        identity = identities.get(_text(identity_id)) or {}
        person_id = _text(identity.get("person_id"))
        person_name = display_name(identity, client)
        if person_id and person_name:
            rows.append({"person_id": person_id, "person_name": person_name, "face_identity_ids": [_text(identity_id)]})
    return rows


def recognize_image(*_args, **_kwargs):
    return {"status": "disabled", "warning": "Face ID is disabled in Settings › Models.", "people": [], "identity_ids": []}
