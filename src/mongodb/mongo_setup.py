import sys
import json
from pathlib import Path
from typing import Dict, Any

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure

from config.settings import (
    MONGODB_URI,
    MONGODB_DATABASE,
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
    COLLECTION_META_STATE,
    COLLECTION_PROCESSED_EVENTS,
    SCHEMAS_DIR
)

_mongo_client = None


def get_mongo_client(uri: str = MONGODB_URI) -> MongoClient:
    global _mongo_client
    if _mongo_client is None:
        try:
            _mongo_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
            _mongo_client.admin.command("ping")
        except ConnectionFailure as err:
            raise ConnectionError(f"Failed to connect to MongoDB at {uri}: {err}") from err
    return _mongo_client


def get_mongo_db(db_name: str = MONGODB_DATABASE, uri: str = MONGODB_URI) -> Database:
    client = get_mongo_client(uri)
    return client[db_name]


def load_schema(schema_filename: str = "orders_schema.json") -> Dict[str, Any]:
    schema_path = SCHEMAS_DIR / schema_filename
    if not schema_path.exists():
        fallback_path = Path(__file__).parent.parent.parent / schema_filename
        if fallback_path.exists():
            schema_path = fallback_path
        else:
            fallback_ar = Path(__file__).parent.parent.parent / "mongodb_orders_schema_ar.json"
            if fallback_ar.exists():
                schema_path = fallback_ar
            else:
                raise FileNotFoundError(f"Schema file {schema_filename} not found.")

    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def initialize_database(schema_filename: str = "orders_schema.json", uri: str = MONGODB_URI, db_name: str = MONGODB_DATABASE) -> Database:
    db = get_mongo_db(db_name, uri)
    schema_doc = load_schema(schema_filename)

    # 1. orders_raw
    if COLLECTION_RAW not in db.list_collection_names():
        db.create_collection(COLLECTION_RAW)
    raw_coll = db[COLLECTION_RAW]
    raw_coll.create_index([("id_order", ASCENDING)])
    raw_coll.create_index([("file_source", ASCENDING)])
    raw_coll.create_index([("id_run", ASCENDING)])
    raw_coll.create_index([("at_ingested", DESCENDING)])
    raw_coll.create_index([("id_run", ASCENDING), ("number_row_source", ASCENDING)])

    # 2. orders_validated (JSON Schema + Unique Index on id_order)
    if COLLECTION_VALIDATED not in db.list_collection_names():
        db.create_collection(
            COLLECTION_VALIDATED,
            validator={"$jsonSchema": schema_doc},
            validationLevel="strict",
            validationAction="error"
        )
    else:
        try:
            db.command("collMod", COLLECTION_VALIDATED, validator={"$jsonSchema": schema_doc})
        except OperationFailure:
            pass

    val_coll = db[COLLECTION_VALIDATED]
    val_coll.create_index([("id_order", ASCENDING)], unique=True, name="ux_id_order")
    val_coll.create_index([("order_date", DESCENDING)])
    val_coll.create_index([("quality_status", ASCENDING)])

    # 3. quarantine_orders
    if COLLECTION_QUARANTINE not in db.list_collection_names():
        db.create_collection(COLLECTION_QUARANTINE)
    quar_coll = db[COLLECTION_QUARANTINE]
    quar_coll.create_index([("id_run", ASCENDING)])
    quar_coll.create_index([("codes_error", ASCENDING)])
    quar_coll.create_index([("quarantined_at", DESCENDING)])
    quar_coll.create_index([("id_order", ASCENDING)])
    quar_coll.create_index([("id_run", ASCENDING), ("source_row_number", ASCENDING)])


    # 4. meta_state & processed_events
    if COLLECTION_META_STATE not in db.list_collection_names():
        db.create_collection(COLLECTION_META_STATE)
    db[COLLECTION_META_STATE].create_index([("pipeline", ASCENDING)], unique=True)
    db[COLLECTION_META_STATE].create_index([("file_fingerprint", ASCENDING)], sparse=True)

    if COLLECTION_PROCESSED_EVENTS not in db.list_collection_names():
        db.create_collection(COLLECTION_PROCESSED_EVENTS)
    db[COLLECTION_PROCESSED_EVENTS].create_index([("event_id", ASCENDING)], unique=True)

    return db


def reset_collections(uri: str = MONGODB_URI, db_name: str = MONGODB_DATABASE):
    db = get_mongo_db(db_name, uri)
    for name in [COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE, COLLECTION_META_STATE, COLLECTION_PROCESSED_EVENTS]:
        db.drop_collection(name)
    initialize_database(uri=uri, db_name=db_name)


def close_mongo_connection():
    global _mongo_client
    if _mongo_client is not None:
        _mongo_client.close()
        _mongo_client = None
