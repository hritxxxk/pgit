-- V1: Initial schema for pgit
-- Managed by Refinery migrations (refinery::embed_migrations!)

CREATE TABLE IF NOT EXISTS commits (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    hash         TEXT UNIQUE NOT NULL,
    message      TEXT,
    created_at   TEXT NOT NULL,
    parent_hash  TEXT
);

CREATE TABLE IF NOT EXISTS manifests (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_hash  TEXT NOT NULL,
    file_path    TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    manifest_blob BLOB NOT NULL,
    total_rows   INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    FOREIGN KEY (commit_hash) REFERENCES commits(hash)
);

CREATE TABLE IF NOT EXISTS features (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id  INTEGER NOT NULL,
    feature_name TEXT NOT NULL,
    count        INTEGER NOT NULL,
    mean         REAL NOT NULL,
    variance     REAL NOT NULL,
    std_dev      REAL NOT NULL,
    min          REAL NOT NULL,
    max          REAL NOT NULL,
    FOREIGN KEY (manifest_id) REFERENCES manifests(id)
);

CREATE TABLE IF NOT EXISTS quantiles (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_id INTEGER NOT NULL,
    percentile REAL NOT NULL,
    value      REAL NOT NULL,
    FOREIGN KEY (feature_id) REFERENCES features(id)
);

CREATE TABLE IF NOT EXISTS head (
    id                   INTEGER PRIMARY KEY CHECK (id = 1),
    current_commit_hash  TEXT
);

INSERT OR IGNORE INTO head (id, current_commit_hash) VALUES (1, NULL);
