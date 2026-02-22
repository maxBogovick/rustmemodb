/// Checks if a snapshot file is compatible with the current runtime version.
pub fn runtime_snapshot_compat_check(
    snapshot_path: impl AsRef<Path>,
    current_version: u32,
) -> Result<RuntimeCompatReport> {
    let path = snapshot_path.as_ref();
    let bytes = std::fs::read(path).map_err(|err| DbError::IoError(err.to_string()))?;

    let snapshot = serde_json::from_slice::<RuntimeSnapshotFile>(&bytes).map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to parse runtime snapshot file '{}': {}",
            path.display(),
            err
        ))
    })?;

    let mut incompatible = Vec::new();
    for entity in snapshot.entities {
        if entity.state.metadata.schema_version > current_version {
            incompatible.push(RuntimeCompatIssue {
                entity_type: entity.state.type_name,
                persist_id: entity.state.persist_id,
                schema_version: entity.state.metadata.schema_version,
                reason: format!(
                    "Entity schema version {} is newer than runtime {}",
                    entity.state.metadata.schema_version, current_version
                ),
            });
        }
    }

    Ok(RuntimeCompatReport {
        snapshot_path: path.to_string_lossy().to_string(),
        current_version,
        compatible: incompatible.is_empty(),
        issues: incompatible,
    })
}

/// Checks if a journal file is compatible with the current runtime version.
pub fn runtime_journal_compat_check(
    journal_path: impl AsRef<Path>,
    current_version: u32,
) -> Result<RuntimeCompatReport> {
    let path = journal_path.as_ref();
    if !path.exists() {
        return Ok(RuntimeCompatReport {
            snapshot_path: path.to_string_lossy().to_string(),
            current_version,
            compatible: true,
            issues: Vec::new(),
        });
    }

    let file = std::fs::File::open(path).map_err(|err| DbError::IoError(err.to_string()))?;
    let reader = std::io::BufReader::new(file);

    let mut incompatible = Vec::new();
    let mut seen = HashSet::new();
    for (line_no, line) in std::io::BufRead::lines(reader).enumerate() {
        let line = line.map_err(|err| DbError::IoError(err.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }

        let record = serde_json::from_str::<RuntimeJournalRecord>(&line).map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to parse runtime journal file '{}' at line {}: {}",
                path.display(),
                line_no + 1,
                err
            ))
        })?;
        let RuntimeJournalOp::Upsert { entity, .. } = record.op else {
            continue;
        };

        if entity.state.metadata.schema_version <= current_version {
            continue;
        }

        let dedupe_key = format!(
            "{}:{}:{}",
            entity.state.type_name, entity.state.persist_id, entity.state.metadata.schema_version
        );
        if !seen.insert(dedupe_key) {
            continue;
        }

        incompatible.push(RuntimeCompatIssue {
            entity_type: entity.state.type_name,
            persist_id: entity.state.persist_id,
            schema_version: entity.state.metadata.schema_version,
            reason: format!(
                "Journal contains entity schema version {} newer than runtime {}",
                entity.state.metadata.schema_version, current_version
            ),
        });
    }

    Ok(RuntimeCompatReport {
        snapshot_path: path.to_string_lossy().to_string(),
        current_version,
        compatible: incompatible.is_empty(),
        issues: incompatible,
    })
}

/// Description of a compatibility issue found during checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCompatIssue {
    /// The type of the entity involved.
    pub entity_type: String,
    /// The ID of the entity.
    pub persist_id: String,
    /// The schema version found on disk.
    pub schema_version: u32,
    /// Human-readable reason for incompatibility.
    pub reason: String,
}

/// Report summarizing the results of a compatibility check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCompatReport {
    /// Path to the file checked.
    pub snapshot_path: String,
    /// The current runtime schema version.
    pub current_version: u32,
    /// Whether the file is compatible.
    pub compatible: bool,
    /// List of issues found (empty if compatible).
    pub issues: Vec<RuntimeCompatIssue>,
}
