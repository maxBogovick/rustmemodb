async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to create parent directory '{}': {}",
                parent.display(),
                err
            ))
        })?;
    }

    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).await.map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to write temp file '{}': {}",
            tmp.display(),
            err
        ))
    })?;

    fs::rename(&tmp, path).await.map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to rename temp file '{}' -> '{}': {}",
            tmp.display(),
            path.display(),
            err
        ))
    })?;
    Ok(())
}
