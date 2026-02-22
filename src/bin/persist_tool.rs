use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use rustmemodb::{
    HeteroPersistVecSnapshot, PERSIST_PUBLIC_API_VERSION_STRING, PersistVecSnapshot,
    RuntimeCompatIssue, persist_public_api_version, runtime_journal_compat_check,
    runtime_snapshot_compat_check,
};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "persist-tool")]
#[command(about = "Developer tooling for RustMemDB persist runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Generate {
        #[command(subcommand)]
        target: GenerateTarget,
    },
    CompatCheck {
        #[arg(long)]
        snapshot: Option<PathBuf>,
        #[arg(long)]
        journal: Option<PathBuf>,
        #[arg(long)]
        current_version: u32,
    },
    ApiVersion,
}

#[derive(Subcommand)]
enum GenerateTarget {
    Entity {
        #[arg(long)]
        name: String,
        #[arg(long)]
        fields: String,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 1)]
        schema_version: u32,
    },
    Migration {
        #[arg(long)]
        entity: String,
        #[arg(long)]
        from: u32,
        #[arg(long)]
        to: u32,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        sql: Vec<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Generate { target } => match target {
            GenerateTarget::Entity {
                name,
                fields,
                out,
                schema_version,
            } => generate_entity(&name, &fields, schema_version, &out),
            GenerateTarget::Migration {
                entity,
                from,
                to,
                out,
                sql,
            } => generate_migration(&entity, from, to, &sql, &out),
        },
        Command::CompatCheck {
            snapshot,
            journal,
            current_version,
        } => compat_check(snapshot.as_deref(), journal.as_deref(), current_version),
        Command::ApiVersion => {
            let version = persist_public_api_version();
            println!(
                "Persist public API version: {} (major={}, minor={}, patch={})",
                PERSIST_PUBLIC_API_VERSION_STRING, version.major, version.minor, version.patch
            );
            Ok(())
        }
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create parent directory '{}'", parent.display()))?;
    }
    Ok(())
}

fn generate_entity(name: &str, fields: &str, schema_version: u32, out: &Path) -> Result<()> {
    let parsed_fields = parse_fields(fields)?;
    let field_lines = parsed_fields
        .iter()
        .map(|(name, ty)| format!("    pub {}: {},", name, ty))
        .collect::<Vec<_>>()
        .join("\n");

    let default_table = format!("{}", name.to_lowercase());
    let persisted_alias = format!("Persisted{}", name);

    let content = format!(
        "use rustmemodb::{{PersistModel, persist_struct, persist_vec}};\n\n#[derive(Debug, Clone, PersistModel)]\n#[persist_model(table = \"{}\", schema_version = {})]\npub struct {} {{\n{}\n}}\n\npersist_struct!(pub struct {} from_struct = {});\npersist_vec!(pub {}Vec, {});\n",
        default_table,
        schema_version,
        name,
        field_lines,
        persisted_alias,
        name,
        persisted_alias,
        persisted_alias
    );

    ensure_parent_dir(out)?;
    fs::write(out, content)
        .with_context(|| format!("Failed to write entity template to '{}'", out.display()))?;

    println!("Generated entity template: {}", out.display());
    Ok(())
}

fn parse_fields(input: &str) -> Result<Vec<(String, String)>> {
    let mut fields = Vec::new();

    for part in input.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (name, ty) = trimmed
            .split_once(':')
            .ok_or_else(|| anyhow!("Invalid field '{}'. Expected format: name:Type", trimmed))?;
        fields.push((name.trim().to_string(), ty.trim().to_string()));
    }

    if fields.is_empty() {
        return Err(anyhow!("No fields parsed. Example: id:String,count:i64"));
    }

    Ok(fields)
}

fn generate_migration(entity: &str, from: u32, to: u32, sql: &[String], out: &Path) -> Result<()> {
    if to <= from {
        return Err(anyhow!("to-version must be greater than from-version"));
    }

    let sql_block = if sql.is_empty() {
        "        // Add SQL steps with .with_sql(\"ALTER TABLE {table} ...\")\n".to_string()
    } else {
        sql.iter()
            .map(|stmt| format!("        .with_sql(\"{}\")\n", stmt.replace('"', "\\\"")))
            .collect::<String>()
    };

    let content = format!(
        "use rustmemodb::{{PersistMigrationPlan, PersistMigrationStep}};\n\npub fn {}_migration_plan() -> PersistMigrationPlan {{\n    let mut plan = PersistMigrationPlan::new({});\n    plan.add_step(\n        PersistMigrationStep::new({}, {})\n{}            .with_state_migrator(|state| {{\n                // transform state.fields if needed\n                let _ = state.fields_object_mut()?;\n                Ok(())\n            }}),\n    )\n    .expect(\"migration step must be valid\");\n    plan\n}}\n",
        entity.to_lowercase(),
        to,
        from,
        to,
        sql_block
    );

    ensure_parent_dir(out)?;
    fs::write(out, content)
        .with_context(|| format!("Failed to write migration template to '{}'", out.display()))?;

    println!("Generated migration template: {}", out.display());
    Ok(())
}

fn compat_check(
    snapshot: Option<&Path>,
    journal: Option<&Path>,
    current_version: u32,
) -> Result<()> {
    if snapshot.is_none() && journal.is_none() {
        return Err(anyhow!(
            "At least one source must be provided: --snapshot <path> and/or --journal <path>"
        ));
    }

    if let Some(snapshot) = snapshot {
        compat_check_snapshot(snapshot, current_version)?;
    }
    if let Some(journal) = journal {
        compat_check_journal(journal, current_version)?;
    }
    Ok(())
}

fn compat_check_journal(journal: &Path, current_version: u32) -> Result<()> {
    let report = runtime_journal_compat_check(journal, current_version)?;
    print_runtime_compat_report(&report.issues, current_version, &report.snapshot_path);
    Ok(())
}

fn compat_check_snapshot(snapshot: &Path, current_version: u32) -> Result<()> {
    if let Ok(report) = runtime_snapshot_compat_check(snapshot, current_version) {
        print_runtime_compat_report(&report.issues, current_version, &report.snapshot_path);
        return Ok(());
    }

    let raw = fs::read_to_string(snapshot)
        .with_context(|| format!("Failed to read snapshot '{}'", snapshot.display()))?;

    if let Ok(typed) = serde_json::from_str::<PersistVecSnapshot>(&raw) {
        let issues = typed
            .states
            .into_iter()
            .filter(|state| state.metadata.schema_version > current_version)
            .map(|state| RuntimeCompatIssue {
                entity_type: state.type_name,
                persist_id: state.persist_id,
                schema_version: state.metadata.schema_version,
                reason: format!(
                    "entity schema {} > current {}",
                    state.metadata.schema_version, current_version
                ),
            })
            .collect::<Vec<_>>();
        print_runtime_compat_report(&issues, current_version, &snapshot.display().to_string());
        return Ok(());
    }

    if let Ok(hetero) = serde_json::from_str::<HeteroPersistVecSnapshot>(&raw) {
        let issues = hetero
            .states
            .into_iter()
            .filter(|state| state.metadata.schema_version > current_version)
            .map(|state| RuntimeCompatIssue {
                entity_type: state.type_name,
                persist_id: state.persist_id,
                schema_version: state.metadata.schema_version,
                reason: format!(
                    "entity schema {} > current {}",
                    state.metadata.schema_version, current_version
                ),
            })
            .collect::<Vec<_>>();
        print_runtime_compat_report(&issues, current_version, &snapshot.display().to_string());
        return Ok(());
    }

    Err(anyhow!(
        "Unsupported snapshot format in '{}'",
        snapshot.display()
    ))
}

fn print_runtime_compat_report(issues: &[RuntimeCompatIssue], current_version: u32, source: &str) {
    println!("Snapshot: {}", source);
    println!("Current schema version: {}", current_version);

    if issues.is_empty() {
        println!("Compatibility: OK");
        return;
    }

    println!("Compatibility: FAILED");
    for issue in issues {
        println!(
            "- {}:{} (schema={}) -> {}",
            issue.entity_type, issue.persist_id, issue.schema_version, issue.reason
        );
    }
}
