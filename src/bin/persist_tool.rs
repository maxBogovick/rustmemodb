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
    Dsl {
        #[command(subcommand)]
        action: DslCommand,
    },
    ApiVersion,
}

#[derive(Subcommand)]
enum DslCommand {
    Check {
        #[arg(long)]
        input: PathBuf,
    },
    Build {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    Fmt {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value_t = false)]
        write: bool,
    },
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
        Command::Dsl { action } => match action {
            DslCommand::Check { input } => dsl_check(&input),
            DslCommand::Build { input, out } => dsl_build(&input, &out),
            DslCommand::Fmt { input, write } => dsl_fmt(&input, write),
        },
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

    let default_table = name.to_lowercase();
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

#[derive(Debug, Clone)]
struct DslSummary {
    app_name: String,
    rest_path: Option<String>,
    aggregates: Vec<String>,
    command_count: usize,
    query_count: usize,
}

fn dsl_check(input: &Path) -> Result<()> {
    let source = fs::read_to_string(input)
        .with_context(|| format!("Failed to read DSL file '{}'", input.display()))?;
    let summary = parse_dsl_summary(&source)
        .with_context(|| format!("Invalid DSL file '{}'", input.display()))?;
    println!("DSL check: OK");
    println!("  app: {}", summary.app_name);
    println!(
        "  rest: {}",
        summary.rest_path.as_deref().unwrap_or("<not declared>")
    );
    println!("  aggregates: {}", summary.aggregates.join(", "));
    println!("  commands: {}", summary.command_count);
    println!("  queries: {}", summary.query_count);
    Ok(())
}

fn dsl_build(input: &Path, out: &Path) -> Result<()> {
    let source = fs::read_to_string(input)
        .with_context(|| format!("Failed to read DSL file '{}'", input.display()))?;
    let summary = parse_dsl_summary(&source)
        .with_context(|| format!("Invalid DSL file '{}'", input.display()))?;

    let aggregate_name = summary
        .aggregates
        .first()
        .ok_or_else(|| anyhow!("DSL must declare at least one aggregate"))?;
    let table_name = aggregate_name.to_ascii_lowercase();
    let rest_path = summary
        .rest_path
        .clone()
        .unwrap_or_else(|| format!("/api/{}", table_name));
    let generated = format!(
        "// Generated by persist-tool dsl build\n\
         // Source: {}\n\
         // App: {}\n\
         // NOTE: This is Phase-1 generated scaffold.\n\
         // Extend fields and domain methods according to DSL semantics.\n\
         \n\
         use rustmemodb::prelude::dx::*;\n\
         use serde::{{Deserialize, Serialize}};\n\
         \n\
         #[domain(table = \"{}\", schema_version = 1)]\n\
         #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\n\
         pub struct {} {{\n\
             pub name: String,\n\
         }}\n\
         \n\
         #[derive(Clone, Debug, PartialEq, Eq, DomainError)]\n\
         pub enum {}Error {{\n\
             #[api_error(status = 422, code = \"validation_error\")]\n\
             Validation(String),\n\
         }}\n\
         \n\
         #[api]\n\
         impl {} {{\n\
             pub fn new(name: String) -> Self {{\n\
                 Self {{ name }}\n\
             }}\n\
         }}\n\
         \n\
         pub const {}_REST_PATH: &str = \"{}\";\n",
        input.display(),
        summary.app_name,
        table_name,
        aggregate_name,
        aggregate_name,
        aggregate_name,
        summary.app_name.to_ascii_uppercase(),
        rest_path,
    );

    ensure_parent_dir(out)?;
    fs::write(out, generated)
        .with_context(|| format!("Failed to write DSL scaffold to '{}'", out.display()))?;
    println!("Generated DSL scaffold: {}", out.display());
    Ok(())
}

fn dsl_fmt(input: &Path, write: bool) -> Result<()> {
    let source = fs::read_to_string(input)
        .with_context(|| format!("Failed to read DSL file '{}'", input.display()))?;
    let formatted = format_dsl_source(&source);
    if write {
        fs::write(input, formatted)
            .with_context(|| format!("Failed to write formatted DSL '{}'", input.display()))?;
        println!("Formatted DSL file: {}", input.display());
    } else {
        println!("{}", formatted);
    }
    Ok(())
}

fn parse_dsl_summary(source: &str) -> Result<DslSummary> {
    let mut app_name: Option<String> = None;
    let mut rest_path: Option<String> = None;
    let mut aggregates = Vec::<String>::new();
    let mut command_count = 0usize;
    let mut query_count = 0usize;

    for raw_line in source.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        if app_name.is_none() && line.starts_with("app ") {
            let name = extract_ident_after_keyword(line, "app")
                .ok_or_else(|| anyhow!("Expected app name after `app`"))?;
            app_name = Some(name);
            rest_path = extract_rest_attr(line);
            continue;
        }

        if line.starts_with("aggregate ") {
            let name = extract_ident_after_keyword(line, "aggregate")
                .ok_or_else(|| anyhow!("Expected aggregate name after `aggregate`"))?;
            aggregates.push(name);
            continue;
        }

        if line.starts_with('!') {
            command_count += 1;
        } else if line.starts_with('?') {
            query_count += 1;
        }
    }

    let app_name = app_name.ok_or_else(|| anyhow!("DSL must contain `app <Name>` declaration"))?;
    if aggregates.is_empty() {
        return Err(anyhow!(
            "DSL must contain at least one `aggregate` declaration"
        ));
    }

    Ok(DslSummary {
        app_name,
        rest_path,
        aggregates,
        command_count,
        query_count,
    })
}

fn format_dsl_source(source: &str) -> String {
    let mut formatted = String::new();
    for line in source.lines() {
        formatted.push_str(line.trim_end());
        formatted.push('\n');
    }
    if !formatted.ends_with("\n") {
        formatted.push('\n');
    }
    formatted
}

fn extract_ident_after_keyword(line: &str, keyword: &str) -> Option<String> {
    let prefix = format!("{keyword} ");
    if !line.starts_with(&prefix) {
        return None;
    }
    let after = line[prefix.len()..].trim();
    let mut name = String::new();
    for ch in after.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            name.push(ch);
        } else {
            break;
        }
    }
    if name.is_empty() { None } else { Some(name) }
}

fn extract_rest_attr(line: &str) -> Option<String> {
    let rest_idx = line.find("@rest(")?;
    let rest_raw = &line[rest_idx + "@rest(".len()..];
    let close_idx = rest_raw.find(')')?;
    let value = rest_raw[..close_idx].trim().trim_matches('"').to_string();
    if value.is_empty() { None } else { Some(value) }
}

#[cfg(test)]
mod tests {
    use super::{
        dsl_build, extract_ident_after_keyword, extract_rest_attr, format_dsl_source,
        parse_dsl_summary,
    };

    fn demo_dsl() -> &'static str {
        r#"
app LedgerCore @rest("/api/ledgers") {
  aggregate LedgerBook {
    !open_account(OpenAccountInput) -> LedgerAccount
    ?balance_report() -> LedgerBalanceReport
  }
}
"#
    }

    #[test]
    fn parse_dsl_summary_extracts_main_contract() {
        let summary = parse_dsl_summary(demo_dsl()).expect("parse dsl summary");
        assert_eq!(summary.app_name, "LedgerCore");
        assert_eq!(summary.rest_path.as_deref(), Some("/api/ledgers"));
        assert_eq!(summary.aggregates, vec!["LedgerBook".to_string()]);
        assert_eq!(summary.command_count, 1);
        assert_eq!(summary.query_count, 1);
    }

    #[test]
    fn parse_dsl_summary_rejects_missing_aggregate() {
        let input = r#"
app Demo @rest("/api/demo") {
}
"#;
        let error = parse_dsl_summary(input).expect_err("missing aggregate must fail");
        assert!(error.to_string().contains("at least one `aggregate`"));
    }

    #[test]
    fn dsl_build_generates_domain_and_api_scaffold() {
        let temp = tempfile::tempdir().expect("temp dir");
        let input_path = temp.path().join("app.dsl");
        let out_path = temp.path().join("generated/model.rs");
        std::fs::write(&input_path, demo_dsl()).expect("write dsl");

        dsl_build(&input_path, &out_path).expect("build scaffold");
        let generated = std::fs::read_to_string(&out_path).expect("read generated scaffold");

        assert!(generated.contains("#[domain("));
        assert!(generated.contains("#[api]"));
        assert!(generated.contains("pub struct LedgerBook"));
        assert!(generated.contains("pub const LEDGERCORE_REST_PATH"));
    }

    #[test]
    fn dsl_fmt_trims_trailing_whitespace_and_adds_newline() {
        let formatted = format_dsl_source("app Demo {   \n  aggregate A {}\t");
        assert!(formatted.ends_with('\n'));
        assert!(formatted.contains("app Demo {"));
        assert!(formatted.contains("aggregate A {}"));
    }

    #[test]
    fn extract_helpers_parse_identifier_and_rest_path() {
        assert_eq!(
            extract_ident_after_keyword("app LedgerCore @rest(\"/api\") {", "app").as_deref(),
            Some("LedgerCore")
        );
        assert_eq!(
            extract_ident_after_keyword("aggregate Board {", "aggregate").as_deref(),
            Some("Board")
        );
        assert_eq!(
            extract_rest_attr("app LedgerCore @rest(\"/api/ledgers\") {").as_deref(),
            Some("/api/ledgers")
        );
        assert_eq!(extract_rest_attr("app LedgerCore {"), None);
    }
}
