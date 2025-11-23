use crate::core::{Result, DbError};

/// Convert SQL LIKE pattern to regex pattern
fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' if i + 1 < chars.len() => {
                i += 1;
                regex.push_str(&regex::escape(&chars[i].to_string()));
            }
            c if ".*+?^${}()|[]\\".contains(c) => {
                regex.push('\\');
                regex.push(c);
            }
            c => regex.push(c),
        }
        i += 1;
    }

    regex.push('$');
    regex
}

/// Evaluate LIKE pattern matching
pub fn eval_like(text: &str, pattern: &str, case_sensitive: bool) -> Result<bool> {
    let regex_pattern = like_to_regex(pattern);

    let result = if case_sensitive {
        regex::Regex::new(&regex_pattern)
            .map_err(|e| DbError::ExecutionError(format!("Invalid LIKE pattern: {}", e)))?
            .is_match(text)
    } else {
        regex::RegexBuilder::new(&regex_pattern)
            .case_insensitive(true)
            .build()
            .map_err(|e| DbError::ExecutionError(format!("Invalid LIKE pattern: {}", e)))?
            .is_match(text)
    };

    Ok(result)
}