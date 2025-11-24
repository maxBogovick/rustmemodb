use crate::core::{Result, DbError};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use regex::Regex;

lazy_static::lazy_static! {
    static ref REGEX_LRU_CACHE: Arc<Mutex<LruCache<String, Arc<Regex>>>> =
        Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap())));
}

/// Конвертировать LIKE паттерн в regex
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

/// Получить или создать regex с LRU кэшем (ограниченный размер)
fn get_or_compile_regex_lru(pattern: &str, case_sensitive: bool) -> Result<Arc<Regex>> {
    let cache_key = format!("{}:{}", case_sensitive, pattern);

    // Проверяем LRU кэш
    {
        let mut cache = REGEX_LRU_CACHE.lock().unwrap();
        if let Some(regex) = cache.get(&cache_key) {
            return Ok(Arc::clone(regex));
        }
    }

    // Компилируем regex
    let regex_pattern = like_to_regex(pattern);
    let compiled = if case_sensitive {
        Regex::new(&regex_pattern)
            .map_err(|e| DbError::ExecutionError(format!("Invalid LIKE pattern: {}", e)))?
    } else {
        regex::RegexBuilder::new(&regex_pattern)
            .case_insensitive(true)
            .build()
            .map_err(|e| DbError::ExecutionError(format!("Invalid LIKE pattern: {}", e)))?
    };

    let compiled_arc = Arc::new(compiled);

    // Сохраняем в LRU кэш
    {
        let mut cache = REGEX_LRU_CACHE.lock().unwrap();
        cache.put(cache_key, Arc::clone(&compiled_arc));
    }

    Ok(compiled_arc)
}

/// Финальная оптимизированная версия
pub fn eval_like(text: &str, pattern: &str, case_sensitive: bool) -> Result<bool> {
    // Сначала проверяем быстрые пути
    match fast_path_check(text, pattern, case_sensitive) {
        Some(result) => return Ok(result),
        None => {}
    }

    // Используем LRU кэш для regex
    let regex = get_or_compile_regex_lru(pattern, case_sensitive)?;
    Ok(regex.is_match(text))
}

fn fast_path_check(text: &str, pattern: &str, case_sensitive: bool) -> Option<bool> {
    // Точное совпадение
    if !pattern.contains('%') && !pattern.contains('_') {
        return Some(if case_sensitive {
            text == pattern
        } else {
            text.eq_ignore_ascii_case(pattern)
        });
    }

    // Начинается с "prefix%"
    if pattern.ends_with('%') && !pattern[..pattern.len()-1].contains('%') && !pattern.contains('_') {
        let prefix = &pattern[..pattern.len()-1];
        return Some(if case_sensitive {
            text.starts_with(prefix)
        } else {
            text.to_lowercase().starts_with(&prefix.to_lowercase())
        });
    }

    // Заканчивается на "%suffix"
    if pattern.starts_with('%') && !pattern[1..].contains('%') && !pattern.contains('_') {
        let suffix = &pattern[1..];
        return Some(if case_sensitive {
            text.ends_with(suffix)
        } else {
            text.to_lowercase().ends_with(&suffix.to_lowercase())
        });
    }

    // Содержит "%substring%"
    if pattern.starts_with('%') && pattern.ends_with('%') && pattern.matches('%').count() == 2 && !pattern.contains('_') {
        let substring = &pattern[1..pattern.len()-1];
        return Some(if case_sensitive {
            text.contains(substring)
        } else {
            text.to_lowercase().contains(&substring.to_lowercase())
        });
    }

    None
}