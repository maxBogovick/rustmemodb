use crate::core::{Result, DbError};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use regex::Regex;

lazy_static::lazy_static! {
    static ref REGEX_LRU_CACHE: Arc<Mutex<LruCache<String, Arc<Regex>>>> =
        Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(200).unwrap())));
}

/// Конвертировать LIKE паттерн в regex
#[inline]
fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() + 2);
    regex.push('^');

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

/// Fast path для простых паттернов (БЕЗ regex)
#[inline]
fn fast_path_like(text: &str, pattern: &str, case_sensitive: bool) -> Option<bool> {
    // 1. Точное совпадение (нет wildcards)
    if !pattern.contains('%') && !pattern.contains('_') {
        return Some(if case_sensitive {
            text == pattern
        } else {
            text.eq_ignore_ascii_case(pattern)
        });
    }

    // 2. Начинается с "prefix%"
    if pattern.ends_with('%')
        && !pattern[..pattern.len()-1].contains('%')
        && !pattern.contains('_')
    {
        let prefix = &pattern[..pattern.len()-1];
        return Some(if case_sensitive {
            text.starts_with(prefix)
        } else {
            text.to_lowercase().starts_with(&prefix.to_lowercase())
        });
    }

    // 3. Заканчивается на "%suffix"
    if pattern.starts_with('%')
        && !pattern[1..].contains('%')
        && !pattern.contains('_')
    {
        let suffix = &pattern[1..];
        return Some(if case_sensitive {
            text.ends_with(suffix)
        } else {
            text.to_lowercase().ends_with(&suffix.to_lowercase())
        });
    }

    // 4. Содержит "%substring%"
    if pattern.starts_with('%')
        && pattern.ends_with('%')
        && pattern.matches('%').count() == 2
        && !pattern.contains('_')
    {
        let substring = &pattern[1..pattern.len()-1];
        return Some(if case_sensitive {
            text.contains(substring)
        } else {
            text.to_lowercase().contains(&substring.to_lowercase())
        });
    }

    None
}

/// Получить compiled regex с кэшированием
fn get_or_compile_regex(pattern: &str, case_sensitive: bool) -> Result<Arc<Regex>> {
    let cache_key = if case_sensitive {
        format!("s:{}", pattern)
    } else {
        format!("i:{}", pattern)
    };

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

    // Сохраняем в кэш
    {
        let mut cache = REGEX_LRU_CACHE.lock().unwrap();
        cache.put(cache_key, Arc::clone(&compiled_arc));
    }

    Ok(compiled_arc)
}

/// ФИНАЛЬНАЯ оптимизированная версия LIKE
#[inline]
pub fn eval_like(text: &str, pattern: &str, case_sensitive: bool) -> Result<bool> {
    // Сначала пробуем fast path (O(n) без regex)
    if let Some(result) = fast_path_like(text, pattern, case_sensitive) {
        return Ok(result);
    }

    // Используем cached regex для сложных паттернов
    let regex = get_or_compile_regex(pattern, case_sensitive)?;
    Ok(regex.is_match(text))
}