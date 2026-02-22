use chrono::{DateTime, Utc};
use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

generate_struct_from_json! {
    name = "LedgerOpenAccountInput",
    json = r#"{
        "owner_name": "Main Wallet",
        "currency": "USD",
        "opening_balance_minor": 10000,
        "note": null
    }"#
}

generate_struct_from_json! {
    name = "LedgerCreateTransferInput",
    json = r#"{
        "from_account_id": "acc_from",
        "to_account_id": "acc_to",
        "amount_minor": 1500,
        "to_amount_minor": null,
        "note": null
    }"#
}

generate_struct_from_json! {
    name = "LedgerAccountBalanceQuery",
    json = r#"{
        "account_id": "acc_1"
    }"#
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerAccount {
    pub id: String,
    pub owner_name: String,
    pub currency: String,
    pub balance_minor: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerEntry {
    pub account_id: String,
    pub currency: String,
    pub amount_minor: i64,
    pub side: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerTransaction {
    pub id: String,
    pub kind: String,
    pub description: String,
    pub entries: Vec<LedgerEntry>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerBalanceAccountLine {
    pub account_id: String,
    pub owner_name: String,
    pub currency: String,
    pub balance_minor: i64,
    pub journal_balance_minor: i64,
    pub consistent: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerBalanceCurrencyLine {
    pub currency: String,
    pub total_minor: i64,
    pub journal_total_minor: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerBalanceReport {
    pub account_lines: Vec<LedgerBalanceAccountLine>,
    pub currency_lines: Vec<LedgerBalanceCurrencyLine>,
    pub transaction_count: usize,
    pub all_balances_consistent: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Autonomous)]
#[persist_model(table = "ledger_books", schema_version = 1)]
pub struct LedgerBook {
    name: String,
    accounts: PersistJson<Vec<LedgerAccount>>,
    transactions: PersistJson<Vec<LedgerTransaction>>,
}

#[derive(Clone, Debug, PartialEq, Eq, ApiError)]
pub enum LedgerDomainError {
    #[api_error(status = 404, code = "account_not_found")]
    AccountNotFound(String),
    #[api_error(status = 422, code = "validation_error")]
    Validation(String),
    #[api_error(status = 422, code = "invalid_currency")]
    InvalidCurrency(String),
    #[api_error(status = 422, code = "invalid_amount")]
    InvalidAmount(&'static str),
    #[api_error(status = 422, code = "same_account_transfer")]
    SameAccountTransfer,
    #[api_error(status = 422, code = "opening_balance_negative")]
    OpeningBalanceNegative,
    #[api_error(status = 409, code = "insufficient_funds")]
    InsufficientFunds {
        account_id: String,
        available_minor: i64,
        required_minor: i64,
    },
}

impl std::fmt::Display for LedgerDomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccountNotFound(account_id) => write!(f, "account not found: {account_id}"),
            Self::Validation(message) => write!(f, "{message}"),
            Self::InvalidCurrency(currency) => write!(f, "invalid currency code: {currency}"),
            Self::InvalidAmount(field) => write!(f, "{field} must be greater than zero"),
            Self::SameAccountTransfer => write!(f, "from_account_id must differ from to_account_id"),
            Self::OpeningBalanceNegative => write!(f, "opening_balance_minor must be >= 0"),
            Self::InsufficientFunds {
                account_id,
                available_minor,
                required_minor,
            } => write!(
                f,
                "insufficient funds for account {account_id}: available={available_minor}, required={required_minor}"
            ),
        }
    }
}

impl std::error::Error for LedgerDomainError {}

#[expose_rest]
impl LedgerBook {
    pub fn new(name: String) -> Self {
        Self {
            name,
            accounts: PersistJson::default(),
            transactions: PersistJson::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn accounts(&self) -> &[LedgerAccount] {
        self.accounts.as_slice()
    }

    pub fn transactions(&self) -> &[LedgerTransaction] {
        self.transactions.as_slice()
    }

    #[query]
    pub fn account_balance(
        &self,
        query: LedgerAccountBalanceQuery,
    ) -> Result<i64, LedgerDomainError> {
        let account_id = query.account_id;
        self.accounts
            .iter()
            .find(|account| account.id == account_id)
            .map(|account| account.balance_minor)
            .ok_or(LedgerDomainError::AccountNotFound(account_id))
    }

    #[view(input = "body")]
    pub fn account_balance_body(
        &self,
        payload: LedgerAccountBalanceQuery,
    ) -> Result<i64, LedgerDomainError> {
        self.account_balance(payload)
    }

    #[query]
    pub fn balance_report(&self) -> LedgerBalanceReport {
        // Balance report is intentionally computed from in-memory transaction history,
        // so API users get instant analytics without SQL aggregations.
        let mut journal_balances = HashMap::<String, i64>::new();
        for account in self.accounts.iter() {
            journal_balances.insert(account.id.clone(), 0);
        }

        let mut journal_totals = BTreeMap::<String, i64>::new();
        for tx in self.transactions.iter() {
            for entry in &tx.entries {
                *journal_totals.entry(entry.currency.clone()).or_insert(0) += entry.amount_minor;
                if let Some(balance) = journal_balances.get_mut(&entry.account_id) {
                    *balance += entry.amount_minor;
                }
            }
        }

        let mut stored_totals = BTreeMap::<String, i64>::new();
        let mut account_lines = Vec::with_capacity(self.accounts.len());
        let mut all_balances_consistent = true;

        for account in self.accounts.iter() {
            *stored_totals.entry(account.currency.clone()).or_insert(0) += account.balance_minor;
            let journal_balance_minor = *journal_balances.get(&account.id).unwrap_or(&0);
            let consistent = account.balance_minor == journal_balance_minor;
            all_balances_consistent &= consistent;

            account_lines.push(LedgerBalanceAccountLine {
                account_id: account.id.clone(),
                owner_name: account.owner_name.clone(),
                currency: account.currency.clone(),
                balance_minor: account.balance_minor,
                journal_balance_minor,
                consistent,
            });
        }
        account_lines.sort_by(|a, b| {
            a.owner_name
                .cmp(&b.owner_name)
                .then(a.account_id.cmp(&b.account_id))
        });

        let currency_keys = stored_totals
            .keys()
            .chain(journal_totals.keys())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();

        let mut currency_lines = Vec::with_capacity(currency_keys.len());
        for currency in currency_keys {
            currency_lines.push(LedgerBalanceCurrencyLine {
                total_minor: *stored_totals.get(&currency).unwrap_or(&0),
                journal_total_minor: *journal_totals.get(&currency).unwrap_or(&0),
                currency,
            });
        }

        LedgerBalanceReport {
            account_lines,
            currency_lines,
            transaction_count: self.transactions.len(),
            all_balances_consistent,
        }
    }

    #[command]
    pub fn open_account(
        &mut self,
        input: LedgerOpenAccountInput,
    ) -> Result<LedgerAccount, LedgerDomainError> {
        let owner_name = normalize_required(input.owner_name, "owner_name")?;
        let currency = normalize_currency_code(input.currency)?;
        let note = json_value_to_optional_string(input.note, "note")?;
        if input.opening_balance_minor < 0 {
            return Err(LedgerDomainError::OpeningBalanceNegative);
        }

        let account = LedgerAccount {
            id: Uuid::new_v4().to_string(),
            owner_name,
            currency: currency.clone(),
            balance_minor: input.opening_balance_minor,
            created_at: Utc::now(),
        };
        self.accounts.push(account.clone());

        if input.opening_balance_minor > 0 {
            let description = note.unwrap_or_else(|| "opening balance".to_string());
            self.transactions.push(build_transaction(
                "opening_balance",
                description,
                vec![
                    LedgerEntry {
                        account_id: account.id.clone(),
                        currency: currency.clone(),
                        amount_minor: input.opening_balance_minor,
                        side: "credit".to_string(),
                    },
                    LedgerEntry {
                        account_id: system_opening_account_id(&currency),
                        currency,
                        amount_minor: -input.opening_balance_minor,
                        side: "debit".to_string(),
                    },
                ],
            ));
        }

        Ok(account)
    }

    #[command]
    pub fn create_transfer(
        &mut self,
        input: LedgerCreateTransferInput,
    ) -> Result<LedgerTransaction, LedgerDomainError> {
        // One command mutates both accounts and appends one immutable transaction event.
        // Persist applies this closure atomically, so domain invariants stay consistent.
        let to_amount_minor = json_value_to_optional_i64(input.to_amount_minor, "to_amount_minor")?;
        let note = json_value_to_optional_string(input.note, "note")?;
        if input.amount_minor <= 0 {
            return Err(LedgerDomainError::InvalidAmount("amount_minor"));
        }
        if input.from_account_id == input.to_account_id {
            return Err(LedgerDomainError::SameAccountTransfer);
        }

        let from_index = self
            .accounts
            .iter()
            .position(|account| account.id == input.from_account_id)
            .ok_or_else(|| LedgerDomainError::AccountNotFound(input.from_account_id.clone()))?;
        let to_index = self
            .accounts
            .iter()
            .position(|account| account.id == input.to_account_id)
            .ok_or_else(|| LedgerDomainError::AccountNotFound(input.to_account_id.clone()))?;

        let from_currency = self.accounts[from_index].currency.clone();
        let to_currency = self.accounts[to_index].currency.clone();
        let available_minor = self.accounts[from_index].balance_minor;

        if available_minor < input.amount_minor {
            return Err(LedgerDomainError::InsufficientFunds {
                account_id: input.from_account_id,
                available_minor,
                required_minor: input.amount_minor,
            });
        }

        let credit_minor = if from_currency == to_currency {
            input.amount_minor
        } else {
            let converted =
                to_amount_minor.ok_or(LedgerDomainError::InvalidAmount("to_amount_minor"))?;
            if converted <= 0 {
                return Err(LedgerDomainError::InvalidAmount("to_amount_minor"));
            }
            converted
        };

        let (from_account, to_account) =
            two_accounts_mut(&mut self.accounts, from_index, to_index);
        from_account.balance_minor -= input.amount_minor;
        to_account.balance_minor += credit_minor;

        let mut entries = vec![
            LedgerEntry {
                account_id: from_account.id.clone(),
                currency: from_currency.clone(),
                amount_minor: -input.amount_minor,
                side: "debit".to_string(),
            },
            LedgerEntry {
                account_id: to_account.id.clone(),
                currency: to_currency.clone(),
                amount_minor: credit_minor,
                side: "credit".to_string(),
            },
        ];

        if from_currency != to_currency {
            entries.push(LedgerEntry {
                account_id: system_fx_account_id(&from_currency),
                currency: from_currency,
                amount_minor: input.amount_minor,
                side: "credit".to_string(),
            });
            entries.push(LedgerEntry {
                account_id: system_fx_account_id(&to_currency),
                currency: to_currency,
                amount_minor: -credit_minor,
                side: "debit".to_string(),
            });
        }

        let tx = build_transaction(
            "transfer",
            note.unwrap_or_else(|| "account transfer".to_string()),
            entries,
        );
        self.transactions.push(tx.clone());
        Ok(tx)
    }
}

fn build_transaction(
    kind: &str,
    description: String,
    entries: Vec<LedgerEntry>,
) -> LedgerTransaction {
    LedgerTransaction {
        id: Uuid::new_v4().to_string(),
        kind: kind.to_string(),
        description,
        entries,
        created_at: Utc::now(),
    }
}

fn normalize_required(value: String, field: &'static str) -> Result<String, LedgerDomainError> {
    let normalized = value.trim().to_string();
    if normalized.is_empty() {
        return Err(LedgerDomainError::Validation(format!(
            "{field} must not be empty"
        )));
    }
    Ok(normalized)
}

fn normalize_currency_code(currency: String) -> Result<String, LedgerDomainError> {
    let normalized = currency.trim().to_ascii_uppercase();
    let valid = normalized.len() == 3 && normalized.chars().all(|ch| ch.is_ascii_alphabetic());
    if valid {
        Ok(normalized)
    } else {
        Err(LedgerDomainError::InvalidCurrency(currency))
    }
}

fn json_value_to_optional_string(
    value: Option<JsonValue>,
    field: &'static str,
) -> Result<Option<String>, LedgerDomainError> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value)),
        Some(other) => Err(LedgerDomainError::Validation(format!(
            "{field} must be string or null, got {other}"
        ))),
    }
}

fn json_value_to_optional_i64(
    value: Option<JsonValue>,
    field: &'static str,
) -> Result<Option<i64>, LedgerDomainError> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(number)) => number.as_i64().map(Some).ok_or_else(|| {
            LedgerDomainError::Validation(format!("{field} must be i64-compatible number"))
        }),
        Some(other) => Err(LedgerDomainError::Validation(format!(
            "{field} must be number or null, got {other}"
        ))),
    }
}

fn system_opening_account_id(currency: &str) -> String {
    format!("system:opening:{currency}")
}

fn system_fx_account_id(currency: &str) -> String {
    format!("system:fx:{currency}")
}

fn two_accounts_mut(
    accounts: &mut [LedgerAccount],
    left_index: usize,
    right_index: usize,
) -> (&mut LedgerAccount, &mut LedgerAccount) {
    if left_index < right_index {
        let (left, right) = accounts.split_at_mut(right_index);
        (&mut left[left_index], &mut right[0])
    } else {
        let (left, right) = accounts.split_at_mut(left_index);
        (&mut right[0], &mut left[right_index])
    }
}

#[cfg(test)]
mod tests {
    use super::{LedgerBook, LedgerCreateTransferInput, LedgerDomainError, LedgerOpenAccountInput};

    #[test]
    fn transfer_updates_balances_and_transactions() {
        let mut ledger = LedgerBook::new("Personal".to_string());

        let source = ledger
            .open_account(LedgerOpenAccountInput {
                owner_name: "Main".to_string(),
                currency: "usd".to_string(),
                opening_balance_minor: 10_000,
                note: None,
            })
            .expect("open source account");
        let target = ledger
            .open_account(LedgerOpenAccountInput {
                owner_name: "Savings".to_string(),
                currency: "USD".to_string(),
                opening_balance_minor: 500,
                note: None,
            })
            .expect("open target account");

        ledger
            .create_transfer(LedgerCreateTransferInput {
                from_account_id: source.id.clone(),
                to_account_id: target.id.clone(),
                amount_minor: 1_200,
                to_amount_minor: None,
                note: None,
            })
            .expect("transfer");

        let source_after = ledger
            .accounts()
            .iter()
            .find(|account| account.id == source.id)
            .expect("source account after transfer");
        let target_after = ledger
            .accounts()
            .iter()
            .find(|account| account.id == target.id)
            .expect("target account after transfer");

        assert_eq!(source_after.balance_minor, 8_800);
        assert_eq!(target_after.balance_minor, 1_700);
        assert_eq!(ledger.transactions().len(), 3);
    }

    #[test]
    fn transfer_rejects_insufficient_funds() {
        let mut ledger = LedgerBook::new("Personal".to_string());

        let source = ledger
            .open_account(LedgerOpenAccountInput {
                owner_name: "Main".to_string(),
                currency: "USD".to_string(),
                opening_balance_minor: 100,
                note: None,
            })
            .expect("open source account");
        let target = ledger
            .open_account(LedgerOpenAccountInput {
                owner_name: "Savings".to_string(),
                currency: "USD".to_string(),
                opening_balance_minor: 0,
                note: None,
            })
            .expect("open target account");

        let error = ledger
            .create_transfer(LedgerCreateTransferInput {
                from_account_id: source.id.clone(),
                to_account_id: target.id.clone(),
                amount_minor: 500,
                to_amount_minor: None,
                note: None,
            })
            .expect_err("insufficient funds");

        assert!(matches!(
            error,
            LedgerDomainError::InsufficientFunds {
                available_minor: 100,
                required_minor: 500,
                ..
            }
        ));
        assert_eq!(ledger.transactions().len(), 1);
    }
}
