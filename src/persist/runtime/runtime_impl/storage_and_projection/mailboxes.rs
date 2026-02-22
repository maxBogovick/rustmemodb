impl PersistEntityRuntime {
    /// Starts a command on the entity's mailbox, marking it as busy/inflight.
    ///
    /// Checks out a 'permit' for the specific entity key. Used for concurrency control
    /// to prevent parallel execution on the same entity within the runtime.
    fn mailbox_start_command(&mut self, key: &RuntimeEntityKey) {
        let now = Utc::now();
        let entry = self
            .entity_mailboxes
            .entry(key.clone())
            .or_insert_with(|| RuntimeEntityMailbox::new(now));
        entry.pending_commands = entry.pending_commands.saturating_add(1);
        entry.inflight = true;
        entry.last_command_at = now;
    }

    /// Completes a command on the entity's mailbox, releasing the busy/inflight status.
    fn mailbox_complete_command(&mut self, key: &RuntimeEntityKey) {
        let Some(entry) = self.entity_mailboxes.get_mut(key) else {
            return;
        };
        entry.pending_commands = entry.pending_commands.saturating_sub(1);
        entry.inflight = false;
        entry.last_command_at = Utc::now();
    }

    /// Checks if the entity is currently processing a command or has pending commands.
    ///
    /// Returns true if the mailbox is busy.
    fn mailbox_is_busy(&self, key: &RuntimeEntityKey) -> bool {
        self.entity_mailboxes
            .get(key)
            .map(|entry| entry.inflight || entry.pending_commands > 0)
            .unwrap_or(false)
    }

    /// Removes the mailbox entry for an entity, typically called when the entity is deleted
    /// or evicted from memory.
    fn mailbox_drop_entity(&mut self, key: &RuntimeEntityKey) {
        self.entity_mailboxes.remove(key);
    }
}
