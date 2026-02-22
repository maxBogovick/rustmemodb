# Habit Hero Lessons (Markdown-First)

This directory is the learning track in a single format:

- one lesson = one large `lesson-XX-*.md`
- each lesson has side-by-side sections:
  - `Was (classic)`
  - `Now (product)`
- only the minimal code required to explain the decision is shown

## Why this format

It keeps the audience focused on value and tradeoffs instead of file sprawl.

You still keep runnable code in:

- `/Users/maxim/RustroverProjects/rustmemodb/education/habit-hero-ab/lesson1`
- `/Users/maxim/RustroverProjects/rustmemodb/education/habit-hero-ab/lesson2`
- `/Users/maxim/RustroverProjects/rustmemodb/education/habit-hero-ab/lesson3`
- `/Users/maxim/RustroverProjects/rustmemodb/education/habit-hero-ab/lesson4`

The markdown files point to those sources and extract the exact pieces needed for learning.

## Current lessons

1. `lesson-01-user-registration.md`
2. `lesson-02-read-model-pagination.md`
3. `lesson-03-write-model-concurrency.md`
4. `lesson-04-command-first-bulk-audit.md`

## Template for next lessons

Use `_lesson-template.md` and keep the same structure so readers always know what to expect.
