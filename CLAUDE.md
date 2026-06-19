# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package

`@jcbuisson/express-x-drizzle` is an ESM-only NPM package (no build step). The single source file is `src/drizzle-plugins.mjs`; the package root re-exports it as `drizzle-plugins.mjs` via the `"main"` field in `package.json`.

No test runner or linter is configured. There are no build, test, or lint commands.

To publish: `npm publish` (package is public, not private).

## Architecture

The library exports one function: `drizzleOfflinePlugin(app, db, metadata, models)`.

**Parameters:**
- `app` — an `express-x` application instance (from `@jcbuisson/express-x`)
- `db` — a Drizzle ORM database instance
- `metadata` — a Drizzle table schema used to store per-record timestamps (`uid`, `created_at`, `updated_at`, `deleted_at`)
- `models` — array of Drizzle table schemas to expose as services

**What it does:**

1. **Per-model services** — For each table in `models`, registers an `express-x` service named after the table. Each service exposes: `findUnique`, `findMany`, `createWithMeta`, `updateWithMeta`, `deleteWithMeta`. All mutation methods write to both the model table and the `metadata` table inside a transaction, keeping timestamps in sync.

2. **`sync` service** — Implements an offline-first reconciliation algorithm. The client sends its local metadata dictionary (`{ uid → { created_at, updated_at, deleted_at } }`). The server computes set intersections between client UIDs and database UIDs, then determines add/update/delete operations for both sides. An overlap-aware lock serializes sync calls whose `where` scopes can touch the same records. The return value tells the client which records to add, update, or delete locally, and which UIDs the client should push to the database with full data.

**Key invariant:** The sync algorithm compares `updated_at` (falling back to `created_at`) timestamps to decide which side wins a conflict — the most recently updated side wins. Records marked `deleted_at` on the client trigger a soft-delete propagation to the database.
