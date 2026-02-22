# no_db_api

Schema-first REST example with zero handwritten handlers.

`unistructgen` usage in this example:

- HTTP test payload DTOs are generated at compile time with `rustmemodb::generate_struct_from_json!`,
- tests avoid handwritten request-struct boilerplate and raw ad-hoc JSON maps.

## Run

```bash
cargo run --manifest-path examples/no_db_api/Cargo.toml
```

Server starts on `http://127.0.0.1:3001` and mounts dynamic CRUD under `/api`.

The router is generated from JSON schemas in:

- `examples/no_db_api/schemas/users.json`

Generated endpoints for `users.json`:

- `GET /api/users`
- `POST /api/users`
- `GET /api/users/:id`
- `PATCH /api/users/:id`
- `DELETE /api/users/:id`
- `GET /api/_openapi.json`

Hot-reload behavior:

- if `schemas/users.json` changes while the server is running, the router reloads schema metadata automatically,
- new nullable fields are migrated to storage via automatic `ALTER TABLE ... ADD COLUMN`,
- no restart is required.

Example create request:

```bash
curl -X POST http://127.0.0.1:3001/api/users \
  -H 'content-type: application/json' \
  -d '{"username":"alice","email":"alice@example.com","age":30,"active":true}'
```
