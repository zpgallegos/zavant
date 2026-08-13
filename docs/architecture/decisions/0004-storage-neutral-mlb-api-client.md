# ADR 0004: Separate MLB HTTP retrieval from contracts and storage

- Status: accepted
- Date: 2026-08-09

## Context

The legacy acquisition code made unbounded `requests.get` calls directly inside orchestration and immediately parsed or persisted their results. Network failures, unsuccessful HTTP responses, invalid source payloads, and storage failures therefore shared one control flow and were difficult to test independently.

The v2 contracts and local stores already define validation and persistence boundaries. The HTTP client must support schedules, corrected-game pages, and complete live-game feeds without assuming where their bytes will be stored.

## Decision

Introduce a storage-neutral `MlbStatsApiClient` with typed methods for the public resources currently in scope. A successful method returns exact response bytes, request and response URLs, response headers, status, and attempt count. Resource contracts such as `RawGameResponse` remain responsible for parsing and validating the JSON; storage adapters remain responsible for persistence and idempotency.

Use a minimal injectable `HttpTransport` protocol. The default implementation uses Python's standard library, preserving the project's dependency-free runtime foundation. Tests supply a deterministic fake transport rather than patching global network functions.

Every attempt has an explicit timeout. Retry only failures that are reasonably transient:

- Transport failures where no HTTP response was received.
- HTTP 408, 425, 429, 500, 502, 503, and 504.

Use bounded exponential delays and honor a numeric `Retry-After` header up to the configured maximum. Do not retry other client responses, such as 400, 401, 403, or 404. Distinguish transport exhaustion from a final unsuccessful HTTP response with separate exception types.

## Alternatives considered

- Keep HTTP calls inside acquisition orchestration. This reduces files but couples network, contract, and storage behavior and makes failure testing harder.
- Return parsed contract objects from the client. This is convenient but conflates successful HTTP retrieval with source-schema validity and loses a clean raw-byte handoff.
- Depend immediately on a third-party HTTP library. Libraries such as `httpx` offer useful capabilities, but the current synchronous requirements are small enough for an isolated standard-library transport. The protocol permits replacement without changing client consumers.
- Retry every unsuccessful response. This can amplify invalid requests and authorization problems while adding latency without a reasonable chance of success.
- Retry indefinitely. This prevents an acquisition run from reaching a durable failed state and makes operational timing unpredictable.

## Consequences

- HTTP, schema, and storage failures can be handled and tested independently.
- Acquisition services can record exact source provenance and attempt counts.
- Tests run without network access or real delays.
- The default transport is synchronous; concurrency belongs in later orchestration rather than inside individual requests.
- Retry jitter, connection pooling, or richer telemetry can be added through the client or a replacement transport if production evidence requires them.
