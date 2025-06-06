# GraphDB-wikidata

This project contains a Rust based graph database server. The repository does not ship a GUI by default, so a small HTML frontend is included under the `frontend/` directory.

## Requirements

- Rust toolchain (for building and running the server)
- A browser to open the frontend

## Building the Server

From the project root run:

```bash
cargo build --release
```

## Creating a Database

Prepare a Wikidata JSON dump and build the database:

```bash
cargo run --release -- --database-dir wikidata create-db --file /path/to/wikidata.json
```

## Starting the Server

Launch the API server on port `8005` (default):

```bash
cargo run --release -- --database-dir wikidata server --port 8005
```

The server exposes a `/query` endpoint and allows any origin via CORS so it can be accessed from a browser.

## Running the Frontend

A simple frontend is provided in `frontend/`. Start a static file server in that directory, e.g.:

```bash
cd frontend
python3 -m http.server 8080
```

Then open `http://localhost:8080` in your browser. Enter a SPARQL query and submit it; the frontend will send it to `http://127.0.0.1:8005/query` and display the JSON result.

> The previously used remote GUI at `wikidata.bonxai.org` is currently unavailable. This local frontend serves as a minimal replacement.
