# Google Sheets Demo

This example is a small local web app packaged as one Docker image.

The browser owns the Google login and talks to Google Sheets directly. The
Python server stays focused on CSV parsing and data-quality checks. Lightweight
sheet-only cleanup steps stay in the browser to keep the round-trips small.

This keeps the design signal clear and supports the distribution goal:

- the demo consumes the public `off_data_quality.checks` API
- the same app could live in a separate repository and depend on a wheel
- one built image can be run locally with one `docker run` command
- the end user only sees `Login with Google` and `Choose spreadsheet`

## Scope

This MVP is intentionally CSV-first.

Included flows:

- load a CSV file into the `Data` sheet
- validate the `Data` sheet with the public Python checks API
- clear derived validation output
- prepare a `Ready for OFF upload` sheet with only passing rows
- show a placeholder for the future Open Food Facts upload step

## Layout

- `server.py`: tiny Python HTTP server
- `api.py`: JSON endpoints and payload shaping
- `data_sources.py`: CSV ingestion
- `workflow.py`: validation and sheet transformation logic
- `web/`: static HTML, CSS, and browser-side Sheets integration
- `Dockerfile` and `compose.yaml`: local app packaging

## Authentication model

The user-facing flow starts from `Choose spreadsheet`. If Google access is
needed, the page opens the Google sign-in flow first and then opens Google
Picker for file selection.

The image carries the Google OAuth client ID, the Google Picker API key, and
the Google Cloud project number for the demo app. The browser uses Google
Identity Services for the login and Google Picker for file selection.

Those values are public app identifiers, not private backend secrets. A public
image must still keep the API key restricted to localhost referrers and the
Picker API, and it should use a dedicated Google Cloud project because quota is
shared across every user of the image.

The access model is intentionally narrower than the earlier URL-based version:

- Google Picker selects the spreadsheet from Drive
- the browser requests the `drive.file` scope
- the app then reads and writes the chosen spreadsheet through the Google
  Sheets API

The end user does not provide any JSON file, secret, or environment variable.
There is no client secret in this runtime design.

## Maintainer setup

To build a distributable image, configure the Google app once:

1. Create a Google Cloud project.
2. Enable `Google Sheets API`.
3. Enable `Google Picker API`.
4. Create an OAuth client for a Web application.
5. Add `http://localhost` and `http://localhost:8501` as authorized JavaScript
   origins.
6. Create an API key for Google Picker.
7. Restrict the API key to the Picker API and localhost referrers.
8. Note the Google Cloud project number.
9. If you publish from GitHub Actions, add these repository secrets:
   - `GOOGLE_SHEETS_DEMO_GOOGLE_CLIENT_ID`
   - `GOOGLE_SHEETS_DEMO_GOOGLE_API_KEY`
   - `GOOGLE_SHEETS_DEMO_GOOGLE_CLOUD_PROJECT_NUMBER`
10. Build the image with:
   - `GOOGLE_SHEETS_DEMO_GOOGLE_CLIENT_ID`
   - `GOOGLE_SHEETS_DEMO_GOOGLE_API_KEY`
   - `GOOGLE_SHEETS_DEMO_GOOGLE_CLOUD_PROJECT_NUMBER`

This setup is much lighter than a `spreadsheets`-scope app that works from an
arbitrary pasted URL. With `drive.file` and Picker, the demo can usually stay
outside the heavier sensitive-scope verification path.

## Run it

Run the published image:

```bash
docker run --rm -p 127.0.0.1:8501:8501 ghcr.io/bobcorn/openfoodfacts-data-quality:google-sheets-demo
```

Then open [http://localhost:8501](http://localhost:8501).

This tag is published by
`.github/workflows/publish-google-sheets-demo-image.yml`. Versioned releases
also publish tags such as `google-sheets-demo-1.2.3`.

The older application demo image stays separate on the `demo` tag.

For a local build with Compose:

```bash
cp .env.example .env
cd examples/google_sheets_demo
docker compose up --build
```

By default the Compose file binds the app only to `127.0.0.1`, not the whole
local network.

To build one distributable image locally with the Google client ID baked in:

```bash
/Users/marco/Development/openfoodfacts-data-quality/examples/google_sheets_demo/build_image.sh \
  your-client-id.apps.googleusercontent.com \
  your-google-picker-api-key \
  your-google-cloud-project-number
```

## Before the demo

1. Create or choose a Google Sheet.
2. Click `Choose spreadsheet`.
3. Sign in with Google if the page asks for it.
4. Pick the file from Google Drive.
5. If the sheet already has data, skip the CSV upload step.
6. If you want to start from local data, upload a CSV file from the page.

## Suggested demo flow

1. Click `Choose spreadsheet`.
2. Sign in with Google if the page asks for it.
3. Pick the file from Google Drive.
4. Upload the CSV into `Data`, if needed.
5. Click `Validate data`.
6. Show the derived `dq_*` columns and row highlighting in `Data`.
7. Open the selected sheet in Google Sheets and fix a few rows.
8. Click `Validate data` again.
9. Click `Prepare upload candidates`.
10. Show `Ready for OFF upload`.
11. Click `Upload to Open Food Facts` and show that the final integration is still pending.

## Environment variables

For local Compose builds, `.env` only carries optional builder settings:

- `GOOGLE_SHEETS_DEMO_BIND_HOST`
- `GOOGLE_SHEETS_DEMO_PORT`
- `GOOGLE_SHEETS_DEMO_GOOGLE_CLIENT_ID`
- `GOOGLE_SHEETS_DEMO_GOOGLE_API_KEY`
- `GOOGLE_SHEETS_DEMO_GOOGLE_CLOUD_PROJECT_NUMBER`

End users do not need any environment variables if they run a prebuilt image.

## Why Docker builds a wheel

The image builds and installs the packaged library wheel first, then layers the
small demo app on top of it.

That makes the dependency boundary obvious: the demo is a client of the public
library API, not a special in-repo integration that reaches into internal
runtime modules.
