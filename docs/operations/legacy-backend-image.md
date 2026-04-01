# Legacy Backend Image

[Documentation](../index.md) / [Operations](index.md) / Legacy Backend Image

Application runs in this repository depend on a custom Open Food Facts server image when they may need to materialize reference results through the backend.

## Source

The image comes from the [`data-quality`](https://github.com/bobcorn/openfoodfacts-server/tree/data-quality) branch of the [`bobcorn/openfoodfacts-server`](https://github.com/bobcorn/openfoodfacts-server) fork.

That branch supports this repository's compared and enriched run workflows. It keeps the legacy backend runnable on `linux/amd64` and `linux/arm64` and publishes deterministic tags to GHCR.

## Published Tags

The fork publishes two related image families:

- `ghcr.io/<owner>/openfoodfacts-server:main-<sha>-base`
  Backend base image for multiple architectures.
- `ghcr.io/<owner>/openfoodfacts-server:main-<sha>`
  Data quality image for multiple architectures built on top of that base image.

The publish workflow in [`data-quality-server-image.yml`](https://github.com/bobcorn/openfoodfacts-server/blob/data-quality/.github/workflows/data-quality-server-image.yml) validates that `main-<sha>` matches the merge-base with `openfoodfacts/openfoodfacts-server` `main` before building and pushing images.

This repository pins one of those published tags through `SERVER_BASE_IMAGE` in the root `Dockerfile`.

## Prebaked Data

The data-quality image includes the backend runtime and prebaked artifacts such as:

- taxonomy build results
- language build outputs
- pro platform field metadata

Those artifacts are created in [`Dockerfile.data-quality-server`](https://github.com/bobcorn/openfoodfacts-server/blob/data-quality/Dockerfile.data-quality-server) before the image is published.

These artifacts are required because the legacy backend path used here calls `normalize_product_data`, `analyze_and_enrich_product_data`, category property lookups, and tag property lookups. The image needs the backend in a ready to run state.

## ARM Support

The fork adds `OFF_SKIP_IMAGE_STACK` to make the backend image buildable on `linux/arm64`.

On ARM, the build skips the image processing stack behind that flag, including dependencies such as:

- `Image::Magick`
- `Barcode::ZBar`
- `Image::OCR::Tesseract`
- `Imager::*`

That keeps the backend image buildable on ARM for the data quality flows used here.

## Runtime Scope

This dependency applies to the reference path whenever a run needs backend-backed reference results.

That includes:

- `enriched_products` runs, because the application reference path currently materializes enriched snapshots through the backend
- compared `raw_products` runs, because reference findings still come from legacy emitted tags

The backend emits a versioned result envelope. Python validates that envelope and then projects `ReferenceResult` onto enriched snapshots and reference findings.

The reference loader checks the cache before starting backend work. A warm cache can satisfy a compared or enriched run without starting a live backend worker for the covered products. Cold cache materialization still uses this image.

Runtime only execution can avoid the backend image if it does not need reference findings or enriched snapshots.

## Local Checkout

A local checkout of `openfoodfacts-server` is not required for the normal Docker application flow.

The container already embeds the backend runtime and sets `LEGACY_BACKEND_FINGERPRINT` from the pinned image reference.

A local checkout becomes relevant when you need:

- fingerprinting from local source instead of the pinned image fingerprint
- legacy snippet extraction
- legacy inventory export

## Cache Fingerprint

The backend image also affects the reference result cache.

In Docker runs, `LEGACY_BACKEND_FINGERPRINT` is set from `SERVER_BASE_IMAGE`. Changing the pinned backend image changes the reference result cache fingerprint and therefore the cache key.

## Updating The Dependency

To refresh the backend dependency:

1. update the `data-quality` branch against a newer `openfoodfacts-server` `main`
2. publish a new `main-<sha>` image for multiple architectures from the fork workflow
3. update `SERVER_BASE_IMAGE` in this repository
4. rebuild the data-quality image and let the reference cache compute a new fingerprint

## Next Reads

- [Application Run Flow](../architecture/application-run-flow.md)
- [Local Development](../guides/local-development.md)
- [Configuration and Artifacts](configuration-and-artifacts.md)
- [CI and Releases](ci-and-releases.md)
