# Legacy Backend Image

[Back to documentation](../index.md)

Compared runs and enriched application runs rely on this backend image when the [reference path](../concepts/reference-and-parity.md#reference-path) needs live materialization.

## Source

The image comes from the [`data-quality`](https://github.com/bobcorn/openfoodfacts-server/tree/data-quality) branch of the [`bobcorn/openfoodfacts-server`](https://github.com/bobcorn/openfoodfacts-server) fork.

That branch keeps the legacy backend runnable on `linux/amd64` and `linux/arm64` for this repository's workflows and publishes deterministic tags to GHCR.

## Published Tags

The fork publishes two related image families:

- `ghcr.io/<owner>/openfoodfacts-server:main-<sha>-base`
  backend base image for multiple architectures
- `ghcr.io/<owner>/openfoodfacts-server:main-<sha>`
  data-quality image for multiple architectures built on top of that base image

The publish workflow in [`data-quality-server-image.yml`](https://github.com/bobcorn/openfoodfacts-server/blob/data-quality/.github/workflows/data-quality-server-image.yml) checks that `main-<sha>` matches the merge base with `openfoodfacts/openfoodfacts-server` `main` before it builds and pushes images.

This repository pins one of those published tags through `SERVER_BASE_IMAGE` in the root `Dockerfile`.

## Prebaked Data

The data-quality image includes the backend runtime plus prebaked artifacts such as:

- taxonomy build results
- language build outputs
- pro platform field metadata

These artifacts are created in [`Dockerfile.data-quality-server`](https://github.com/bobcorn/openfoodfacts-server/blob/data-quality/Dockerfile.data-quality-server) before the image is published.

They are required because the legacy backend path used here calls `normalize_product_data` and `analyze_and_enrich_product_data`, and it depends on category property lookups plus tag property lookups.

## ARM Support

The fork adds `OFF_SKIP_IMAGE_STACK` so the backend image can build on `linux/arm64`.

On ARM, the build skips the image processing stack behind that flag, including dependencies such as:

- `Image::Magick`
- `Barcode::ZBar`
- `Image::OCR::Tesseract`
- `Imager::*`

That keeps the backend image buildable on ARM for the data-quality flows used here.

## Scope

This dependency matters whenever a run needs [reference results](../concepts/reference-and-parity.md#reference-data) produced by the backend.

That includes:

- `enriched_products` runs, because the application reference path currently materializes [enriched snapshots](data-contracts.md#enriched-snapshot) through the backend
- compared `raw_products` runs, because reference findings still come from legacy emitted tags

The reference loader checks the cache before it starts backend work. A warm cache can satisfy a compared or enriched run without starting a live backend worker for covered products. Cold cache materialization still uses this image.

Execution can skip the backend image when the run needs no comparison, no reference findings, and no enriched snapshots.

## Checkout

A local checkout of `openfoodfacts-server` is not required for the normal [Docker application flow](../how-to/run-the-project-locally.md).

The container already embeds the backend runtime and sets `LEGACY_BACKEND_FINGERPRINT` from the pinned image reference.

A local checkout matters when you need:

- fingerprinting from local source instead of the pinned image fingerprint
- legacy snippet extraction

## Cache Fingerprint

The backend image affects the [reference result cache](run-configuration-and-artifacts.md#reference-cache).

In Docker runs, `LEGACY_BACKEND_FINGERPRINT` is set from `SERVER_BASE_IMAGE`. Changing the pinned backend image changes the reference cache fingerprint and therefore the cache key.

## Updates

To refresh the backend dependency:

1. Update the `data-quality` branch against a newer `openfoodfacts-server` `main`.
2. Publish a new `main-<sha>` image for multiple architectures from the fork workflow.
3. Update `SERVER_BASE_IMAGE` in this repository.
4. Rebuild the data-quality image and let the reference cache compute a new fingerprint.

[Back to documentation](../index.md)
