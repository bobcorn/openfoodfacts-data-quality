# CI and Releases

[Documentation](../index.md) / [Operations](index.md) / CI and Releases

GitHub Actions currently covers three workflows:

- validation
- demo image publishing
- Python release publishing

## Validation

`.github/workflows/ci.yml` runs on pushes, pull requests, and manual dispatch.

It does four things:

- installs the project with app and dev dependencies
- runs `make check`
- builds the source and wheel distributions
- verifies and smoke tests the built wheel

This workflow is the repository CI gate.

## Demo Image

`.github/workflows/publish-demo-image.yml` runs on version tags.

It:

- reruns the validation gate
- builds the Docker `demo` target
- publishes an image for multiple architectures to GHCR

The demo image runs the application with defaults for the demo against the bundled sample snapshot, on top of the Open Food Facts server base image used for legacy backend execution.

That base image is pinned through the repository `Dockerfile` and comes from the backend flow for multiple architectures documented in [Legacy Backend Image](legacy-backend-image.md).

## Python Release

`.github/workflows/release-library.yml` also runs on version tags.

It:

- reruns the validation gate
- checks that the git tag matches the project version
- builds the source and wheel distributions
- verifies and smoke tests the built wheel
- attaches the distributions to the matching GitHub Release

## Next

- [Testing and Quality](../reference/testing-and-quality.md)
- [Local Development](../guides/local-development.md)
- [Legacy Backend Image](legacy-backend-image.md)
- [Configuration and Artifacts](configuration-and-artifacts.md)
