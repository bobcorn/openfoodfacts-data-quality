# CI and Releases

[Documentation](../index.md) / [Operations](index.md) / CI and Releases

GitHub Actions currently covers four workflows:

- pull request title validation
- validation
- demo image publishing
- Python release publishing

## Pull Request Titles

`.github/workflows/semantic-pr.yml` runs on pull requests from branches in this repository.

It validates pull request titles against the Conventional Commits style used for squash merges on `main`.

This workflow is meant to keep pull request titles and squashed commit messages aligned.

## Validation

`.github/workflows/ci.yml` runs on pushes, pull requests, and manual dispatch.

It does four things:

- installs the project with app and dev dependencies
- runs `make check`
- builds the source and wheel distributions
- verifies and smoke-tests the built wheel

This workflow is the repository CI gate.

## Demo Image

`.github/workflows/publish-demo-image.yml` runs on version tags.

It:

- reruns the validation gate
- builds the Docker `demo` target
- publishes a multi-architecture image to GHCR

The demo image runs the parity application with demo-oriented defaults against the bundled sample snapshot, on top of the Open Food Facts server base image used for legacy backend execution.

That base image is pinned through the repository `Dockerfile` and comes from the custom multi-arch backend flow documented in [Legacy Backend Image](legacy-backend-image.md).

## Python Release

`.github/workflows/release-library.yml` also runs on version tags.

It:

- reruns the validation gate
- checks that the git tag matches the project version
- builds the source and wheel distributions
- verifies and smoke-tests the built wheel
- attaches the distributions to the matching GitHub Release

## Next Reads

- [Testing and Quality](../reference/testing-and-quality.md)
- [Local Development](../guides/local-development.md)
- [Legacy Backend Image](legacy-backend-image.md)
- [Configuration and Artifacts](configuration-and-artifacts.md)
