# CI and Releases

[Documentation](../index.md) / [Operations](index.md) / CI and Releases

The repository has three GitHub workflows:

- validation
- demo image publishing
- Python release publishing

## Validation Workflow

[`ci.yml`](../../.github/workflows/ci.yml) runs on pushes, pull requests, and manual dispatch.

It does four things:

- installs the project with app and dev dependencies
- runs `make check`
- builds the source and wheel distributions
- verifies and smoke-tests the built wheel

This workflow checks the repository as code and as a distributable package.

## Demo Image Workflow

[`publish-demo-image.yml`](../../.github/workflows/publish-demo-image.yml) runs on version tags.

It:

- reruns the repository validation gate
- builds the Docker `demo` target
- publishes a multi-architecture image to GHCR

The demo image is built from the Docker `demo` target and published to GHCR. It runs the parity application with demo-oriented defaults against the bundled sample snapshot, on top of the Open Food Facts server base image used for legacy backend execution.

## Python Release Workflow

[`release-library.yml`](../../.github/workflows/release-library.yml) also runs on version tags.

It:

- reruns the repository validation gate
- checks that the git tag matches the project version
- builds the source and wheel distributions
- verifies and smoke-tests the built wheel
- attaches the distributions to the matching GitHub Release

This makes the built library available from the GitHub Release page.

[Back to Operations](index.md) | [Back to Documentation](../index.md)
