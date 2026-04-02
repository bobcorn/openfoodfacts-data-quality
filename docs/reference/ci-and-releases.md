# CI and Releases

[Back to documentation](../index.md)

These GitHub Actions workflows validate, publish, and release the repository.

## Run validation in CI

`.github/workflows/ci.yml` runs on pushes and pull requests. You can also start it manually.

It:

- installs the project with app and dev dependencies
- runs `make check`
- builds the source and wheel distributions
- verifies and smoke tests the built wheel

This workflow is the repository CI gate.

## Publish the demo image

`.github/workflows/publish-demo-image.yml` runs on version tags.

It:

- reruns the validation gate
- builds the Docker `demo` target
- publishes an image for multiple architectures to GHCR

The demo image runs the application against the bundled [sample snapshot](glossary.md#source-snapshot) on top of the Open Food Facts server base image used for legacy backend execution.

The repository pins that base image through the root `Dockerfile`. See [Legacy Backend Image](legacy-backend-image.md).

## Publish the Python release

`.github/workflows/release-library.yml` also runs on version tags.

It:

- reruns the validation gate
- checks that the git tag matches the project version
- builds the source and wheel distributions
- verifies and smoke tests the built wheel
- attaches the distributions to the matching GitHub Release

[Back to documentation](../index.md)
