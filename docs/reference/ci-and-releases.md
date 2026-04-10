[Back to documentation index](../index.md)

# CI and releases

These GitHub Actions workflows validate, publish, and release the repository.

## CI validation workflow

`.github/workflows/validate-project.yml` runs on pushes and pull requests. You
can also start it manually.

It installs the project with app and dev dependencies, runs `make check`,
builds the source and wheel distributions, and verifies the built wheel with a
smoke test.

This workflow is the repository CI gate. See
[Validate changes](../how-to/validate-changes.md#run-the-ci-gate).

## Migration demo image workflow

`.github/workflows/publish-migration-demo-image.yml` runs on version tags.

It reruns the validation gate, builds the Docker `demo` target, and publishes
images for amd64 and arm64 to GHCR.

The migration demo image runs the
[migration flow](../explanation/migration-runs.md) against the bundled
[sample snapshot](glossary.md#source-snapshot) on top of the backend image used
for legacy execution.

The repository pins that base image through the root `Dockerfile`. See
[Legacy backend image](legacy-backend-image.md).

This workflow publishes `migration-demo`. The local root Compose flow builds a
different image from the `runtime` target and tags it as `migration:local`.

It publishes a separate GHCR package:

- `ghcr.io/bobcorn/migration-demo`

Release tags publish `latest`, `1.2.3`, and `1.2`.

## Google Sheets demo image workflow

`.github/workflows/publish-google-sheets-demo-image.yml` also runs on version
tags.

It reruns the validation gate, checks the Google Sheets app build secrets,
builds `apps/google_sheets/Dockerfile`, and publishes images for amd64 and
arm64 to GHCR.

The local Google Sheets Compose flow builds the same app image and tags it as
`google-sheets-demo:local`.

It publishes a separate GHCR package:

- `ghcr.io/bobcorn/google-sheets-demo`

Release tags publish `latest`, `1.2.3`, and `1.2`.

## Python release workflow

`.github/workflows/publish-python-release.yml` also runs on version tags.

It reruns the validation gate, checks that the git tag matches the project
version, builds the source and wheel distributions, verifies the built wheel
with a smoke test, and attaches the distributions to the matching GitHub
Release.

The attached wheel is the packaged install artifact for library users because
the project is not published on PyPI.

## Pull request title workflow

`.github/workflows/validate-pull-request-title.yml` runs on pull request
events.

It validates the pull request title against the repository's allowed
Conventional Commit types.

## See also

- [Validate changes](../how-to/validate-changes.md)
- [Legacy backend image](legacy-backend-image.md)
- [Run configuration and artifacts](run-configuration-and-artifacts.md)

[Back to documentation index](../index.md)
