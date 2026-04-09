[Back to documentation index](../index.md)

# CI and releases

These GitHub Actions workflows validate, publish, and release the repository.

## CI validation workflow

`.github/workflows/validate-project.yml` runs on pushes and pull requests. You
can also start it manually.

It:

- installs the project with app and dev dependencies
- runs `make check`
- builds the source and wheel distributions
- verifies the built wheel and runs a smoke test

This workflow is the repository CI gate. See
[Validate changes](../how-to/validate-changes.md#run-the-ci-gate).

## Demo image workflow

`.github/workflows/publish-demo-image.yml` runs on version tags.

It:

- reruns the validation gate
- builds the Docker `demo` target
- publishes an image for multiple architectures to GHCR

The demo image runs the
[application flow](../explanation/application-runs.md) against the bundled
[sample snapshot](glossary.md#source-snapshot) on top of the backend image used
for legacy execution.

The repository pins that base image through the root `Dockerfile`. See
[Legacy backend image](legacy-backend-image.md).

## Google Sheets demo image workflow

`.github/workflows/publish-google-sheets-demo-image.yml` also runs on version
tags.

It:

- reruns the validation gate
- checks that the Google Sheets demo build secrets are configured
- builds `examples/google_sheets_demo/Dockerfile`
- publishes a multi-architecture image to GHCR

The published image keeps the older `demo` image untouched. It uses the tag
`google-sheets-demo`, plus versioned tags such as
`google-sheets-demo-1.2.3`.

## Python release workflow

`.github/workflows/publish-python-release.yml` also runs on version tags.

It:

- reruns the validation gate
- checks that the git tag matches the project version
- builds the source and wheel distributions
- verifies the built wheel and runs a smoke test
- attaches the distributions to the matching GitHub Release

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
