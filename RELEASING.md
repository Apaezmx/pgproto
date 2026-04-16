# Releasing pgproto

To release a new version of `pgproto`, follow these steps:

## 1. Update Version Numbers
You need to update the version number in the following files:

### `META.json`
Update the `"version"` field in two places:
*   Line 4: `"version": "X.Y.Z"`
*   Line 13: `"version": "X.Y.Z"` inside the `"provides"` section.

### `.github/workflows/release.yml`
Currently, the version is hardcoded in the `source-distribution` job. Update all references to the version (e.g., `0.2.4`) to the new version (lines 70-85).
*(Note: We should ideally make this dynamic in the workflow).*

## 2. Commit Changes
Commit the version updates:
```bash
git add META.json .github/workflows/release.yml
git commit -m "Bump version to X.Y.Z"
git push origin main
```

## 3. Create and Push Tag
Create a git tag with the version prefixed by `v`:
```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

This will trigger the release workflow to build binaries, push the Docker image, and create the release assets.
