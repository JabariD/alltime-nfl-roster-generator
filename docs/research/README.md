# Research Directory

**Temporary artifacts from exploratory work and agent research.**

This directory is gitignored and should be treated as ephemeral. Clean it out periodically.

## Contents

Research artifacts created during development:
- NGS integration research (nflverse data exploration)
- Example code and integration patterns
- Data source evaluations
- Temporary analysis notebooks

## Cleanup

```bash
# Remove all research artifacts
rm -rf docs/research/*

# Or keep README only
find docs/research -type f ! -name 'README.md' -delete
```

## Moving to Production

If research artifacts become valuable:
1. Extract key insights → Update `docs/design.md` or `docs/milestones.md`
2. Move code examples → Create proper module in `pipeline/` or `tests/`
3. Archive data source notes → Add to `DATA_STRUCTURE.md` or inline documentation
4. Delete the research artifact

**Rule:** Nothing in `docs/research/` should be required for the pipeline to function.
