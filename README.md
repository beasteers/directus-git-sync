# directus-git-sync
A lightweight sidecar to apply directus schema and configuration.

Handles:
 - schema
 - flows (and operations)
 - dashboards (and panels)
 - webhooks
 - policies, roles, and permissions
 - non-user presets
 - settings


This is meant for doing GitOps-style bring-up of Directus, meaning that it only exports things that you might want to store in git.

API endpoints it (purposefully) does not handle:
 - collection items
 - files
 - activity
 - folders (for now? idk)
 - notifications
 - relations
 - revisions
 - shares
 - translations

I think there could be a separate script that's specifically for "data" migration/import/export.

`directus-git-sync export` writes the same complete resource set consumed by
`diff` and `apply`. Runtime identities are deliberately excluded: administrator
policies/roles, role user membership, and user-scoped presets remain owned by
the target environment.

`directus-git-sync diff` is non-mutating and emits a structured JSON plan.
`directus-git-sync apply` refuses to run without `--yes`, refuses snapshots
with missing policy dependencies or user bindings, verifies required extension
versions are already installed, and fails if a second plan does not converge.
Application deployment tooling should record and environment-bind the reviewed
plan before invoking apply.
