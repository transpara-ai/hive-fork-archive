# Civilization worker boundary

The Workbench API can delegate route, implementation, ordinary review and
independent verification to Platform's bounded runner. Configure
`CIVILIZATION_RUNNER_URL` and `CIVILIZATION_RUNNER_TOKEN_FILE`; the endpoint must
use HTTPS or loopback HTTP and a separate token of at least 32 characters.

Hive records `civilization.worker.assigned` before dispatch. Confirmation remains
a signed named-human action before implementation. Provider invocations use
stable attempt IDs and record both model evidence and Platform's image, policy,
input and output hashes. Transport outages retain the same assignment and never
fall back to local execution. A completed artifact is fetched before computing a
new input, so a crash after application does not execute it again. Unexpected
worktree changes fail validation instead of being overwritten.

Review evidence now binds the implementation workspace digest. A matching
completed review can survive restart; a later implementation always needs a new
review. Independent verification has its own durable assignment and immutable
artifact binding. Failed verification still creates a repairable intervention;
operator resolution permits a new attempt. The resulting prepared artifact keeps
the verifier receipt inside its signed state event.

The runner request contains only work/attempt identity, repository, operation,
base, canonical patch, prompt and provider selection. Images, commands, mounts,
credentials, resource limits and networks are Platform administrator policy.
Neither Hive nor workers receive a Docker socket. Worker Git metadata is
read-only and each attempt gets its own checkout. Verification has no provider
credential or network.

Deploy the compatible Platform runner first. Enable this API mode only after its
actual worker image and repository checks pass qualification. Remove Codex/Claude
credential and home mounts from the API. The controller still owns Git worktree
preparation, its EventGraph credential/signing key and any explicitly retained
repository-read credential. These are not passed to workers. The API image still
contains legacy tools for compatibility; their presence is not permission to
execute providers when runner mode is enabled.

Remote-runner mode rejects `CIVILIZATION_PUBLISH_ENABLED=true`. Acceptance remains
review of a delivered artifact. Implementing an authorized publishing/deployment
boundary is separate work; this change does not enable pushing, merging or
deployment from an accepted result. The older Hive role daemon is a separate
runtime and has not been converted by the Workbench API switch.

Operational installation, image policy, credential refresh, retention and
recovery instructions live in Platform's `runner/README.md`. Treat attempt homes
as credential-bearing storage; archive access must be restricted accordingly.
