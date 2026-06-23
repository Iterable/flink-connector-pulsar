# Design: v5.0.0-iterable — Flink 2.2 / Pulsar 4.x upgrade

**Date:** 2026-06-23
**Status:** Approved
**Author:** Victor Babenko (with Claude Code)

## 1. Goal & scope

Produce `5.0.0-iterable`, a fork release of the Pulsar connector running on
**Flink 2.2.0 + Pulsar 4.0.9**, retaining all current Iterable features:

- Namespace-pattern subscriber (dynamic namespace/topic discovery)
- Multi-pattern subscriber (consume from multiple namespaces)
- PulsarAdmin lifecycle / resource-leak fix
- Split-reader subscription retry (FLINK-38600)
- Partition-name in consumer-creation error messages
- Package-publishing setup (`publish.yml`, `com.iterable.flink` coordinates)

We adopt streamnative's Flink-2 migration wholesale rather than re-implementing
it, layering our features on top.

**Out of scope:** new connector features; upstreaming fixes to apache (deferred);
Java 21 support.

## 2. Strategy & rationale

We merge `streamnative/main` into a branch off our `v4.2-iterable`.

`streamnative/main` (remote `streamnative` = `git@github.com:streamnative/flink-connector-pulsar.git`,
HEAD `5bbbf74` as of 2026-06-17) is the maintained Flink-2.2 + Pulsar-4 trunk. It
is a **superset** of the `upgrade/flink_2.2.0` branch (apache PR #118): that branch
was squash-merged into `streamnative/main` as commit `df5b906`
("[improve] Upgrade to support the new version API of Flink 2.2.0 (#9)"), and
`main` carries one additional fix on top — `5bbbf74`
("[improve] A checkpoint only requires the creation of one transaction (#11)").
Both are Flink 2.2.0 + Pulsar 4.0.9.

Basing on `streamnative/main` gives us, in one merge:

- The maintainers' Flink-2 source-reader migration (removal of
  `FutureCompletingBlockingQueue`, now framework-managed).
- Their source correctness fixes — message-dedup within a fetch
  ("fix repeated receiving messages") and `MessageIdAdv` cursor handling /
  richer snapshot/seek logging.
- The Pulsar 3.0.x → 4.0.9 client bump.
- The Flink-2 sink API migration (`TwoPhaseCommittingSink` → `Sink` +
  `SupportsCommitter`, `InitContext` → `WriterInitContext`).

### Alternatives considered

- **apache PR #123** (`vernedeng:feature/upgrade-flink-2.2.1`, Flink 2.2.1 +
  Pulsar **3.0.5**). Cleanest merge (1 conflict) and freshest Flink, but it is on
  Pulsar 3.x — the Iterable cluster runs Pulsar 4.x — and it lacks streamnative's
  source-correctness fixes. Its Java-17 CI job hangs (Java 11 passes). Rejected
  because it would force us to redo the 3→4 bump and re-discover the message-dedup
  bug ourselves, on the riskiest part of the migration.
- **streamnative `upgrade/flink_2.2.0` branch directly.** Same Flink/Pulsar
  versions, same merge cost, but older (HEAD 2026-05-22) and missing the #11
  checkpoint fix. `streamnative/main` strictly dominates it.
- **Other streamnative branches.** `v5.0-sn` is Flink 2.0.0; `v4.3-sn`/`v4.2-sn`/
  `sn-main` are still Flink 1.x. None match Flink 2.2 + Pulsar 4.

### Trade-off accepted

`streamnative/main` squash-merged the upgrade branch, so streamnative's internal
fixes (message-dedup, cursor logging) arrive inside one large merge rather than as
individually attributable commits. The code is present; granular history is not.
This is acceptable.

### Merge mechanics

Branch `v5.0-iterable` is created from `v4.2-iterable`, then `streamnative/main`
is merged **into** it with our branch as the first parent, so the Iterable line
remains the trunk. A 3-way merge (not literal cherry-picks) is used because the two
lines diverged from a common ancestor (`34f217b`) and both modified overlapping
files; a 3-way merge reconciles this correctly, whereas replaying streamnative's
commits onto our newer base would generate spurious conflicts.

## 3. Decisions

| Decision | Choice |
|----------|--------|
| Base branch | `v5.0-iterable`, branched from `v4.2-iterable` |
| Integration | `git merge streamnative/main` (iterable = first parent / trunk) |
| Flink version | 2.2.0 (from streamnative) |
| Pulsar version | 4.0.9 (from streamnative) |
| Release JDKs | Both 11 and 17 must be green (matches streamnative + apache matrix; only Java 8 is dropped by Flink 2.x) |
| Release label | `5.0.0-iterable` (follows `4.2.5-iterable` 3-part scheme) |
| Fork coordinates | `com.iterable.flink` (retained) |

## 4. Execution phases

### Phase A — Branch & merge
Create `v5.0-iterable` from `v4.2-iterable`; `git merge streamnative/main` with our
branch as first parent.

### Phase B — Resolve merge conflicts (13 hunks / 7 files)
Resolution rule: **fork identity stays ours; Flink-2 / Pulsar-4 churn takes
streamnative; NOTICE is regenerated; CI matrices are merged.**

| File | Hunks | Resolution |
|------|-------|------------|
| `pom.xml` | 3 | Take streamnative versions/deps (Flink 2.2.0, Pulsar 4.0.9, Jackson 2.18.6, OpenTelemetry). Re-assert `com.iterable.flink` groupId and bump version to `5.0.0-iterable`. |
| `flink-sql-connector-pulsar/pom.xml` | 4 | Keep Iterable identity (groupId, shade includes). Take streamnative's OpenTelemetry shade `<include>` entries. |
| `flink-sql-connector-pulsar/.../META-INF/NOTICE` | 1 | Regenerate from build — both sides are stale vs Pulsar 4.0.9. |
| `.github/workflows/push_pr.yml` ← `ci.yml` | 1 | Rename collision. Merge → Flink 2.2.0, JDK `[11, 17]` (drop Java 8). |
| `.github/workflows/weekly.yml` ← `daily.yml` | 1 | Rename collision. Take streamnative structure, set Flink 2.2.0, prune dead 1.x rows. |
| `source/config/SourceConfiguration.java` | 1 | Take streamnative — refactored config-accessor API (`getAndConvert`, typed `get`); `fetchOneMessageTime` `long`→`int`. We never modified this file; conflict is pure base divergence. |
| `sink/writer/PulsarWriterTest.java` | 2 | Take streamnative — Flink-2 sink-test scaffolding (`JobInfoImpl`/`TaskInfoImpl`, `InternalSinkWriterMetricGroup.wrap(...)`). |

None of our custom feature files conflict (they auto-merge): `NamespacePatternSubscriber`,
`MultiPatternSubscriber`, `PulsarClientFactory`, `RequiresPulsarAdmin`,
`PulsarSourceEnumerator`, and the split-reader retry (byte-identical to streamnative's).

### Phase C — Fix compilation
The real work. Auto-merge is textual, not semantic: our custom code still calls
Flink-1.x APIs that streamnative's migration removed. Known adaptations:

- `ExecutionConfig` → `SerializerConfig` in `PulsarSourceBuilder` (and any of our
  additions that touch serializer creation).
- Effects of the `FutureCompletingBlockingQueue` removal on our
  subscriber/source-reader code paths.
- Sink API: `InitContext` → `WriterInitContext` where our code interacts with it.

Iterate until `mvn -DskipTests compile test-compile` passes.

### Phase D — Fix tests
Get the suite green on **both** JDK 11 and 17.

- Regression focus on our highest-value custom code:
  `NamespacePatternSubscriberTest`, `MultiPatternSubscriberTest`.
- Integration tests (`PulsarSinkITCase`, `PulsarSourceITCase`, `PulsarTableITCase`)
  run against a real Pulsar 4.0.9 container.
- Investigate the Java-17 test hang if it reappears (PR #123 exhibited it;
  streamnative's CI claims `[11, 17]` green, so their source fixes may already
  address it — to be confirmed).

### Phase E — Release
Bump to `5.0.0-iterable`, regenerate NOTICE, tag, publish via existing `publish.yml`.

## 5. Testing & verification

- **Per-phase gates:** B → `mvn validate`; C → `mvn -DskipTests compile test-compile`;
  D → full `mvn verify` on both JDK 11 and 17.
- **Feature regression:** subscriber tests must pass post-migration — most likely
  to break against Flink-2 APIs.
- **Integration:** ITCases validate the migration against Pulsar 4.0.9.
- **Cluster compatibility:** verify against the actual Iterable Pulsar cluster
  version before tagging.

## 6. Risks

1. **Java-17 test hang (highest).** Known on apache PR #123 (Java 11 green, Java 17
   hangs after ~74 min as a timeout, not a compile error). Mitigation: streamnative
   tests `[11, 17]`; if it recurs, apply systematic-debugging to the hung suite.
2. **Custom code vs Flink-2 APIs.** Expected compile breakage in Phase C; bounded,
   since the API deltas are known.
3. **Pulsar 3→4 client behavior.** Subtle runtime differences; covered by ITCases
   on a 4.0.9 container.
4. **NOTICE / shade completeness.** Missing OpenTelemetry shade entries cause runtime
   `NoClassDefFound` in the SQL uber-jar; caught by a SQL-connector smoke test.

## 7. Key references

- Base branch: `v4.2-iterable` (Flink 1.20.3 + Pulsar 3.0.12)
- Merge source: `streamnative/main` @ `5bbbf74`
- Shared ancestor of fork lines: `34f217b` ([FLINK-36180] Fix batch message data loss)
- Iterable / apache shared base: `08c8e2e` ([FLINK-38545] Flink 1.20.3 + Pulsar 3.0.5)
- apache PR #123 (rejected base): `vernedeng:feature/upgrade-flink-2.2.1`
- apache PR #118 (squashed into streamnative/main): `streamnative:upgrade/flink_2.2.0`
