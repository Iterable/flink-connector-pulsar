# v5.0.0-iterable Flink 2.2 / Pulsar 4.x Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Release `5.0.0-iterable` of the Pulsar connector running on Flink 2.2.0 + Pulsar 4.0.9, retaining all Iterable custom features.

**Architecture:** Branch `v5.0-iterable` off `v4.2-iterable` (already done — carries the spec), merge `streamnative/main` (the maintained Flink-2.2 + Pulsar-4 trunk) into it with our branch as first parent, resolve the 13 known merge conflicts, fix compilation of our custom code against Flink-2 APIs, get tests green on JDK 11 and 17, then version-bump and release.

**Tech Stack:** Java 11/17, Maven (multi-module), Apache Flink 2.2.0, Apache Pulsar client 4.0.9, JUnit 5, Testcontainers, GitHub Actions.

## Global Constraints

- **Flink version:** `2.2.0` (from streamnative) — copied verbatim into `pom.xml` `<flink.version>`.
- **Pulsar version:** `4.0.9` (from streamnative) — `<pulsar.version>`.
- **Release version:** `5.0.0-iterable` — replaces `4.2.5-iterable` in all four poms.
- **Fork coordinates:** groupId `com.iterable.flink` must be retained in all poms.
- **Release JDKs:** both 11 and 17 must compile and test green before release (only Java 8 is dropped by Flink 2.x).
- **Java 8 is dropped:** CI matrix must not include JDK 8.
- **Work branch:** `v5.0-iterable` (current branch). Never commit upgrade work to `v4.2-iterable`.
- **The four poms carrying version + coordinates:** `pom.xml`, `flink-connector-pulsar/pom.xml`, `flink-sql-connector-pulsar/pom.xml`, `flink-connector-pulsar-e2e-tests/pom.xml`.
- **No `git push` / no tag push** until a human explicitly approves the release (Task 9).

---

### Task 1: Perform the merge and capture conflict state

**Files:**
- Modify (merge): entire tree on branch `v5.0-iterable`

**Interfaces:**
- Consumes: branch `v5.0-iterable` @ `be9305e` (has spec), remote ref `streamnative/main` @ `5bbbf74`.
- Produces: an in-progress merge with exactly 7 conflicted files for Tasks 2–4 to resolve. Merge is NOT committed in this task.

- [ ] **Step 1: Confirm starting state**

Run:
```bash
cd /Users/victor.babenko/code/flink/flink-connector-pulsar
git checkout v5.0-iterable
git status
git log --oneline -1
```
Expected: on `v5.0-iterable`, clean tree, HEAD is `be9305e Add design spec...`.

- [ ] **Step 2: Fetch latest streamnative/main**

Run:
```bash
git fetch streamnative main
git log --oneline -1 streamnative/main
```
Expected: prints `5bbbf74 [improve] A checkpoint only requires...` (or newer — if newer, note the new SHA; the conflict set may shift slightly and Tasks 2–4 still apply by the same rules).

- [ ] **Step 3: Start the merge (do not commit)**

Run:
```bash
git merge --no-commit --no-ff streamnative/main
```
Expected: `Automatic merge failed; fix conflicts and then commit the result.`

- [ ] **Step 4: Verify the conflict set matches the plan**

Run:
```bash
git diff --name-only --diff-filter=U
```
Expected exactly these 7 files:
```
.github/workflows/push_pr.yml
.github/workflows/weekly.yml
flink-connector-pulsar/src/main/java/org/apache/flink/connector/pulsar/source/config/SourceConfiguration.java
flink-connector-pulsar/src/test/java/org/apache/flink/connector/pulsar/sink/writer/PulsarWriterTest.java
flink-sql-connector-pulsar/pom.xml
flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE
pom.xml
```
If the set differs (e.g. streamnative/main advanced), record the actual set; the resolution rules in Tasks 2–4 (fork identity = ours, Flink-2/Pulsar-4 churn = theirs) still govern. Do not commit yet — proceed to Task 2.

---

### Task 2: Resolve Java source conflicts (SourceConfiguration, PulsarWriterTest)

**Files:**
- Modify: `flink-connector-pulsar/src/main/java/org/apache/flink/connector/pulsar/source/config/SourceConfiguration.java`
- Modify: `flink-connector-pulsar/src/test/java/org/apache/flink/connector/pulsar/sink/writer/PulsarWriterTest.java`

**Interfaces:**
- Consumes: in-progress merge from Task 1.
- Produces: both Java files conflict-free, taking the streamnative side verbatim (these are pure Flink-2 migration churn in files Iterable never customized).

- [ ] **Step 1: Resolve SourceConfiguration.java — take streamnative ("theirs")**

This file was never modified by Iterable; the conflict is pure base divergence (streamnative refactored the config-accessor API and changed `fetchOneMessageTime` from `long` to `int`).

Run:
```bash
git checkout --theirs flink-connector-pulsar/src/main/java/org/apache/flink/connector/pulsar/source/config/SourceConfiguration.java
git add flink-connector-pulsar/src/main/java/org/apache/flink/connector/pulsar/source/config/SourceConfiguration.java
```

- [ ] **Step 2: Verify no conflict markers remain in SourceConfiguration**

Run:
```bash
grep -nE '^(<<<<<<<|=======|>>>>>>>)' flink-connector-pulsar/src/main/java/org/apache/flink/connector/pulsar/source/config/SourceConfiguration.java
```
Expected: no output.

- [ ] **Step 3: Resolve PulsarWriterTest.java — take streamnative ("theirs")**

This is Flink-2 sink-test scaffolding (`JobInfoImpl`/`TaskInfoImpl` imports, `InternalSinkWriterMetricGroup.wrap(...)` replacing the old mock helper). Iterable did not customize this test.

Run:
```bash
git checkout --theirs flink-connector-pulsar/src/test/java/org/apache/flink/connector/pulsar/sink/writer/PulsarWriterTest.java
git add flink-connector-pulsar/src/test/java/org/apache/flink/connector/pulsar/sink/writer/PulsarWriterTest.java
```

- [ ] **Step 4: Verify no conflict markers remain in PulsarWriterTest**

Run:
```bash
grep -nE '^(<<<<<<<|=======|>>>>>>>)' flink-connector-pulsar/src/test/java/org/apache/flink/connector/pulsar/sink/writer/PulsarWriterTest.java
```
Expected: no output.

- [ ] **Step 5: No commit yet**

The merge is committed once as a unit in Task 5. Do not commit here.

---

### Task 3: Resolve CI workflow conflicts (matrix → Flink 2.2.0, JDK 11+17)

**Files:**
- Modify: `.github/workflows/push_pr.yml` (rename collision with streamnative `ci.yml`)
- Modify: `.github/workflows/weekly.yml` (rename collision with streamnative `daily.yml`)

**Interfaces:**
- Consumes: in-progress merge from Task 1.
- Produces: `push_pr.yml` testing Flink `2.2.0` on JDK `[11, 17]` with streamnative's longer timeouts; `weekly.yml` cleaned to Flink 2.2.0 only.

- [ ] **Step 1: Inspect the push_pr.yml conflict**

Run:
```bash
git diff .github/workflows/push_pr.yml
```
You will see HEAD has `flink: [ 1.20.3 ]` / `jdk: [ '8, 11, 17' ]` and streamnative has `flink: [ 2.2.0 ]` / `jdk: [ 11, 17 ]` plus longer timeouts (`timeout_test: 65`).

- [ ] **Step 2: Edit push_pr.yml — set the matrix block**

Replace the entire conflict region (from `<<<<<<<` through `>>>>>>>`) so the `compile_and_test` job reads exactly:

```yaml
  compile_and_test:
    strategy:
      matrix:
        flink: [ 2.2.0 ]
        jdk: [ 11, 17 ]
    uses: apache/flink-connector-shared-utils/.github/workflows/ci.yml@ci_utils
    with:
      flink_version: ${{ matrix.flink }}
      jdk_version: ${{ matrix.jdk }}
      timeout_global: 120
      timeout_test: 80
```

Note: `timeout_test: 80` (keep the larger of our 80 and streamnative's 65, since the Java-17 hang is a known risk). Keep the rest of the file (the `check_dead_links` job, the workflow name comment) from our side — only the matrix region is in conflict.

- [ ] **Step 3: Verify push_pr.yml is clean**

Run:
```bash
grep -nE '^(<<<<<<<|=======|>>>>>>>)' .github/workflows/push_pr.yml
git add .github/workflows/push_pr.yml
```
Expected: no marker output.

- [ ] **Step 4: Resolve weekly.yml — take streamnative structure, prune dead rows**

The streamnative side (`daily.yml`) lists stale Flink branches (1.16.2/v3.0, 1.17.2/v4.0, 1.18-SNAPSHOT/main). We keep only a Flink 2.2.0 entry.

Edit `.github/workflows/weekly.yml`: replace the conflict region so the build matrix contains a single entry:
```yaml
        }, {
          flink: 2.2.0,
          branch: main
```
Remove the `1.16.2`, `1.17.2`, and `1.18-SNAPSHOT` entries. Keep the surrounding YAML structure intact (the opening/closing of the matrix list).

- [ ] **Step 5: Verify weekly.yml is clean and valid**

Run:
```bash
grep -nE '^(<<<<<<<|=======|>>>>>>>)' .github/workflows/weekly.yml
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/weekly.yml')); print('YAML OK')"
git add .github/workflows/weekly.yml
```
Expected: no markers, then `YAML OK`.

- [ ] **Step 6: No commit yet** (committed in Task 5).

---

### Task 4: Resolve pom conflicts (versions, deps, fork identity)

**Files:**
- Modify: `pom.xml`
- Modify: `flink-sql-connector-pulsar/pom.xml`
- Modify: `flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE`

**Interfaces:**
- Consumes: in-progress merge from Task 1.
- Produces: root pom on Flink 2.2.0 / Pulsar 4.0.9 / Jackson 2.18.6 / OpenTelemetry with `com.iterable.flink` retained; SQL connector pom keeping Iterable identity but shading OpenTelemetry; NOTICE marked for regeneration in Task 6.

- [ ] **Step 1: Resolve root pom.xml — 3 hunks**

Edit `pom.xml`:

Hunk 1 (versions block) — replace the conflict region with streamnative's values:
```xml
        <flink.version>2.2.0</flink.version>
        <flink-ci-tools.version>2.2.0</flink-ci-tools.version>
        <pulsar.version>4.0.9</pulsar.version>
        <opentelemetry.version>1.56.0</opentelemetry.version>
```
(Discards our old `1.20.3` / `3.0.12`. Pulsar pin decision: use streamnative's `4.0.9` per spec.)

Hunk 2 (jackson) — take streamnative:
```xml
        <jackson-bom.version>2.18.6</jackson-bom.version>
```
(Keep the `snappy.java.version` line if it exists on our side and is absent on theirs — verify with the diff; do not drop non-conflicting properties.)

Hunk 3 (dependencyManagement) — take streamnative's OpenTelemetry entries:
```xml
            <dependency>
                <groupId>io.opentelemetry</groupId>
                <artifactId>opentelemetry-api</artifactId>
                <version>${opentelemetry.version}</version>
            </dependency>

            <dependency>
                <groupId>io.opentelemetry</groupId>
                <artifactId>opentelemetry-api-incubator</artifactId>
                <version>${opentelemetry.version}-alpha</version>
            </dependency>

            <!-- For dependency convergence -->
```
Preserve our `<!-- Start of pinned versions for dependency convergence -->` comment if it introduces our own pinned-version block below — i.e. keep both the OTel block AND our pinned section; they are adjacent, not mutually exclusive.

- [ ] **Step 2: Verify root pom is clean and groupId retained**

Run:
```bash
grep -nE '^(<<<<<<<|=======|>>>>>>>)' pom.xml
grep -n "com.iterable.flink" pom.xml
grep -nE "flink.version|pulsar.version" pom.xml | head -3
git add pom.xml
```
Expected: no markers; `com.iterable.flink` present; `2.2.0` and `4.0.9` shown.

- [ ] **Step 3: Resolve flink-sql-connector-pulsar/pom.xml — 4 hunks**

Three hunks are fork identity — **keep ours** (`com.iterable.flink`). One hunk adds OpenTelemetry shade includes — **take streamnative**.

For each of the 3 identity hunks, keep the `com.iterable.flink` / `flink-connector-pulsar-parent` / `<include>com.iterable.flink:flink-connector-pulsar</include>` lines (HEAD side). For the version inside this pom keep `4.2.5-iterable` for now (Task 8 bumps it to `5.0.0-iterable` everywhere at once).

For the 4th hunk, add streamnative's shade includes into the `<artifactSet><includes>` block:
```xml
                  <include>io.opentelemetry:opentelemetry-api</include>
                  <include>io.opentelemetry:opentelemetry-api-incubator</include>
                  <include>io.opentelemetry:opentelemetry-context</include>
                  <include>io.opentelemetry:opentelemetry-common</include>
```

- [ ] **Step 4: Verify SQL pom is clean and identity retained**

Run:
```bash
grep -nE '^(<<<<<<<|=======|>>>>>>>)' flink-sql-connector-pulsar/pom.xml
grep -c "com.iterable.flink" flink-sql-connector-pulsar/pom.xml
grep -c "io.opentelemetry" flink-sql-connector-pulsar/pom.xml
git add flink-sql-connector-pulsar/pom.xml
```
Expected: no markers; `com.iterable.flink` count >= 3; `io.opentelemetry` count >= 4.

- [ ] **Step 5: Resolve NOTICE — take streamnative for now (regenerated in Task 6)**

Both sides are stale relative to Pulsar 4.0.9. Take streamnative's side to clear the conflict; Task 6 regenerates the correct content.

Run:
```bash
git checkout --theirs flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE
git add flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE
grep -nE '^(<<<<<<<|=======|>>>>>>>)' flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE
```
Expected: no markers.

- [ ] **Step 6: No commit yet** (committed in Task 5).

---

### Task 5: Commit the merge

**Files:**
- Commit: the merge resolution (all files staged in Tasks 2–4)

**Interfaces:**
- Consumes: fully resolved working tree from Tasks 2–4.
- Produces: a merge commit on `v5.0-iterable` with `streamnative/main` as second parent. Tree may not yet compile (Task 6 handles that).

- [ ] **Step 1: Confirm no conflicts remain anywhere**

Run:
```bash
git diff --name-only --diff-filter=U
git status -s | grep -E '^(UU|AA|DD|U|A )' || echo "all resolved"
```
Expected: empty conflict list; "all resolved" or only staged (`M`/`A`) entries.

- [ ] **Step 2: Commit the merge**

Run:
```bash
git commit -m "$(cat <<'EOF'
Merge streamnative/main: Flink 2.2.0 + Pulsar 4.0.9 migration

Adopts streamnative's Flink-2 migration (source-reader rewrite, sink API,
Pulsar 4.0.9 client, message-dedup and cursor fixes) onto the Iterable fork.
Fork identity (com.iterable.flink) and CI matrix (Flink 2.2.0, JDK 11+17)
preserved. Compilation of custom code against Flink-2 APIs follows.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Verify the merge commit**

Run:
```bash
git log --oneline -1
git log --merges -1 --format='%H %P'
```
Expected: a merge commit (two parent SHAs printed).

---

### Task 6: Make it compile on JDK 17 (adapt custom code to Flink-2 APIs)

**Files:**
- Modify: `flink-connector-pulsar/src/main/java/org/apache/flink/connector/pulsar/source/PulsarSourceBuilder.java` (known: line ~527, `ExecutionConfig` → `SerializerConfig`)
- Modify (as compiler dictates): any of the custom files —
  `common/config/PulsarClientFactory.java`, `common/config/PulsarOptions.java`,
  `sink/writer/topic/MetadataListener.java`,
  `source/enumerator/PulsarSourceEnumerator.java`,
  `source/enumerator/subscriber/{PulsarSubscriber,RequiresPulsarAdmin}.java`,
  `source/enumerator/subscriber/impl/{BasePulsarSubscriber,MultiPatternSubscriber,NamespacePatternSubscriber}.java`
- Modify: `flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE` (regenerate)

**Interfaces:**
- Consumes: merge commit from Task 5.
- Produces: `mvn -DskipTests compile test-compile` passing on JDK 17. Custom feature code adapted to Flink-2 serialization/source/sink APIs.

- [ ] **Step 1: Select JDK 17 and attempt a compile**

Run:
```bash
java -version   # confirm 17; if not, switch via your JDK manager
mvn -q -DskipTests clean compile 2>&1 | tee /tmp/compile-17.log | tail -40
```
Expected: FAIL. Capture the first compile errors from `/tmp/compile-17.log`.

- [ ] **Step 2: Fix the known PulsarSourceBuilder API change**

In `PulsarSourceBuilder.java`, the deserialization-schema overload uses the removed `ExecutionConfig`. Change the import and signature to `SerializerConfig` (matching how streamnative/PR#123 migrated the same method):

Import (line ~22): replace
```java
import org.apache.flink.api.common.ExecutionConfig;
```
with
```java
import org.apache.flink.api.common.serialization.SerializerConfig;
```

Method (line ~527): replace
```java
    public <T extends OUT> PulsarSourceBuilder<T> setDeserializationSchema(
            TypeInformation<T> information, ExecutionConfig config) {
        return setDeserializationSchema(new PulsarTypeInformationWrapper<>(information, config));
    }
```
with
```java
    public <T extends OUT> PulsarSourceBuilder<T> setDeserializationSchema(
            TypeInformation<T> information, SerializerConfig config) {
        return setDeserializationSchema(new PulsarTypeInformationWrapper<>(information, config));
    }
```
(Verify `PulsarTypeInformationWrapper`'s constructor now takes `SerializerConfig` — it does post-merge, since streamnative migrated it. If the wrapper signature differs, match it.)

- [ ] **Step 3: Recompile and fix the next error**

Run:
```bash
mvn -q -DskipTests clean compile 2>&1 | tee /tmp/compile-17.log | tail -40
```
For each error: read the file, identify the removed/changed Flink-2 API, and adapt the call site. Likely categories (fix only what the compiler reports — do not pre-emptively change working code):
- Serialization: `ExecutionConfig` → `SerializerConfig` in any remaining call site.
- Source: residual references to `FutureCompletingBlockingQueue` / `RecordsWithSplitIds` queue plumbing (streamnative removed these; our `PulsarPartitionSplitReader` merged cleanly but verify).
- Sink: `InitContext` → `WriterInitContext` if any custom code touches it.
Repeat Step 3 until main sources compile.

- [ ] **Step 4: Compile tests**

Run:
```bash
mvn -q -DskipTests test-compile 2>&1 | tee /tmp/testcompile-17.log | tail -40
```
Fix test-compile errors the same way. Our custom tests (`NamespacePatternSubscriberTest`, `MultiPatternSubscriberTest`, `MultiPatternSubscriber`-related) are most likely to need adaptation. Repeat until test sources compile.

- [ ] **Step 5: Regenerate the SQL connector NOTICE**

Run:
```bash
mvn -q -pl flink-sql-connector-pulsar -am -DskipTests package 2>&1 | tail -20
```
Then inspect the generated NOTICE/shade output and update `flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE` so the bundled-dependency list reflects Pulsar `4.0.9` and the shaded OpenTelemetry artifacts. Confirm:
```bash
grep -n "pulsar-client" flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE
grep -n "opentelemetry" flink-sql-connector-pulsar/src/main/resources/META-INF/NOTICE
```
Expected: pulsar entries show `4.0.9`; opentelemetry entries present.

- [ ] **Step 6: Full compile gate on JDK 17, then commit**

Run:
```bash
mvn -q -DskipTests clean test-compile && echo "COMPILE OK (17)"
```
Expected: `COMPILE OK (17)`.

```bash
git add -A
git commit -m "$(cat <<'EOF'
Adapt custom code to Flink 2 APIs; regenerate SQL NOTICE

Migrate ExecutionConfig -> SerializerConfig and any remaining removed
Flink-1.x API references in Iterable feature code. Regenerate the SQL
connector NOTICE for Pulsar 4.0.9 + shaded OpenTelemetry.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 7: Confirm it also compiles on JDK 11**

Run (switch to JDK 11 first):
```bash
java -version   # confirm 11
mvn -q -DskipTests clean test-compile && echo "COMPILE OK (11)"
```
Expected: `COMPILE OK (11)`. If JDK 11 reveals additional errors (e.g. APIs available only in 17), fix and amend the commit, then re-verify both JDKs.

---

### Task 7: Get tests green (unit + feature regression + integration) on JDK 17

**Files:**
- Modify (as failures dictate): test files and, if a real regression, the corresponding main source.

**Interfaces:**
- Consumes: compiling tree from Task 6.
- Produces: `mvn verify` green on JDK 17 (and re-checked on 11 in Task 7 Step 6).

- [ ] **Step 1: Run our highest-value feature tests first**

Run:
```bash
mvn -q test -pl flink-connector-pulsar \
  -Dtest='NamespacePatternSubscriberTest,MultiPatternSubscriberTest' 2>&1 | tee /tmp/feature-tests.log | tail -40
```
Expected: PASS. These exercise the Iterable custom subscribers against Flink-2 APIs and are the most likely custom code to break. Fix any failures — if a test asserts old behavior changed by the migration, update the test; if the migration broke real behavior, fix the source (apply superpowers:systematic-debugging).

- [ ] **Step 2: Run the full unit-test suite**

Run:
```bash
mvn -q test 2>&1 | tee /tmp/unit-tests.log | tail -60
```
Expected: BUILD SUCCESS. Triage failures one at a time. Watch specifically for a hang (no progress for many minutes) — this is the known Java-17 risk; if it occurs, note which test class hangs for Step 4.

- [ ] **Step 3: Run integration tests against a Pulsar 4.0.9 container**

Run (Docker must be available):
```bash
mvn -q verify -pl flink-connector-pulsar \
  -Dtest='PulsarSinkITCase,PulsarSourceITCase' \
  -DfailIfNoTests=false 2>&1 | tee /tmp/it-tests.log | tail -60
```
Expected: PASS. These validate the source/sink migration against the real Pulsar 4.0.9 client/broker.

- [ ] **Step 4: If the Java-17 hang appears, debug it**

The hang manifested on apache PR #123 (Java 11 passed, Java 17 hung ~74 min then dumped threads). streamnative's CI claims [11,17] green, so their source fixes may already resolve it — but if a test hangs here:
- Identify the hung class from `/tmp/unit-tests.log` / thread dump.
- Run that class alone with a thread dump on timeout:
  ```bash
  mvn -q test -pl flink-connector-pulsar -Dtest='<HungTestClass>' -Dsurefire.timeout=300 2>&1 | tail -60
  ```
- Apply superpowers:systematic-debugging. Likely suspects: a source reader that no longer terminates after the `FutureCompletingBlockingQueue` removal, or a Testcontainers/networking interaction specific to the JDK-17 image.
- Do not silently `@Disabled` a test to go green — if a test must be excluded, document why in the commit and surface it in Task 9.

- [ ] **Step 5: Full verify gate on JDK 17, then commit**

Run:
```bash
mvn -q clean verify 2>&1 | tee /tmp/verify-17.log | tail -20
echo "exit: ${PIPESTATUS[0]}"
```
Expected: BUILD SUCCESS, exit 0.

```bash
git add -A
git commit -m "$(cat <<'EOF'
Fix tests for Flink 2.2 / Pulsar 4 migration

Green on JDK 17: feature regression (namespace/multi-pattern subscribers),
unit suite, and Pulsar 4.0.9 integration tests.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```
(If no source/test changes were needed beyond Task 6, skip the commit.)

- [ ] **Step 6: Re-verify on JDK 11**

Run (switch to JDK 11):
```bash
java -version   # confirm 11
mvn -q clean verify 2>&1 | tee /tmp/verify-11.log | tail -20
echo "exit: ${PIPESTATUS[0]}"
```
Expected: BUILD SUCCESS, exit 0. Fix any JDK-11-only failures and amend, then re-verify both JDKs. **Release gate: both /tmp/verify-11.log and /tmp/verify-17.log must show BUILD SUCCESS.**

---

### Task 8: Version bump to 5.0.0-iterable

**Files:**
- Modify: `pom.xml`
- Modify: `flink-connector-pulsar/pom.xml`
- Modify: `flink-sql-connector-pulsar/pom.xml`
- Modify: `flink-connector-pulsar-e2e-tests/pom.xml`

**Interfaces:**
- Consumes: green tree from Task 7.
- Produces: all four poms at `5.0.0-iterable`; build still green.

- [ ] **Step 1: Bump the version in all four poms**

Run:
```bash
mvn -q versions:set -DnewVersion=5.0.0-iterable -DgenerateBackupPoms=false
```
This updates the parent version and all module references in one pass.

- [ ] **Step 2: Verify every pom shows the new version and none show the old one**

Run:
```bash
grep -rn "5.0.0-iterable" --include=pom.xml .
grep -rn "4.2.5-iterable" --include=pom.xml . || echo "no stale version refs"
```
Expected: all four poms show `5.0.0-iterable`; "no stale version refs".

- [ ] **Step 3: Confirm groupId is still the fork's**

Run:
```bash
grep -rn "com.iterable.flink" --include=pom.xml . | wc -l
```
Expected: count >= 4 (one per pom).

- [ ] **Step 4: Build gate after bump**

Run:
```bash
mvn -q -DskipTests clean verify 2>&1 | tail -15 && echo "PACKAGE OK"
```
Expected: `PACKAGE OK`.

- [ ] **Step 5: Commit**

Run:
```bash
git add -A
git commit -m "$(cat <<'EOF'
Release version 5.0.0-iterable

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Release readiness review and handoff (human-gated)

**Files:**
- None (verification + human approval only)

**Interfaces:**
- Consumes: `v5.0-iterable` branch with the version-bump commit.
- Produces: a release-readiness summary for human approval. NO push/tag without explicit human sign-off.

- [ ] **Step 1: Assemble the evidence**

Run:
```bash
git log --oneline f012cd8..HEAD
echo "--- JDK 11 verify ---"; tail -3 /tmp/verify-11.log
echo "--- JDK 17 verify ---"; tail -3 /tmp/verify-17.log
grep -rn "5.0.0-iterable" --include=pom.xml . | wc -l
```

- [ ] **Step 2: Produce the readiness summary**

Write a short summary covering: both JDK verify results (must be BUILD SUCCESS), any tests excluded and why (from Task 7 Step 4), Flink 2.2.0 / Pulsar 4.0.9 confirmed in poms, and version `5.0.0-iterable` in all four poms.

- [ ] **Step 3: STOP — request human approval before release**

Present the summary. Do NOT run `git push`, create a tag, or trigger `publish.yml` until a human explicitly approves. The release is published when a human pushes a `v*` tag (the `publish.yml` `on: push: tags: 'v*'` trigger) — that step is intentionally manual.

---

## Notes for the implementer

- **Pulsar cluster version check:** the spec calls for verifying against the actual Iterable Pulsar cluster version before tagging. If the cluster is on a 4.x version other than 4.0.9, raise it during Task 9 — a client-version change would mean re-running Task 7.
- **streamnative/main may advance:** if Task 1 Step 2 shows a SHA newer than `5bbbf74`, the conflict set in Task 1 Step 4 may differ slightly. The resolution rule is invariant: fork identity (`com.iterable.flink`, version, `publish.yml`) stays ours; Flink-2/Pulsar-4 mechanical churn takes streamnative; NOTICE is regenerated; CI matrix is merged to Flink 2.2.0 / JDK [11,17].
- **Reference (do not merge):** apache PR #123 migrated the same `PulsarSourceBuilder` / `PulsarTypeInformationWrapper` methods to `SerializerConfig` — useful as a worked example if the Task 6 API adaptation is unclear.
