# HerdDB - Development Guidelines

## Local Build Dependencies (jvector + BookKeeper)
HerdDB depends on two artifacts that are **not** on Maven Central:

1. **jvector** — the `eolivelli/jvector` fork (`main` branch). Built on the
   default JDK (currently 25).
2. **BookKeeper 4.17.4-SNAPSHOT** — the `eolivelli/bookkeeper` fork on branch
   `fix-concurrentlonghashmap-record-4.17`, carrying a temporary fix for the
   `PendingAddOp` leak / `ConcurrentLongHashMap` record issue tracked in
   issue #435. The `circe-checksum` module bundles a Lombok 1.18.30
   annotation processor that **does not** run on JDK 25 (`TypeTag :: UNKNOWN`),
   so this build must use **JDK 17**. CI follows the same pattern (see
   `.github/workflows/ci.yml`).

Bootstrap them once into your `~/.m2/repository` — they live there permanently
and can be reused for every PR / agent run:

```
# jvector — default JDK is fine
git clone https://github.com/eolivelli/jvector ~/dev/jvector
mvn -f ~/dev/jvector/pom.xml -DskipTests -Drat.skip=true clean install

# BookKeeper — must build on JDK 17
git clone --branch fix-concurrentlonghashmap-record-4.17 \
    https://github.com/eolivelli/bookkeeper ~/dev/bookkeeper
JAVA_HOME=$(sdk home java 17.0.19-tem) PATH=$JAVA_HOME/bin:$PATH \
  mvn -f ~/dev/bookkeeper/pom.xml -DskipTests \
      -Drat.skip=true -Dspotbugs.skip=true \
      -Dcheckstyle.skip=true -Dlicense.skip=true \
      clean install
```

After bootstrap, the artifacts you care about are under:
- `~/.m2/repository/io/github/jbellis/`  (jvector)
- `~/.m2/repository/org/apache/bookkeeper/` (incl. `bookkeeper-server`,
  `bookkeeper-common`, `bookkeeper-stats-api`, `circe-checksum`, all
  4.17.4-SNAPSHOT)

For agent workflows that use an isolated Maven repo
(`-Dmaven.repo.local=$MAVEN_REPO`), do **not** re-clone and re-build these
dependencies on every run. Instead, copy the relevant subtrees from
`~/.m2/repository` into `$MAVEN_REPO` once at the start of the run:

```
mkdir -p "$MAVEN_REPO/io/github" "$MAVEN_REPO/org/apache"
cp -r ~/.m2/repository/io/github/jbellis     "$MAVEN_REPO/io/github/"
cp -r ~/.m2/repository/org/apache/bookkeeper "$MAVEN_REPO/org/apache/"
```

Re-run the bootstrap commands above whenever the upstream `eolivelli/jvector`
or `eolivelli/bookkeeper` branch heads move.

## Git Workflow
Never push commits directly to the `master` branch. Always create a feature branch and
open a pull request so that CI runs and changes can be reviewed before merging.

## Before Sending a PR
Run the code validation checks locally before opening a pull request:
```
mvn -B checkstyle:check apache-rat:check spotbugs:check install -DskipTests -Pci
```
This matches what `.github/workflows/pr-validation.yml` runs in CI (checkstyle, Apache RAT license headers, SpotBugs).

For any change related to indexes, checkpoints, or concurrency, also run every
subclass of `DirectMultipleConcurrentUpdatesSuite` (in
`herddb-core/src/test/java/herddb/server/hammer/`), the BLink-level
concurrent search/insert hammer (`BLinkConcurrentSearchInsertTest` in
`herddb-utils/src/test/java/herddb/index/blink/`), and — for any change
that touches the lazy data-page read path or the value cache — every
subclass of `LazyValueCacheConcurrentUpdatesSuite` (in
`herddb-remote-file-service/src/test/java/herddb/remote/hammer/`); make
sure they all pass:
```
mvn -pl herddb-core -Dtest='DirectMultipleConcurrentUpdatesSuiteNoIndexesTest,DirectMultipleConcurrentUpdatesSuiteWithNonUniqueIndexesTest,DirectMultipleConcurrentUpdatesSuiteWithUniqueIndexesTest' test
mvn -pl herddb-utils -Dtest='BLinkConcurrentSearchInsertTest' test
mvn -pl herddb-remote-file-service -Dtest='LazyValueCacheConcurrentUpdatesSuiteNoIndexesTest,LazyValueCacheConcurrentUpdatesSuiteWithNonUniqueIndexesTest,LazyValueCacheConcurrentUpdatesSuiteWithUniqueIndexesTest' test
```
The `DirectMultipleConcurrentUpdatesSuite` tests exercise concurrent DML with
periodic checkpoints and recovery against the file DSM, so they are the main
regression gate for the primary-key index, the checkpoint pipeline, and
transaction isolation. `BLinkConcurrentSearchInsertTest` hammers the
lower-level BLink with many concurrent searches against fewer concurrent
writers on overlapping keys, catching regressions in the per-node and anchor
lock primitives that drive every primary-key lookup.
`LazyValueCacheConcurrentUpdatesSuite` mirrors the file-DSM hammer's workload
shape (insert + concurrent UPDATE/GET + post-restart consistency check) but
routes every read through `RemoteFileDataStorageManager` + `LazyValueCache`,
so it is the regression gate for the lazy data-page read path, the value
cache's refcount discipline, and post-restart recovery against remote
storage. Run them multiple times when the first pass is green to reduce the
chance of a flake masking a real bug.

## Test Categories
Tests that require ZooKeeper/BookKeeper infrastructure (cluster mode) must be annotated with
`@Category(ClusterTest.class)` (import `herddb.core.ClusterTest` and `org.junit.experimental.categories.Category`).
This includes any test that:
- Uses `ZKTestEnv`
- Extends `ReplicatedLogtestcase`, `MultiServerBase`, or `BookkeeperFailuresBase`
- Starts multiple `Server` instances with ZooKeeper coordination

CI runs cluster and core tests in separate workflows. Forgetting the annotation means the test
will only run in the core workflow and will likely fail there due to missing infrastructure.

## Testing Code Changes
Always cover code changes with unit or integration tests. Run the new tests locally and ensure
they pass before opening a PR. Also run the existing tests related to the code touched by the
current work to catch regressions.

Never run the full `herddb-core` test suite — it takes a very long time. Instead, run single
tests or small batches using Maven's `-Dtest=...` selector, for example:
```
mvn -pl herddb-core -Dtest=MyChangedTest test
mvn -pl herddb-core -Dtest='Foo*Test,BarTest#someMethod' test
```

## Catching Exceptions
Never catch `Throwable` or bare `Exception` unless strictly necessary. Catch the
narrowest exception type(s) the code in the `try` block can actually throw. If a broad
catch is unavoidable (e.g. isolating a plugin boundary, preventing a background thread
from dying, top-level request handlers), add a comment explaining *why* the broad catch
is required.

## k3s-local Vector Benchmark
Monitoring and supervision guidance for the k3s-local vector benchmark lives in the
`herddb-k3s-bench` agent definition at `.claude/agents/herddb-k3s-bench.md`.
