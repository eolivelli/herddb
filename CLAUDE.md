# HerdDB - Development Guidelines

## Before Sending a PR
Run the code validation checks locally before opening a pull request:
```
mvn -B checkstyle:check apache-rat:check spotbugs:check install -DskipTests -Pjenkins
```
This matches what `.github/workflows/pr-validation.yml` runs in CI (checkstyle, Apache RAT license headers, SpotBugs).

## Test Categories
Tests that require ZooKeeper/BookKeeper infrastructure (cluster mode) must be annotated with
`@Category(ClusterTest.class)` (import `herddb.core.ClusterTest` and `org.junit.experimental.categories.Category`).
This includes any test that:
- Uses `ZKTestEnv`
- Extends `ReplicatedLogtestcase`, `MultiServerBase`, or `BookkeeperFailuresBase`
- Starts multiple `Server` instances with ZooKeeper coordination

CI runs cluster and core tests in separate workflows. Forgetting the annotation means the test
will only run in the core workflow and will likely fail there due to missing infrastructure.
