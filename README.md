# TimeoutMigrationTool

## How to Test Locally

Run a RavenDb Docker container for v4:

`docker run -d -p 8080:8080 -p 38888:38888 ravendb/ravendb`

Run a local RavenDb server instance for v3.5 on port 8383.

Run a RabbitMQ Docker container:

`docker run -d --hostname my-rabbit -p 5672:5672 -p 15672:15672  rabbitmq:3-management`

## Project structure

The solution is divided up into a single tool project with multiple test projects. The test are divided into unit test, integration tests and acceptance tests.

### Tests

* Unit tests, `TimeoutMigrationTool.Tests`, are quick tests not needing any infrastructure and can be used by all sources and targets
* Integrations tests are named `TimeoutMigrationTool.{Source|Target}.IntegrationTests` and requires infrastructure for the specific source/target to be present
* Acceptance tests run full NServiceBus endpoints and are named `TimeoutMigrationTool.{Source}.AcceptanceTests`. They contain end to end tests for all supported permutations of targets. So when TargetX is added we would add new test(s) into all existing acceptance tests projects. They require infastrucure for the specific source and all supported targets to be present.

### Manual Test Scenarios

* Single endpoint test
  * A project to generate fake data should be used to generate timeouts for 1 endpoint
  * A preview of the migration should be run 
  * Migration should be run using the cutoffDate to verify that only subset of timeouts is migrated (another preview can be done to make sure that maximum timeout date is no greater than used cutoffDate)
* Multiple endpoints test
  * A project to generate fake data should be run multiple times with changed endpoint names
  * A preview of the migration should be run to see all ednpoints that are available to be migrated
  * A migration should be run for only one of the endpoints to verify that only subset of timeouts is migrated
  * A migration for all remaining endpoints should be run to move the remaining timeouts.
  
  
## Bulk test results

### Raven 3

* For 1 million timeouts, using the index
   * 8m to list endpoints
   * 30min total to do a full prepare
   * Total migration time: 1h1m

* For 100K timeouts, not using the index

### Raven 4


* For 1 million timeouts, using the index


* For 1 million timeouts, not using the index

### Sql P
