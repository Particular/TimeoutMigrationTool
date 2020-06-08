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
