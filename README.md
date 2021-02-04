# TimeoutMigrationTool

## Delayed message behavior across transports

A delayed message is a message that should not be sent before x date and time. Depending on the transport's capabilities, the delayed message or timeout can be stored by the destination and made available in the input queue when the delay expires, or it might be kept by the source endpoint until the delay expires, and sent out at that point as a normal message.
How this works impacts how the endpoint's source and target are viewed.

Here's an overview per transport:
|   Transport	|   Where are timeouts stored before expiry?	|
|---	|---	|
|   RabbitMQ	|   On the shared delayed delivery infrastructure (shared by all endpoints)	|
|   Azure Storage Queues	|   On the source endpoint in an Azure Table, which is polled	|
|   SQL Server |   On the source endpoint in a table	|
|   Amazon SQS	|   On the target endpoint in a .fifo queue	|

This drives how the migration tool behaves. Let's consider that an endpoint called Sales, containing timeouts that are to be consumed by endpoints Invoicing and Reporting.
- For RabbitMQ, all the delayed messages from Sales, independent of their destination, will be migrated into the shared delayed delivery infrastructure that's used by the RabbitMQ transport.
- For Azure Storage Queues, all the delayed messages from Sales will be migrated directly to the delayed delivery Azure Table from the Sales endpoint. They are kept there until a polling mechanism picks them up as expired and sends them to the appropriate destination.
- For SQL Server transport, all the delayed messages from Sales, will be migrated into the Sales endpoint. They are kept there until a polling mechanism picks them up as expired and sends them to the appropriate destination.
- For Amazon SQS, the delayed messages for Invoicing will be migrated directly to the invoicing.fifo queue, and the delayed messages for Reporting to reporting.fifo.


## How to Test Locally

### For the source

Run a RavenDb Docker container for v4:

`docker run -d -p 8080:8080 -p 38888:38888 ravendb/ravendb:4.2-ubuntu-latest`

Run a local RavenDb server instance for v3.5 on port 8383.

### For the target

Run a SQL Server Docker container:

`docker run --name SqlServer -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=some-password" -p 1433:1433 -d  mcr.microsoft.com/mssql/server:2019-latest`

Run a RabbitMQ Docker container:

`docker run -d --hostname my-rabbit -p 5672:5672 -p 15672:15672  rabbitmq:3-management`

Run an Oracle Docker container:

`docker run -d -it --name oracle -p 1521:1521 store/oracle/database-enterprise:12.2.0.1`

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

These tests were run on development machines, with both the storage and the broker running on the same machine, so although these results don't represent a functional production environment, they provide a rough estimate of migration duration times.

### Raven 3

* For 1 million timeouts, using the index:
   * Listing endpoints: 8 minutes
   * Prepare: 22 minutes
   * Migration: 31 minutes
   * Total migration time: 1 hour and 1 minute

* For 100K timeouts, not using the index:
   * Listing endpoints: 2 minutes
   * Prepare: 2 minutes
   * Migration: 2 minutes
   * Total migration time: 6 minutes

* For 300K timeouts, not using the index:
   * Listing endpoints: 10 minutes
   * Prepare: 19 minutes
   * Migration: 8 minutes
   * Total migration time: 37 minutes

### Raven 4

* For 1 million timeouts, using the index:
   * Listing endpoints: 2 minutes
   * Prepare: 4 minutes
   * Migration: 30 minutes
   * Total migration time: 36 minutes

* For 1 million timeouts, not using the index:
   * Listing endpoints: 5 minutes
   * Prepare: 14 minutes
   * Migration: 30 minutes
   * Total migration time: 49 minutes

### Sql P

* For 1 million timeouts
   * Listing endpoints: around 10 seconds
   * Prepare:7 minutes
   * Migration: 30 minutes
   * Total: around 40-45 minutes
