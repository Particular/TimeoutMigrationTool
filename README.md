# TimeoutMigrationTool

## How to Test Locally

Run a RavenDb Docker container for v4:

`docker run -d -p 8080:8080 -p 38888:38888 ravendb/ravendb`

Run a local RavenDb server instance for v3.5 on port 8383.

Run a RabbitMQ Docker container:

`docker run -d --hostname my-rabbit --name my-rabbit -p 5672:5672 -p 15672:15672  rabbitmq:3-management`