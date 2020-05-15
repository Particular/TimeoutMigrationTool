# TimeoutMigrationTool

## How to Test Locally

Run a RavenDb Docker container:

`docker run -d -p 8080:8080 -p 38888:38888 ravendb/ravendb`

Run a RabbitMQ Docker container:

`docker run -d --hostname my-rabbit --name my-rabbit -p 5672:5672 -p 15672:15672  rabbitmq:3-management`