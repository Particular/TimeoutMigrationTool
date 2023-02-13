namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Asp;
    using ASQ;
    using McMaster.Extensions.CommandLineUtils;
    using Microsoft.Extensions.Logging;
    using Msmq;
    using NHibernate;
    using NoOp;
    using RabbitMq;
    using RavenDB;
    using SqlP;
    using SqlT;

    // usage:
    //  migrate-timeouts preview ravendb|sqlp|nhb|asp --src-specific-options rabbitmq|sqlt --target-specific-options  [--cutoff-time] [--endpoint-filter]
    //  migrate-timeouts migrate ravendb|sqlp|nhb|asp --src-specific-options rabbitmq|sqlt --target-specific-options  [--cutoff-time] [--endpoint-filter]
    //  migrate-timeouts migrate asp --endpoint Asp.FakeTimeouts --timeoutTableName TimeoutData --partitionKeyScope yyyy-MM-dd --source "UseDevelopmentStorage=true" rabbitmq --target amqp://guest:guest@localhost:5672
    //  migrate-timeouts abort ravendb|sqlp|nhb|asp --src-specific-options rabbitmq|sqlt --target-specific-options  [--cutoff-time] [--endpoint-filter]
    //  abort could also be
    //  migrate-timeouts abort ravendb|sqlp|nhb|asp --src-specific-options rabbitmq|sqlt --target-specific-options [--cutoff-time] [--endpoint-filter]

    // Examples:
    //  migrate-timeouts preview ravendb --serverUrl http://localhost:8080 --databaseName raven-timeout-test --prefix TimeoutDatas --ravenVersion 4 rabbitmq --target amqp://guest:guest@localhost:5672
    //  migrate-timeouts preview sqlp --source \"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=MyTestDB;Integrated Security=True;\" --dialect MsSqlServer rabbitmq --target amqp://guest:guest@localhost:5672
    //  migrate-timeouts preview nhb --source \"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=MyTestDB;Integrated Security=True;\" --dialect MsSqlDatabaseDialect rabbitmq --target amqp://guest:guest@localhost:5672
    //  migrate-timeouts preview asp --endpoint Asp.FakeTimeouts --timeoutTableName TimeoutData --partitionKeyScope yyyy-MM-dd --containerName containerName --source "UseDevelopmentStorage=true" rabbitmq --target amqp://guest:guest@localhost:5672
    class Program
    {
        static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += UnhandledAppdomainExceptionHandler;

            var app = new CommandLineApplication
            {
                Name = "migrate-timeouts"
            };

            var verboseOption = app.Option("-v|--verbose", "Show verbose output", CommandOptionType.NoValue, true);
            var cutoffTimeOption =
                new CommandOption($"-c|--{ApplicationOptions.CutoffTime}", CommandOptionType.SingleValue)
                {
                    Description = "The cut off time to apply when finding eligible timeouts"
                };

            var endpointFilterOption =
                new CommandOption($"--{ApplicationOptions.EndpointFilter}", CommandOptionType.SingleValue)
                {
                    Description =
                        $"The endpoint to migrate timeouts for, use --{ApplicationOptions.AllEndpoints} to include all endpoints"
                };

            var allEndpointsOption =
                new CommandOption($"--{ApplicationOptions.AllEndpoints}", CommandOptionType.NoValue)
                {
                    Description = "Option to include migrate timeouts for all found endpoints in the database"
                };

            var sourceSqlPConnectionString = new CommandOption($"-s|--{ApplicationOptions.SqlSourceConnectionString}",
                CommandOptionType.SingleValue)
            {
                Description = "Connection string for source storage",
                Inherited = true
            };

            var sourceSqlPDialect =
                new CommandOption($"-d|--{ApplicationOptions.SqlSourceDialect}", CommandOptionType.SingleValue)
                {
                    Description = "The sql dialect to use",
                    Inherited = true
                };

            var sourceNHibernateConnectionString = new CommandOption($"--{ApplicationOptions.NHibernateSourceConnectionString}",
                CommandOptionType.SingleValue)
            {
                Description = "Connection string for NHibernate source storage",
                Inherited = true
            };

            var sourceNHibernateDialect =
                new CommandOption($"--{ApplicationOptions.NHibernateSourceDialect}", CommandOptionType.SingleValue)
                {
                    Description = "The sql dialect to use with NHibernate source",
                    Inherited = true
                };

            var sourceRavenDbServerUrlOption =
                new CommandOption($"--{ApplicationOptions.RavenServerUrl}", CommandOptionType.SingleValue)
                {
                    Description = "The url to the ravendb server",
                    Inherited = true
                };

            var sourceRavenDbDatabaseNameOption = new CommandOption($"--{ApplicationOptions.RavenDatabaseName}",
                CommandOptionType.SingleValue)
            {
                Description = "The name of the ravendb database",
                Inherited = true
            };

            var sourceRavenDbPrefixOption =
                new CommandOption($"--{ApplicationOptions.RavenTimeoutPrefix}", CommandOptionType.SingleValue)
                {
                    Description = "The prefix used for the document collection containing the timeouts",
                    Inherited = true
                };

            var sourceRavenDbVersion =
                new CommandOption($"--{ApplicationOptions.RavenVersion}", CommandOptionType.SingleValue)
                {
                    Description = "The version of RavenDB being used in your environment. Only 3.5 and 4 are supported",
                    Inherited = true
                };

            var sourceRavenDbForceUseIndexOption =
                new CommandOption($"--{ApplicationOptions.ForceUseIndex}", CommandOptionType.NoValue)
                {
                    Description =
                        "Force the usage of an index to boost performance. Can only be used when endpoints are shut down.",
                    Inherited = true
                };

            var sourceAspConnectionString = new CommandOption($"-s|--{ApplicationOptions.AspSourceConnectionString}",
                CommandOptionType.SingleValue)
            {
                Description = "Connection string for the Azure Storage Persistence",
                Inherited = true
            };

            var sourceAspContainerName = new CommandOption($"-c|--{ApplicationOptions.AspSourceContainerName}",
                CommandOptionType.SingleValue)
            {
                Description = "The container name to be used to download timeout data from",
                Inherited = true
            };

            var sourceAspPartitionKeyScope = new CommandOption($"-p|--{ApplicationOptions.AspSourcePartitionKeyScope}",
                CommandOptionType.SingleValue)
            {
                Description = "The partition key scope format to be used. Must follow the pattern of starting with year, month and day.",
                Inherited = true
            };

            var sourceAspTimeoutTableName = new CommandOption($"--{ApplicationOptions.AspTimeoutTableName}",
                CommandOptionType.SingleValue)
            {
                Description = "The timeout table name to migrate timeouts from",
                Inherited = true
            };

            // Target parameters

            var targetRabbitConnectionString =
                new CommandOption($"-t|--{ApplicationOptions.RabbitMqTargetConnectionString}",
                    CommandOptionType.SingleValue)
                {
                    Description = "The connection string for the target transport"
                };

            var targetRabbitUseV1DelayInfrastructure =
                new CommandOption($"--{ApplicationOptions.UseRabbitDelayInfrastructureVersion1}",
                    CommandOptionType.NoValue)
                {
                    Description = "Use Version 1 of the RabbitMQ Delay Infrastructure",
                };

            var targetSqlTConnectionString =
                new CommandOption($"-t|--{ApplicationOptions.SqlTTargetConnectionString}",
                    CommandOptionType.SingleValue)
                {
                    Description = "The connection string for the SQL Server transport"
                };

            var targetSqlTSchemaName =
                new CommandOption($"-s|--{ApplicationOptions.SqlTTargetSchema}",
                    CommandOptionType.SingleValue)
                {
                    Description = "The default schema used for the SQL Server transport",
                };

            var targetMsmqSqlSchemaName =
                new CommandOption($"-s|--{ApplicationOptions.MsmqSqlTargetSchema}",
                    CommandOptionType.SingleValue)
                {
                    Description = "The default schema used for the SQL Server instance with MSMQ timeout data",
                };

            var targetMsmqSqlConnectionString =
                new CommandOption($"-t|--{ApplicationOptions.MsmqSqlTargetConnectionString}",
                    CommandOptionType.SingleValue)
                {
                    Description = "The connection string for the SQL Server instance with MSMQ timeout data"
                };

            var targetAsqConnectionString =
                new CommandOption($"-t|--{ApplicationOptions.AsqTargetConnectionString}",
                    CommandOptionType.SingleValue)
                {
                    Description = "The connection string to the table storage"
                };

            var targetAsqDelayedDeliveryTableName =
                new CommandOption($"-t|--{ApplicationOptions.AsqDelayedDeliveryTableName}",
                    CommandOptionType.SingleValue)
                {
                    Description = "The name of the delayed delivery table for ASQ. Only necessary if the default was overidden in the endpoint configuration."
                };

            var runParameters = new Dictionary<string, string>();

            var batchSize = 1024;
            app.Command("preview", previewCommand =>
            {
                previewCommand.OnExecute(() =>
                {
                    Console.WriteLine("Specify a source with the required options.");
                    previewCommand.ShowHelp();
                    return 1;
                });

                previewCommand.Description = "Lists endpoints that can be migrated.";

                previewCommand.Command("ravendb", ravenDbCommand =>
                {
                    ravenDbCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        ravenDbCommand.ShowHelp();
                        return 1;
                    });

                    ravenDbCommand.AddOption(sourceRavenDbServerUrlOption);
                    ravenDbCommand.AddOption(sourceRavenDbDatabaseNameOption);
                    ravenDbCommand.AddOption(sourceRavenDbPrefixOption);
                    ravenDbCommand.AddOption(sourceRavenDbVersion);
                    ravenDbCommand.AddOption(sourceRavenDbForceUseIndexOption);

                    ravenDbCommand.Command("rabbitmq", ravenToRabbitCommand =>
                    {
                        ravenToRabbitCommand.AddOption(targetRabbitConnectionString);
                        ravenToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        ravenToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix,
                                ravenVersion, forceUseIndex);
                            var transportAdapter = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);
                            var runner = new PreviewRunner(logger, timeoutStorage, transportAdapter);

                            await runner.Run();
                        });
                    });

                    ravenDbCommand.Command("sqlt", ravenDbToSqlTCommand =>
                    {
                        ravenDbToSqlTCommand.AddOption(targetSqlTConnectionString);
                        ravenDbToSqlTCommand.AddOption(targetSqlTSchemaName);

                        ravenDbToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix, ravenVersion, forceUseIndex);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema ?? "dbo");

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    ravenDbCommand.Command("msmq", ravenDbToMsmqCommand =>
                    {
                        ravenDbToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        ravenDbToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        ravenDbToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix, ravenVersion, forceUseIndex);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema ?? "dbo");

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    ravenDbCommand.Command("asq", ravenDbToAsqCommand =>
                    {
                        ravenDbToAsqCommand.AddOption(targetAsqConnectionString);
                        ravenDbToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        ravenDbToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix, ravenVersion, forceUseIndex);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });
                });

                previewCommand.Command("sqlp", sqlpCommand =>
                {
                    sqlpCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        sqlpCommand.ShowHelp();
                        return 1;
                    });

                    sourceSqlPDialect.Validators.Add(new SqlDialectValidator());

                    sqlpCommand.AddOption(sourceSqlPConnectionString);
                    sqlpCommand.AddOption(sourceSqlPDialect);

                    sqlpCommand.Command("rabbitmq", sqlPToRabbitCommand =>
                    {
                        sqlPToRabbitCommand.AddOption(targetRabbitConnectionString);
                        sqlPToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        sqlPToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, batchSize);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    sqlpCommand.Command("sqlt", sqlpToSqlTCommand =>
                    {
                        sqlpToSqlTCommand.AddOption(targetSqlTConnectionString);
                        sqlpToSqlTCommand.AddOption(targetSqlTSchemaName);

                        sqlpToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, 5 * batchSize);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema ?? "dbo");

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    sqlpCommand.Command("msmq", sqlpToMsmqCommand =>
                    {
                        sqlpToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        sqlpToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        sqlpToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, 5 * batchSize);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema ?? "dbo");

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    sqlpCommand.Command("asq", sqlpToAsqCommand =>
                    {
                        sqlpToAsqCommand.AddOption(targetAsqConnectionString);
                        sqlpToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        sqlpToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, 1024);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });
                });

                previewCommand.Command("nhb", nHibernateCommand =>
                {
                    nHibernateCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        nHibernateCommand.ShowHelp();
                        return 1;
                    });

                    sourceNHibernateDialect.Validators.Add(new NHibernateDialectValidator());

                    nHibernateCommand.AddOption(sourceNHibernateConnectionString);
                    nHibernateCommand.AddOption(sourceNHibernateDialect);

                    nHibernateCommand.Command("rabbitmq", nHibernateToRabbitCommand =>
                    {
                        nHibernateToRabbitCommand.AddOption(targetRabbitConnectionString);
                        nHibernateToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        nHibernateToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    nHibernateCommand.Command("sqlt", nHibernateToSqlTCommand =>
                    {
                        nHibernateToSqlTCommand.AddOption(targetSqlTConnectionString);
                        nHibernateToSqlTCommand.AddOption(targetSqlTSchemaName);

                        nHibernateToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema ?? "dbo");

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    nHibernateCommand.Command("msmq", nHibernateToMsmqCommand =>
                    {
                        nHibernateToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        nHibernateToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        nHibernateToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema ?? "dbo");

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    nHibernateCommand.Command("asq", nHibernateToAsqCommand =>
                    {
                        nHibernateToAsqCommand.AddOption(targetAsqConnectionString);
                        nHibernateToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        nHibernateToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, 1024, dialect);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });
                });

                previewCommand.Command("asp", aspCommand =>
                {
                    aspCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        aspCommand.ShowHelp();
                        return 1;
                    });

                    aspCommand.AddOption(endpointFilterOption);
                    aspCommand.AddOption(sourceAspConnectionString);
                    aspCommand.AddOption(sourceAspContainerName);
                    aspCommand.AddOption(sourceAspPartitionKeyScope);
                    aspCommand.AddOption(sourceAspTimeoutTableName);

                    aspCommand.Command("rabbitmq", aspToRabbitCommand =>
                    {
                        aspToRabbitCommand.AddOption(targetRabbitConnectionString);
                        aspToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        aspToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var endpointName = endpointFilterOption.Value();

                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    aspCommand.Command("sqlt", aspToSqlTCommand =>
                    {
                        aspToSqlTCommand.AddOption(targetSqlTConnectionString);
                        aspToSqlTCommand.AddOption(targetSqlTSchemaName);

                        aspToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var timeoutsTarget =
                                new SqlTTimeoutsTarget(logger, targetConnectionString, schema ?? "dbo");

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    aspCommand.Command("msmq", aspToMsmqCommand =>
                    {
                        aspToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        aspToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        aspToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema ?? "dbo");
                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    aspCommand.Command("asq", aspToAsqCommand =>
                    {
                        aspToAsqCommand.AddOption(targetAsqConnectionString);
                        aspToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        aspToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var endpointName = endpointFilterOption.Value();

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var runner = new PreviewRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });
                });
            });

            app.Command("migrate", migrateCommand =>
            {
                migrateCommand.OnExecute(() =>
                {
                    Console.WriteLine("Specify a source with the required options.");
                    migrateCommand.ShowHelp();
                    return 1;
                });

                migrateCommand.Description = "Performs migration of selected endpoint(s).";

                migrateCommand.AddOption(allEndpointsOption);
                migrateCommand.AddOption(endpointFilterOption);
                migrateCommand.AddOption(cutoffTimeOption);

                migrateCommand.Command("ravendb", ravenDbCommand =>
                {
                    ravenDbCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        ravenDbCommand.ShowHelp();
                        return 1;
                    });

                    ravenDbCommand.AddOption(sourceRavenDbServerUrlOption);
                    ravenDbCommand.AddOption(sourceRavenDbDatabaseNameOption);
                    ravenDbCommand.AddOption(sourceRavenDbPrefixOption);
                    ravenDbCommand.AddOption(sourceRavenDbVersion);

                    ravenDbCommand.Command("rabbitmq", ravenDbToRabbitMqCommand =>
                    {
                        ravenDbToRabbitMqCommand.AddOption(targetRabbitConnectionString);
                        ravenDbToRabbitMqCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        ravenDbToRabbitMqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString,
                                targetConnectionString);

                            runParameters.Add(ApplicationOptions.RavenServerUrl, serverUrl);
                            runParameters.Add(ApplicationOptions.RavenDatabaseName, databaseName);
                            runParameters.Add(ApplicationOptions.RavenTimeoutPrefix, prefix);
                            runParameters.Add(ApplicationOptions.RavenVersion, ravenVersion.ToString());

                            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix, ravenVersion, forceUseIndex);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    ravenDbCommand.Command("sqlt", ravenDbPToSqlTCommand =>
                    {
                        ravenDbPToSqlTCommand.AddOption(targetSqlTConnectionString);
                        ravenDbPToSqlTCommand.AddOption(targetSqlTSchemaName);

                        ravenDbPToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            runParameters.Add(ApplicationOptions.RavenServerUrl, serverUrl);
                            runParameters.Add(ApplicationOptions.RavenDatabaseName, databaseName);
                            runParameters.Add(ApplicationOptions.RavenTimeoutPrefix, prefix);
                            runParameters.Add(ApplicationOptions.RavenVersion, ravenVersion.ToString());

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.SqlTTargetConnectionString, targetConnectionString);

                            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix, ravenVersion, forceUseIndex);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema ?? "dbo");

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    ravenDbCommand.Command("asq", ravenDbToAsqCommand =>
                    {
                        ravenDbToAsqCommand.AddOption(targetAsqConnectionString);
                        ravenDbToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        ravenDbToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            runParameters.Add(ApplicationOptions.RavenServerUrl, serverUrl);
                            runParameters.Add(ApplicationOptions.RavenDatabaseName, databaseName);
                            runParameters.Add(ApplicationOptions.RavenTimeoutPrefix, prefix);
                            runParameters.Add(ApplicationOptions.RavenVersion, ravenVersion.ToString());

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            if (targetAsqDelayedDeliveryTableName.HasValue() && allEndpointsOption.HasValue())
                            {
                                Console.WriteLine("It is not possible to override the delayed delivery table name and migrate all endpoints");
                                return;
                            }

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.AsqTargetConnectionString, targetConnectionString);
                            runParameters.Add(ApplicationOptions.AsqDelayedDeliveryTableName, delayedDeliveryTableNameOverride);

                            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix, ravenVersion, forceUseIndex);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource, timeoutsTarget);
                        });
                    });

                    ravenDbCommand.Command("msmq", ravenDbToMsmqCommand =>
                    {
                        ravenDbToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        ravenDbToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        ravenDbToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.RavenServerUrl, serverUrl);
                            runParameters.Add(ApplicationOptions.RavenDatabaseName, databaseName);
                            runParameters.Add(ApplicationOptions.RavenTimeoutPrefix, prefix);
                            runParameters.Add(ApplicationOptions.RavenVersion, ravenVersion.ToString());

                            runParameters.Add(ApplicationOptions.MsmqSqlTargetConnectionString, targetConnectionString);
                            runParameters.Add(ApplicationOptions.MsmqSqlTargetSchema, schema);

                            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix, ravenVersion, forceUseIndex);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema ?? "dbo");

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    ravenDbCommand.Command("noop", ravenDbCommandToNoopCommand =>
                    {
                        ravenDbCommandToNoopCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            runParameters.Add(ApplicationOptions.RavenServerUrl, serverUrl);
                            runParameters.Add(ApplicationOptions.RavenDatabaseName, databaseName);
                            runParameters.Add(ApplicationOptions.RavenTimeoutPrefix, prefix);
                            runParameters.Add(ApplicationOptions.RavenVersion, ravenVersion.ToString());

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix, ravenVersion, forceUseIndex);
                            var timeoutsTarget = new NoOpTarget();

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });
                });

                migrateCommand.Command("sqlp", sqlpCommand =>
                {
                    sqlpCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        sqlpCommand.ShowHelp();
                        return 1;
                    });

                    sourceSqlPDialect.Validators.Add(new SqlDialectValidator());

                    sqlpCommand.AddOption(sourceSqlPConnectionString);
                    sqlpCommand.AddOption(sourceSqlPDialect);

                    sqlpCommand.Command("rabbitmq", sqlPToRabbitCommand =>
                    {
                        sqlPToRabbitCommand.AddOption(targetRabbitConnectionString);
                        sqlPToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        sqlPToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());
                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceSqlPDialect.Value());

                            runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString, targetConnectionString);

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, batchSize);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    sqlpCommand.Command("sqlt", sqlPToSqlTCommand =>
                    {
                        sqlPToSqlTCommand.AddOption(targetSqlTConnectionString);
                        sqlPToSqlTCommand.AddOption(targetSqlTSchemaName);

                        sqlPToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());
                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceSqlPDialect.Value());

                            runParameters.Add(ApplicationOptions.SqlTTargetConnectionString,
                                targetConnectionString);

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, 5 * batchSize);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema ?? "dbo");

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    sqlpCommand.Command("asq", sqlPToAsqCommand =>
                    {
                        sqlPToAsqCommand.AddOption(targetAsqConnectionString);
                        sqlPToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        sqlPToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());
                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            if (targetAsqDelayedDeliveryTableName.HasValue() && allEndpointsOption.HasValue())
                            {
                                Console.WriteLine("It is not possible to override the delayed delivery table name and migrate all endpoints");
                                return;
                            }

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceSqlPDialect.Value());
                            runParameters.Add(ApplicationOptions.AsqTargetConnectionString, targetConnectionString);
                            runParameters.Add(ApplicationOptions.AsqDelayedDeliveryTableName, delayedDeliveryTableNameOverride);

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, 1024);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    sqlpCommand.Command("msmq", sqlpToMsmqCommand =>
                    {
                        sqlpToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        sqlpToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        sqlpToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());
                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceSqlPDialect.Value());

                            runParameters.Add(ApplicationOptions.MsmqSqlTargetConnectionString, targetConnectionString);
                            runParameters.Add(ApplicationOptions.MsmqSqlTargetSchema, schema);

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, 5 * batchSize);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema ?? "dbo");

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    sqlpCommand.Command("noop", sqlpCommandToNoopCommand =>
                    {
                        sqlpCommandToNoopCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());
                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceSqlPDialect.Value());

                            var timeoutsSource = new SqlTimeoutsSource(sourceConnectionString, dialect, 5 * batchSize);
                            var timeoutsTarget = new NoOpTarget();

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });
                });

                migrateCommand.Command("nhb", nHibernateCommand =>
                {
                    nHibernateCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        nHibernateCommand.ShowHelp();
                        return 1;
                    });

                    sourceNHibernateDialect.Validators.Add(new NHibernateDialectValidator());

                    nHibernateCommand.AddOption(sourceNHibernateConnectionString);
                    nHibernateCommand.AddOption(sourceNHibernateDialect);

                    nHibernateCommand.Command("rabbitmq", nHibernateToRabbitCommand =>
                    {
                        nHibernateToRabbitCommand.AddOption(targetRabbitConnectionString);
                        nHibernateToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        nHibernateToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());
                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.NHibernateSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.NHibernateSourceDialect, sourceNHibernateDialect.Value());

                            runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString, targetConnectionString);

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    nHibernateCommand.Command("sqlt", nHibernateToSqlTCommand =>
                    {
                        nHibernateToSqlTCommand.AddOption(targetSqlTConnectionString);
                        nHibernateToSqlTCommand.AddOption(targetSqlTSchemaName);

                        nHibernateToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.NHibernateSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.NHibernateSourceDialect, sourceNHibernateDialect.Value());

                            runParameters.Add(ApplicationOptions.SqlTTargetConnectionString, targetConnectionString);

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema ?? "dbo");

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    nHibernateCommand.Command("asq", nHibernateToAsqCommand =>
                    {
                        nHibernateToAsqCommand.AddOption(targetAsqConnectionString);
                        nHibernateToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        nHibernateToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            if (targetAsqDelayedDeliveryTableName.HasValue() && allEndpointsOption.HasValue())
                            {
                                Console.WriteLine("It is not possible to override the delayed delivery table name and migrate all endpoints");
                                return;
                            }

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.NHibernateSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.NHibernateSourceDialect, sourceNHibernateDialect.Value());

                            runParameters.Add(ApplicationOptions.AsqTargetConnectionString, targetConnectionString);
                            runParameters.Add(ApplicationOptions.AsqDelayedDeliveryTableName, delayedDeliveryTableNameOverride);

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, 1024, dialect);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource, timeoutsTarget);
                        });
                    });

                    nHibernateCommand.Command("msmq", nHibernateToMsmqCommand =>
                    {
                        nHibernateToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        nHibernateToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        nHibernateToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.NHibernateSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.NHibernateSourceDialect, sourceNHibernateDialect.Value());

                            runParameters.Add(ApplicationOptions.MsmqSqlTargetConnectionString, targetConnectionString);
                            runParameters.Add(ApplicationOptions.MsmqSqlTargetSchema, schema);

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema ?? "dbo");

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    nHibernateCommand.Command("noop", nHibernateToNoopCommand =>
                    {
                        nHibernateToNoopCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());


                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.NHibernateSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.NHibernateSourceDialect, sourceNHibernateDialect.Value());

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new NoOpTarget();

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });
                });

                migrateCommand.Command("asp", aspCommand =>
                {
                    aspCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        aspCommand.ShowHelp();
                        return 1;
                    });

                    aspCommand.AddOption(endpointFilterOption);
                    aspCommand.AddOption(sourceAspConnectionString);
                    aspCommand.AddOption(sourceAspContainerName);
                    aspCommand.AddOption(sourceAspPartitionKeyScope);
                    aspCommand.AddOption(sourceAspTimeoutTableName);

                    aspCommand.Command("rabbitmq", aspToRabbitCommand =>
                    {
                        aspToRabbitCommand.AddOption(targetRabbitConnectionString);
                        aspToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        aspToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();
                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.AspSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.AspSourceContainerName, sourceContainerName);
                            runParameters.Add(ApplicationOptions.AspSourcePartitionKeyScope, sourcePartitionKeyScope);
                            runParameters.Add(ApplicationOptions.AspTimeoutTableName, sourceTimeoutTableName);

                            runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString,
                                targetConnectionString);

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    aspCommand.Command("sqlt", aspToSqlTCommand =>
                    {
                        aspToSqlTCommand.AddOption(targetSqlTConnectionString);
                        aspToSqlTCommand.AddOption(targetSqlTSchemaName);

                        aspToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.AspSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.AspSourceContainerName, sourceContainerName);
                            runParameters.Add(ApplicationOptions.AspSourcePartitionKeyScope, sourcePartitionKeyScope);
                            runParameters.Add(ApplicationOptions.AspTimeoutTableName, sourceTimeoutTableName);

                            runParameters.Add(ApplicationOptions.SqlTTargetConnectionString, targetConnectionString);

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);

                            var timeoutsTarget =
                                new SqlTTimeoutsTarget(logger, targetConnectionString, schema ?? "dbo");

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    aspCommand.Command("asq", aspToAsqCommand =>
                    {
                        aspToAsqCommand.AddOption(targetAsqConnectionString);
                        aspToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        aspToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            if (targetAsqDelayedDeliveryTableName.HasValue() && allEndpointsOption.HasValue())
                            {
                                Console.WriteLine("It is not possible to override the delayed delivery table name and migrate all endpoints");
                                return;
                            }

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.AspSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.AspSourceContainerName, sourceContainerName);
                            runParameters.Add(ApplicationOptions.AspSourcePartitionKeyScope, sourcePartitionKeyScope);
                            runParameters.Add(ApplicationOptions.AspTimeoutTableName, sourceTimeoutTableName);

                            runParameters.Add(ApplicationOptions.AsqTargetConnectionString, targetConnectionString);
                            runParameters.Add(ApplicationOptions.AsqDelayedDeliveryTableName, delayedDeliveryTableNameOverride);

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource, timeoutsTarget);
                        });
                    });

                    aspCommand.Command("msmq", nHibernateToMsmqCommand =>
                    {
                        nHibernateToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        nHibernateToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        nHibernateToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.AspSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.AspSourceContainerName, sourceContainerName);
                            runParameters.Add(ApplicationOptions.AspSourcePartitionKeyScope, sourcePartitionKeyScope);
                            runParameters.Add(ApplicationOptions.AspTimeoutTableName, sourceTimeoutTableName);

                            runParameters.Add(ApplicationOptions.MsmqSqlTargetConnectionString, targetConnectionString);
                            runParameters.Add(ApplicationOptions.MsmqSqlTargetSchema, schema);

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);

                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema ?? "dbo");

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });

                    aspCommand.Command("noop", aspToNoopCommand =>
                    {
                        aspToNoopCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.AspSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.AspSourceContainerName, sourceContainerName);
                            runParameters.Add(ApplicationOptions.AspSourcePartitionKeyScope, sourcePartitionKeyScope);
                            runParameters.Add(ApplicationOptions.AspTimeoutTableName, sourceTimeoutTableName);

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new NoOpTarget();

                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutsSource,
                                timeoutsTarget);
                        });
                    });
                });
            });

            app.Command("abort", abortCommand =>
            {
                abortCommand.OnExecute(() =>
                {
                    Console.WriteLine("Specify a source with the required options.");
                    abortCommand.ShowHelp();
                    return 1;
                });

                abortCommand.Description = "Aborts currently ongoing migration and restores unmigrated timeouts.";

                abortCommand.Command("ravendb", ravenDbCommand =>
                {
                    ravenDbCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        ravenDbCommand.ShowHelp();
                        return 1;
                    });

                    ravenDbCommand.AddOption(sourceRavenDbServerUrlOption);
                    ravenDbCommand.AddOption(sourceRavenDbDatabaseNameOption);
                    ravenDbCommand.AddOption(sourceRavenDbPrefixOption);
                    ravenDbCommand.AddOption(sourceRavenDbVersion);

                    ravenDbCommand.Command("rabbitmq", ravenToRabbitCommand =>
                    {
                        ravenToRabbitCommand.AddOption(targetRabbitConnectionString);
                        ravenToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        ravenDbCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;

                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix,
                                ravenVersion, false);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    ravenDbCommand.Command("sqlt", ravenDbToSqlTCommand =>
                    {
                        ravenDbToSqlTCommand.AddOption(targetSqlTConnectionString);
                        ravenDbToSqlTCommand.AddOption(targetSqlTSchemaName);

                        ravenDbToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix,
                                ravenVersion, false);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema);

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    ravenDbCommand.Command("asq", ravenDbToAsqCommand =>
                    {
                        ravenDbToAsqCommand.AddOption(targetAsqConnectionString);
                        ravenDbToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        ravenDbToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix,
                                ravenVersion, false);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    ravenDbCommand.Command("msmq", ravenDbToMsmqCommand =>
                    {
                        ravenDbToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        ravenDbToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        ravenDbToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix,
                                ravenVersion, false);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema);

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    ravenDbCommand.Command("noop", ravenDbToNoopCommand =>
                    {
                        ravenDbToNoopCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;

                            var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix,
                                ravenVersion, false);
                            var timeoutsTarget = new NoOpTarget();

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });
                });

                abortCommand.Command("sqlp", sqlpCommand =>
                {
                    sqlpCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        sqlpCommand.ShowHelp();
                        return 1;
                    });

                    sourceSqlPDialect.Validators.Add(new SqlDialectValidator());

                    sqlpCommand.AddOption(sourceSqlPConnectionString);
                    sqlpCommand.AddOption(sourceSqlPDialect);

                    sqlpCommand.Command("rabbitmq", sqlPToRabbitCommand =>
                    {
                        sqlPToRabbitCommand.AddOption(targetRabbitConnectionString);
                        sqlPToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        sqlPToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var timeoutStorage = new SqlTimeoutsSource(sourceConnectionString, dialect, batchSize);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    sqlpCommand.Command("sqlt", sqlpToSqlTCommand =>
                    {
                        sqlpToSqlTCommand.AddOption(targetSqlTConnectionString);
                        sqlpToSqlTCommand.AddOption(targetSqlTSchemaName);

                        sqlpToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var timeoutStorage = new SqlTimeoutsSource(sourceConnectionString, dialect, 5 * batchSize);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema);

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    sqlpCommand.Command("asq", sqlpToAsqCommand =>
                    {
                        sqlpToAsqCommand.AddOption(targetAsqConnectionString);
                        sqlpToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        sqlpToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            var timeoutStorage = new SqlTimeoutsSource(sourceConnectionString, dialect, 1024);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    sqlpCommand.Command("msmq", sqlpToMsmqCommand =>
                    {
                        sqlpToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        sqlpToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        sqlpToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var timeoutStorage = new SqlTimeoutsSource(sourceConnectionString, dialect, 1024);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema);

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    sqlpCommand.Command("noop", sqlpToNoopTCommand =>
                    {
                        sqlpToNoopTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var timeoutStorage = new SqlTimeoutsSource(sourceConnectionString, dialect, 5 * batchSize);
                            var timeoutsTarget = new NoOpTarget();

                            var runner = new AbortRunner(logger, timeoutStorage, timeoutsTarget);
                            await runner.Run();
                        });
                    });
                });

                abortCommand.Command("nhb", nhbCommand =>
                {
                    nhbCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        nhbCommand.ShowHelp();
                        return 1;
                    });

                    sourceSqlPDialect.Validators.Add(new SqlDialectValidator());

                    nhbCommand.AddOption(sourceNHibernateConnectionString);
                    nhbCommand.AddOption(sourceNHibernateDialect);

                    nhbCommand.Command("rabbitmq", nhbToRabbitCommand =>
                    {
                        nhbToRabbitCommand.AddOption(targetRabbitConnectionString);
                        nhbToRabbitCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        nhbToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    nhbCommand.Command("sqlt", nHibernateToSqlTCommand =>
                    {
                        nHibernateToSqlTCommand.AddOption(targetSqlTConnectionString);
                        nHibernateToSqlTCommand.AddOption(targetSqlTSchemaName);

                        nHibernateToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema);

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    nhbCommand.Command("asq", nHibernateToAsqCommand =>
                    {
                        nHibernateToAsqCommand.AddOption(targetAsqConnectionString);
                        nHibernateToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        nHibernateToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, 1024, dialect);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    nhbCommand.Command("msmq", nhibernateToMsmqCommand =>
                    {
                        nhibernateToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        nhibernateToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        nhibernateToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, 1024, dialect);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema);

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    nhbCommand.Command("noop", nHibernateToNoopCommand =>
                    {
                        nHibernateToNoopCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var timeoutsSource = new NHibernateTimeoutsSource(sourceConnectionString, batchSize, dialect);
                            var timeoutsTarget = new NoOpTarget();

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });
                });

                abortCommand.Command("asp", aspCommand =>
                {
                    aspCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a target with the required options.");
                        aspCommand.ShowHelp();
                        return 1;
                    });

                    aspCommand.AddOption(endpointFilterOption);
                    aspCommand.AddOption(sourceAspConnectionString);
                    aspCommand.AddOption(sourceAspContainerName);
                    aspCommand.AddOption(sourceAspPartitionKeyScope);
                    aspCommand.AddOption(sourceAspTimeoutTableName);

                    aspCommand.Command("rabbitmq", aspToRabbitMqCommand =>
                    {
                        aspToRabbitMqCommand.AddOption(targetRabbitConnectionString);
                        aspToRabbitMqCommand.AddOption(targetRabbitUseV1DelayInfrastructure);

                        aspToRabbitMqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var targetUseVersion1DelayInfrastructure = targetRabbitUseV1DelayInfrastructure.HasValue();

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new RabbitMqTimeoutTarget(logger, targetConnectionString, targetUseVersion1DelayInfrastructure);

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    aspCommand.Command("sqlt", aspToSqlTCommand =>
                    {
                        aspToSqlTCommand.AddOption(targetSqlTConnectionString);

                        aspToSqlTCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var targetConnectionString = targetSqlTConnectionString.Value();
                            var schema = targetSqlTSchemaName.Value();

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new SqlTTimeoutsTarget(logger, targetConnectionString, schema);

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    aspCommand.Command("asq", aspToAsqCommand =>
                    {
                        aspToAsqCommand.AddOption(targetAsqConnectionString);
                        aspToAsqCommand.AddOption(targetAsqDelayedDeliveryTableName);

                        aspToAsqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var targetConnectionString = targetAsqConnectionString.Value();
                            var delayedDeliveryTableNameOverride = targetAsqDelayedDeliveryTableName.HasValue() ? targetAsqDelayedDeliveryTableName.Value() : null;

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new ASQTarget(targetConnectionString, new DelayedDeliveryTableNameProvider(delayedDeliveryTableNameOverride));

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    aspCommand.Command("msmq", aspToMsmqCommand =>
                    {
                        aspToMsmqCommand.AddOption(targetMsmqSqlConnectionString);
                        aspToMsmqCommand.AddOption(targetMsmqSqlSchemaName);

                        aspToMsmqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var targetConnectionString = targetMsmqSqlConnectionString.Value();
                            var schema = targetMsmqSqlSchemaName.Value();

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new MsmqTarget(logger, targetConnectionString, schema);

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });

                    aspCommand.Command("noop", aspToNoopCommand =>
                    {
                        aspToNoopCommand.AddOption(targetSqlTConnectionString);

                        aspToNoopCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceAspConnectionString.Value();
                            var sourceContainerName = sourceAspContainerName.Value();
                            var sourcePartitionKeyScope = sourceAspPartitionKeyScope.Value();
                            var sourceTimeoutTableName = sourceAspTimeoutTableName.Value();

                            var endpointName = endpointFilterOption.Value();

                            var timeoutsSource = new AspTimeoutsSource(sourceConnectionString, batchSize,
                                sourceContainerName ?? "timeoutstate", endpointName, sourceTimeoutTableName,
                                partitionKeyScope: sourcePartitionKeyScope ?? AspConstants.PartitionKeyScope);
                            var timeoutsTarget = new NoOpTarget();

                            var runner = new AbortRunner(logger, timeoutsSource, timeoutsTarget);
                            await runner.Run();
                        });
                    });
                });
            });

            app.HelpOption(true);

            app.OnExecute(() =>
            {
                Console.WriteLine("Specify a subcommand");
                app.ShowHelp();
                return 1;
            });

            return app.Execute(args);
        }

        static DateTime? GetCutoffTime(CommandOption cutoffTimeOption)
        {
            if (!cutoffTimeOption.HasValue())
            {
                return null;
            }

            if (DateTime.TryParse(cutoffTimeOption.Value(), out var cutoffTime))
            {
                return cutoffTime;
            }

            throw new ArgumentException(
                "Unable to parse the cutofftime, please supply the cutoffTime in the following format 'yyyy-MM-dd hh:mm:ss'");
        }


        static Task RunMigration(ILogger logger, EndpointFilter endpointFilter, DateTime? cutoffTime,
            Dictionary<string, string> runParameters, ITimeoutsSource timeoutsSource,
            ITimeoutsTarget transportTimeoutTargetCreator)
        {
            var migrationRunner = new MigrationRunner(logger, timeoutsSource, transportTimeoutTargetCreator);

            if (cutoffTime.HasValue)
            {
                runParameters.Add(ApplicationOptions.CutoffTime, cutoffTime.Value.ToShortTimeString());
                logger.LogDebug($"Cutoff time: {cutoffTime.Value}");
            }
            else
            {
                cutoffTime = DateTime.Parse("2012-01-01");
            }


            return migrationRunner.Run(cutoffTime.Value, endpointFilter, runParameters);
        }

        static EndpointFilter ParseEndpointFilter(CommandOption allEndpointsOption,
            CommandOption endpointFilterOption)
        {
            if (allEndpointsOption.HasValue())
            {
                return EndpointFilter.IncludeAll;
            }

            if (!endpointFilterOption.HasValue())
            {
                throw new ArgumentException(
                    $"Either specify a specific endpoint using --{ApplicationOptions.EndpointFilter} or use the --{ApplicationOptions.AllEndpoints} option");
            }

            return EndpointFilter.SpecificEndpoint(endpointFilterOption.Value());
        }

        static void UnhandledAppdomainExceptionHandler(object sender, UnhandledExceptionEventArgs args)
        {
            var exception = (Exception)args.ExceptionObject;
            Console.WriteLine("Unhandled appdomain exception: " + exception);
            Console.WriteLine("Runtime terminating: {0}", args.IsTerminating);
        }
    }
}