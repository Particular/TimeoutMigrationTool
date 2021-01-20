using Particular.TimeoutMigrationTool.NHibernate;

namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using McMaster.Extensions.CommandLineUtils;
    using Microsoft.Extensions.Logging;
    using RabbitMq;
    using RavenDB;
    using SqlP;

    // usage:
    //  migrate-timeouts preview ravendb|sqlp --src-specific-options rabbitmq --target-specific-options  [--cutoff-time] [--endpoint-filter]
    //  migrate-timeouts migrate ravendb|sqlp --src-specific-options rabbitmq --target-specific-options  [--cutoff-time] [--endpoint-filter]
    //  migrate-timeouts abort ravendb|sqlp --src-specific-options rabbitmq --target-specific-options  [--cutoff-time] [--endpoint-filter]
    //  abort could also be
    //  migrate-timeouts abort ravendb|sqlp --src-specific-options  [--cutoff-time] [--endpoint-filter]

    // Examples:
    //  migrate-timeouts preview ravendb --serverUrl http://localhost:8080 --databaseName raven-timeout-test --prefix TimeoutDatas --ravenVersion 4 rabbitmq --target amqp://guest:guest@localhost:5672
    //  migrate-timeouts preview sqlp --source \"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=MyTestDB;Integrated Security=True;\" --dialect MsSqlServer rabbitmq --target amqp://guest:guest@localhost:5672
    internal class Program
    {
        private static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += UnhandledAppdomainExceptionHandler;

            var app = new CommandLineApplication
            {
                Name = "migrate-timeouts"
            };

            var verboseOption = app.Option("-v|--verbose", "Show verbose output", CommandOptionType.NoValue, true);
            var targetRabbitConnectionString =
                new CommandOption($"-t|--{ApplicationOptions.RabbitMqTargetConnectionString}",
                    CommandOptionType.SingleValue)
                {
                    Description = "The connection string for the target transport"
                };

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

            var runParameters = new Dictionary<string, string>();

            app.Command("preview", previewCommand =>
            {
                previewCommand.Description = "Lists endpoints that can be migrated.";

                previewCommand.Options.Add(targetRabbitConnectionString);

                previewCommand.Command("ravendb", ravenDBCommand =>
                {
                    ravenDBCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a subcommand");
                        ravenDBCommand.ShowHelp();
                        return 1;
                    });

                    ravenDBCommand.Options.Add(sourceRavenDbServerUrlOption);
                    ravenDBCommand.Options.Add(sourceRavenDbDatabaseNameOption);
                    ravenDBCommand.Options.Add(sourceRavenDbPrefixOption);
                    ravenDBCommand.Options.Add(sourceRavenDbVersion);
                    ravenDBCommand.Options.Add(sourceRavenDbForceUseIndexOption);

                    ravenDBCommand.Command("rabbitmq", ravenToRabbitCommand =>
                    {
                        ravenToRabbitCommand.Options.Add(targetRabbitConnectionString);

                        ravenToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var targetConnectionString = targetRabbitConnectionString.Value();
                            var ravenVersion = sourceRavenDbVersion.Value() == "3.5"
                                ? RavenDbVersion.ThreeDotFive
                                : RavenDbVersion.Four;
                            var forceUseIndex = sourceRavenDbForceUseIndexOption.HasValue();

                            var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix,
                                ravenVersion, forceUseIndex);
                            var transportAdapter = new RabbitMqTimeoutTarget(logger, targetConnectionString);
                            var runner = new PreviewRunner(logger, timeoutStorage, transportAdapter);

                            await runner.Run();
                        });
                    });
                });

                previewCommand.Command("sqlp", sqlpCommand =>
                {
                    sourceSqlPDialect.Validators.Add(new SqlDialectValidator());

                    sqlpCommand.Options.Add(sourceSqlPConnectionString);
                    sqlpCommand.Options.Add(sourceSqlPDialect);

                    sqlpCommand.Command("rabbitmq", sqlPToRabbitCommand =>
                    {
                        sqlPToRabbitCommand.Options.Add(targetRabbitConnectionString);

                        sqlPToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var targetConnectionString = targetRabbitConnectionString.Value();

                            var timeoutStorage = new SqlTimeoutsSource(sourceConnectionString, dialect, 1024);
                            var transportAdapter = new RabbitMqTimeoutTarget(logger, targetConnectionString);
                            var runner = new PreviewRunner(logger, timeoutStorage, transportAdapter);

                            await runner.Run();
                        });
                    });
                });

                previewCommand.Command("nhb", nHibernateCommand =>
                {
                    sourceNHibernateDialect.Validators.Add(new NHibernateDialectValidator());

                    nHibernateCommand.Options.Add(sourceNHibernateConnectionString);
                    nHibernateCommand.Options.Add(sourceNHibernateDialect);

                    nHibernateCommand.Command("rabbitmq", nHibernateToRabbitCommand =>
                    {
                        nHibernateToRabbitCommand.Options.Add(targetRabbitConnectionString);

                        nHibernateToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());

                            var targetConnectionString = targetRabbitConnectionString.Value();

                            var timeoutStorage = new NHibernateTimeoutSource(sourceConnectionString, 1024, dialect);
                            var transportAdapter = new RabbitMqTimeoutTarget(logger, targetConnectionString);
                            var runner = new PreviewRunner(logger, timeoutStorage, transportAdapter);

                            await runner.Run();
                        });
                    });
                });
            });

            app.Command("migrate", migrateCommand =>
            {
                migrateCommand.Description = "Performs migration of selected endpoint(s).";

                migrateCommand.Options.Add(targetRabbitConnectionString);

                migrateCommand.Options.Add(allEndpointsOption);
                migrateCommand.Options.Add(endpointFilterOption);
                migrateCommand.Options.Add(cutoffTimeOption);

                migrateCommand.Command("ravendb", ravenDBCommand =>
                {
                    ravenDBCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a subcommand");
                        ravenDBCommand.ShowHelp();
                        return 1;
                    });

                    ravenDBCommand.Options.Add(sourceRavenDbServerUrlOption);
                    ravenDBCommand.Options.Add(sourceRavenDbDatabaseNameOption);
                    ravenDBCommand.Options.Add(sourceRavenDbPrefixOption);
                    ravenDBCommand.Options.Add(sourceRavenDbVersion);

                    ravenDBCommand.Command("rabbitmq", ravenDbToRabbitMqCommand =>
                    {
                        ravenDbToRabbitMqCommand.Options.Add(targetRabbitConnectionString);

                        ravenDbToRabbitMqCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var serverUrl = sourceRavenDbServerUrlOption.Value();
                            var databaseName = sourceRavenDbDatabaseNameOption.Value();
                            var prefix = sourceRavenDbPrefixOption.Value();
                            var targetConnectionString = targetRabbitConnectionString.Value();
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

                            var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, prefix,
                                ravenVersion, forceUseIndex);

                            var transportAdapter = new RabbitMqTimeoutTarget(logger, targetConnectionString);
                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutStorage,
                                transportAdapter);
                        });
                    });
                });

                migrateCommand.Command("sqlp", sqlpCommand =>
                {
                    sourceSqlPDialect.Validators.Add(new SqlDialectValidator());

                    sqlpCommand.Options.Add(sourceSqlPConnectionString);
                    sqlpCommand.Options.Add(sourceSqlPDialect);

                    sqlpCommand.Command("rabbitmq", sqlPToRabbitCommand =>
                    {
                        sqlPToRabbitCommand.Options.Add(targetRabbitConnectionString);

                        sqlPToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());
                            var targetConnectionString = targetRabbitConnectionString.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceSqlPDialect.Value());

                            runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString,
                                targetConnectionString);

                            var timeoutStorage = new SqlTimeoutsSource(sourceConnectionString, dialect, 1024);

                            var transportAdapter = new RabbitMqTimeoutTarget(logger, targetConnectionString);
                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutStorage,
                                transportAdapter);
                        });
                    });
                });

                migrateCommand.Command("nhb", nHibernateCommand =>
                {
                    sourceNHibernateDialect.Validators.Add(new NHibernateDialectValidator());

                    nHibernateCommand.Options.Add(sourceNHibernateConnectionString);
                    nHibernateCommand.Options.Add(sourceNHibernateDialect);

                    nHibernateCommand.Command("rabbitmq", nHibernateToRabbitCommand =>
                    {
                        nHibernateToRabbitCommand.Options.Add(targetRabbitConnectionString);

                        nHibernateToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceNHibernateConnectionString.Value();
                            var dialect = DatabaseDialect.Parse(sourceNHibernateDialect.Value());
                            var targetConnectionString = targetRabbitConnectionString.Value();

                            var cutoffTime = GetCutoffTime(cutoffTimeOption);

                            runParameters.Add(ApplicationOptions.NHibernateSourceConnectionString, sourceConnectionString);
                            runParameters.Add(ApplicationOptions.NHibernateSourceDialect, sourceNHibernateDialect.Value());

                            runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString,
                                targetConnectionString);

                            var timeoutStorage = new NHibernateTimeoutSource(sourceConnectionString, 1024, dialect);

                            var transportAdapter = new RabbitMqTimeoutTarget(logger, targetConnectionString);
                            var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                            await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutStorage,
                                transportAdapter);
                        });
                    });
                });
            });

            app.Command("abort", abortCommand =>
            {
                // TODO: we might not need the target given that we don't use it, we just use the source to cleanup
                abortCommand.Description = "Aborts currently ongoing migration and restores unmigrated timeouts.";
                abortCommand.OnExecute(() =>
                {
                    Console.WriteLine("Specify a subcommand");
                    abortCommand.ShowHelp();
                    return 1;
                });

                abortCommand.Command("ravendb", ravenDBCommand =>
                {
                    ravenDBCommand.OnExecute(() =>
                    {
                        Console.WriteLine("Specify a subcommand");
                        ravenDBCommand.ShowHelp();
                        return 1;
                    });

                    ravenDBCommand.Options.Add(sourceRavenDbServerUrlOption);
                    ravenDBCommand.Options.Add(sourceRavenDbDatabaseNameOption);
                    ravenDBCommand.Options.Add(sourceRavenDbPrefixOption);
                    ravenDBCommand.Options.Add(sourceRavenDbVersion);

                    ravenDBCommand.Command("rabbitmq", ravenToRabbitCommand =>
                    {
                        ravenToRabbitCommand.Options.Add(targetRabbitConnectionString);
                        ravenDBCommand.OnExecuteAsync(async ct =>
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
                            var runner = new AbortRunner(logger, timeoutStorage);

                            await runner.Run();
                        });
                    });
                });

                abortCommand.Command("sqlp", sqlpCommand =>
                {
                    sourceSqlPDialect.Validators.Add(new SqlDialectValidator());

                    sqlpCommand.Options.Add(sourceSqlPConnectionString);
                    sqlpCommand.Options.Add(sourceSqlPDialect);

                    sqlpCommand.Command("rabbitmq", sqlPToRabbitCommand =>
                    {
                        sqlPToRabbitCommand.Options.Add(targetRabbitConnectionString);

                        sqlPToRabbitCommand.OnExecuteAsync(async ct =>
                        {
                            var logger = new ConsoleLogger(verboseOption.HasValue());

                            var sourceConnectionString = sourceSqlPConnectionString.Value();
                            var dialect = SqlDialect.Parse(sourceSqlPDialect.Value());

                            var timeoutStorage = new SqlTimeoutsSource(sourceConnectionString, dialect, 1024);
                            var runner = new AbortRunner(logger, timeoutStorage);

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

        private static DateTime? GetCutoffTime(CommandOption cutoffTimeOption)
        {
            if (!cutoffTimeOption.HasValue()) return null;

            if (DateTime.TryParse(cutoffTimeOption.Value(), out var cutoffTime)) return cutoffTime;

            throw new ArgumentException(
                "Unable to parse the cutofftime, please supply the cutoffTime in the following format 'yyyy-MM-dd hh:mm:ss'");
        }


        private static Task RunMigration(ILogger logger, EndpointFilter endpointFilter, DateTime? cutoffTime,
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

        private static EndpointFilter ParseEndpointFilter(CommandOption allEndpointsOption,
            CommandOption endpointFilterOption)
        {
            if (allEndpointsOption.HasValue()) return EndpointFilter.IncludeAll;

            if (!endpointFilterOption.HasValue())
            {
                throw new ArgumentException(
                    $"Either specify a specific endpoint using --{ApplicationOptions.EndpointFilter} or use the --{ApplicationOptions.AllEndpoints} option");
            }

            return EndpointFilter.SpecificEndpoint(endpointFilterOption.Value());
        }

        private static void UnhandledAppdomainExceptionHandler(object sender, UnhandledExceptionEventArgs args)
        {
            var exception = (Exception) args.ExceptionObject;
            Console.WriteLine("Unhandled appdomain exception: " + exception);
            Console.WriteLine("Runtime terminating: {0}", args.IsTerminating);
        }
    }
}