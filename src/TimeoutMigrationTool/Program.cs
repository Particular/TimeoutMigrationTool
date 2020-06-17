namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using McMaster.Extensions.CommandLineUtils;
    using SqlP;
    using RabbitMq;
    using RavenDB;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    // usage:
    //  migrate-timeouts ravendb preview --raven-specific-options --target-specific-options
    //  migrate-timeouts ravendb migrate --raven-specific-options --target-specific-options [--cutoff-time] [--endpoint-filter]
    //  migrate-timeouts ravendb abort --raven-specific-options
    //  migrate-timeouts sqlp preview --sqlp-specific-options --target-specific-options
    //  migrate-timeouts sqlp migrate --sqlp-specific-options --target-specific-options [--cutoff-time] [--endpoint-filter]
    //  migrate-timeouts sqlp abort --sqlp-specific-options
    //
    // Examples:
    //  ravendb preview --serverUrl http://localhost:8080 --databaseName raven-timeout-test --prefix TimeoutDatas --ravenVersion 4 --target amqp://guest:guest@localhost:5672
    //  sqlp preview --source \"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=MyTestDB;Integrated Security=True;\" --dialect MsSqlServer --target amqp://guest:guest@localhost:5672
    class Program
    {
        const string CutoffTimeFormat = "yyyy-MM-dd HH:mm:ss:ffffff Z";
        static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(UnhandledAppdomainExceptionHandler);

            var app = new CommandLineApplication
            {
                Name = "migrate-timeouts"
            };

            var verboseOption = app.Option("-v|--verbose", "Show verbose output", CommandOptionType.NoValue, true);
            var targetOption = new CommandOption($"-t|--{ApplicationOptions.RabbitMqTargetConnectionString}", CommandOptionType.SingleValue)
            {
                Description = "The connection string for the target transport"
            };

            var cutoffTimeOption = new CommandOption($"-c|--{ApplicationOptions.CutoffTime}", CommandOptionType.SingleValue)
            {
                Description = "The cut off time to apply when finding eligible timeouts"
            };

            var endpointFilterOption = new CommandOption($"--{ApplicationOptions.EndpointFilter}", CommandOptionType.SingleValue)
            {
                Description = $"The endpoint to migrate timeouts for, use --{ApplicationOptions.AllEndpoints} to include all endpoints"
            };

            var allEndpointsOption = new CommandOption($"--{ApplicationOptions.AllEndpoints}", CommandOptionType.NoValue)
            {
                Description = "Option to include migrate timeouts for all found endpoints in the database"
            };

            app.HelpOption(inherited: true);
            var runParameters = new Dictionary<string, string>();

            app.Command("sqlp", sqlpCommand =>
            {
                var sourceOption = new CommandOption($"-s|--{ApplicationOptions.SqlSourceConnectionString}", CommandOptionType.SingleValue)
                {
                    Description = "Connection string for source storage",
                    Inherited = true
                };

                var sourceDialect = new CommandOption($"-d|--{ApplicationOptions.SqlSourceDialect}", CommandOptionType.SingleValue)
                {
                    Description = "The sql dialect to use",
                    Inherited = true
                };

                sourceDialect.Validators.Add(new SqlDialectValidator());

                sqlpCommand.Options.Add(sourceOption);
                sqlpCommand.Options.Add(sourceDialect);

                sqlpCommand.Command("preview", previewCommand =>
                {
                    previewCommand.Description = "Lists endpoints that can be migrated.";

                    previewCommand.Options.Add(targetOption);

                    previewCommand.OnExecuteAsync(async ct =>
                    {
                        var logger = new ConsoleLogger(verboseOption.HasValue());

                        var sourceConnectionString = sourceOption.Value();
                        var dialect = SqlDialect.Parse(sourceDialect.Value());

                        var targetConnectionString = targetOption.Value();

                        var timeoutStorage = new SqlTimeoutStorage(sourceConnectionString, dialect, 1024);
                        var transportAdapter = new RabbitMqTimeoutCreator(logger, targetConnectionString);
                        var runner = new PreviewRunner(logger, timeoutStorage, transportAdapter);

                        await runner.Run();
                    });
                });

                sqlpCommand.Command("migrate", migrateCommand =>
                {
                    migrateCommand.Description = "Performs migration of selected endpoint(s).";

                    migrateCommand.Options.Add(targetOption);

                    migrateCommand.Options.Add(allEndpointsOption);
                    migrateCommand.Options.Add(endpointFilterOption);
                    migrateCommand.Options.Add(cutoffTimeOption);

                    migrateCommand.OnExecuteAsync(async ct =>
                    {
                        var logger = new ConsoleLogger(verboseOption.HasValue());

                        var sourceConnectionString = sourceOption.Value();
                        var dialect = SqlDialect.Parse(sourceDialect.Value());
                        var targetConnectionString = targetOption.Value();

                        var cutoffTime = GetCutoffTime(cutoffTimeOption);

                        runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                        runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceDialect.Value());

                        runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString, targetConnectionString);

                        var timeoutStorage = new SqlTimeoutStorage(sourceConnectionString, dialect, 1024);

                        var transportAdapter = new RabbitMqTimeoutCreator(logger, targetConnectionString);
                        var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                        await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutStorage, transportAdapter);
                    });
                });

                sqlpCommand.Command("abort", abortCommand =>
                {
                    abortCommand.Description = "Aborts currently ongoing migration and restores unmigrated timeouts.";

                    abortCommand.OnExecuteAsync(async ct =>
                    {
                        var logger = new ConsoleLogger(verboseOption.HasValue());

                        var sourceConnectionString = sourceOption.Value();
                        var dialect = SqlDialect.Parse(sourceDialect.Value());

                        var timeoutStorage = new SqlTimeoutStorage(sourceConnectionString, dialect, 1024);
                        var runner = new AbortRunner(logger, timeoutStorage);

                        await runner.Run();
                    });
                });
            });

            app.Command("ravendb", ravenDBCommand =>
            {
                ravenDBCommand.OnExecute(() =>
                {
                    Console.WriteLine("Specify a subcommand");
                    ravenDBCommand.ShowHelp();
                    return 1;
                });

                var serverUrlOption = new CommandOption($"--{ApplicationOptions.RavenServerUrl}", CommandOptionType.SingleValue)
                {
                    Description = "The url to the ravendb server",
                    Inherited = true
                };

                var databaseNameOption = new CommandOption($"--{ApplicationOptions.RavenDatabaseName}", CommandOptionType.SingleValue)
                {
                    Description = "The name of the ravendb database",
                    Inherited = true
                };

                var prefixOption = new CommandOption($"--{ApplicationOptions.RavenTimeoutPrefix}", CommandOptionType.SingleValue)
                {
                    Description = "The prefix used for the document collection containing the timeouts",
                    Inherited = true
                };

                var ravenDbVersion = new CommandOption($"--{ApplicationOptions.RavenVersion}", CommandOptionType.SingleValue)
                {
                    Description = "The version of RavenDB being used in your environment. Only 3.5 and 4 are supported",
                    Inherited = true
                };

                ravenDBCommand.Options.Add(serverUrlOption);
                ravenDBCommand.Options.Add(databaseNameOption);
                ravenDBCommand.Options.Add(prefixOption);
                ravenDBCommand.Options.Add(ravenDbVersion);

                ravenDBCommand.Command("preview", previewCommand =>
                {
                    previewCommand.Description = "Lists endpoints that can be migrated.";

                    previewCommand.Options.Add(targetOption);

                    previewCommand.OnExecuteAsync(async ct =>
                    {
                        var logger = new ConsoleLogger(verboseOption.HasValue());

                        var serverUrl = serverUrlOption.Value();
                        var databaseName = databaseNameOption.Value();
                        var prefix = prefixOption.Value();
                        var targetConnectionString = targetOption.Value();
                        var ravenVersion = ravenDbVersion.Value() == "3.5"
                            ? RavenDbVersion.ThreeDotFive
                            : RavenDbVersion.Four;

                        var timeoutStorage = new RavenDBTimeoutStorage(logger, serverUrl, databaseName, prefix, ravenVersion);
                        var transportAdapter = new RabbitMqTimeoutCreator(logger, targetConnectionString);
                        var runner = new PreviewRunner(logger, timeoutStorage, transportAdapter);

                        await runner.Run();
                    });
                });

                ravenDBCommand.Command("migrate", migrateCommand =>
                {
                    migrateCommand.Description = "Performs migration of selected endpoint(s).";

                    migrateCommand.Options.Add(targetOption);

                    migrateCommand.Options.Add(allEndpointsOption);
                    migrateCommand.Options.Add(endpointFilterOption);
                    migrateCommand.Options.Add(cutoffTimeOption);

                    migrateCommand.OnExecuteAsync(async ct =>
                    {
                        var logger = new ConsoleLogger(verboseOption.HasValue());

                        var serverUrl = serverUrlOption.Value();
                        var databaseName = databaseNameOption.Value();
                        var prefix = prefixOption.Value();
                        var targetConnectionString = targetOption.Value();
                        var ravenVersion = ravenDbVersion.Value() == "3.5"
                            ? RavenDbVersion.ThreeDotFive
                            : RavenDbVersion.Four;

                        var cutoffTime = GetCutoffTime(cutoffTimeOption);

                        runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString, targetConnectionString);

                        runParameters.Add(ApplicationOptions.RavenServerUrl, serverUrl);
                        runParameters.Add(ApplicationOptions.RavenDatabaseName, databaseName);
                        runParameters.Add(ApplicationOptions.RavenTimeoutPrefix, prefix);
                        runParameters.Add(ApplicationOptions.RavenVersion, ravenVersion.ToString());

                        var timeoutStorage = new RavenDBTimeoutStorage(logger, serverUrl, databaseName, prefix, ravenVersion);

                        var transportAdapter = new RabbitMqTimeoutCreator(logger, targetConnectionString);
                        var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                        await RunMigration(logger, endpointFilter, cutoffTime, runParameters, timeoutStorage, transportAdapter);
                    });
                });

                ravenDBCommand.Command("abort", abortCommand =>
                {
                    abortCommand.Description = "Aborts currently ongoing migration and restores unmigrated timeouts.";

                    abortCommand.OnExecuteAsync(async ct =>
                    {
                        var logger = new ConsoleLogger(verboseOption.HasValue());

                        var serverUrl = serverUrlOption.Value();
                        var databaseName = databaseNameOption.Value();
                        var prefix = prefixOption.Value();
                        var ravenVersion = ravenDbVersion.Value() == "3.5"
                           ? RavenDbVersion.ThreeDotFive
                           : RavenDbVersion.Four;

                        var timeoutStorage = new RavenDBTimeoutStorage(logger, serverUrl, databaseName, prefix, ravenVersion);
                        var runner = new AbortRunner(logger, timeoutStorage);

                        await runner.Run();
                    });
                });
            });

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

            throw new Exception($"Unable to parse the cutofftime, please supply the cutoffTime in the following format '{CutoffTimeFormat}'");
        }


        static Task RunMigration(ILogger logger, EndpointFilter endpointFilter, DateTime? cutOffTime, Dictionary<string, string> runParameters, ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutCreator)
        {
            var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportTimeoutCreator);

            if (cutOffTime.HasValue)
            {
                runParameters.Add(ApplicationOptions.CutoffTime, cutOffTime.Value.ToString(CutoffTimeFormat));
            }
            else
            {
                cutOffTime = DateTime.UtcNow.AddDays(-1);
            }


            return migrationRunner.Run(cutOffTime.Value, endpointFilter, runParameters);
        }

        static EndpointFilter ParseEndpointFilter(CommandOption allEndpointsOption, CommandOption endpointFilterOption)
        {
            if (allEndpointsOption.HasValue())
            {
                return EndpointFilter.IncludeAll;
            }

            return EndpointFilter.SpecificEndpoint(endpointFilterOption.Value());
        }

        static void UnhandledAppdomainExceptionHandler(object sender, UnhandledExceptionEventArgs args)
        {
            var exception = (Exception)args.ExceptionObject;
            Console.WriteLine("Unhandled appdomain exception: " + exception.ToString());
            Console.WriteLine("Runtime terminating: {0}", args.IsTerminating);
        }
    }
}