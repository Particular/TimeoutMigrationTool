namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using McMaster.Extensions.CommandLineUtils;
    using SqlP;
    using RabbitMq;
    using RavenDB;
    using System.Threading.Tasks;
    using System.Linq;

    class Program
    {
        static int Main(string[] args)
        {
            var app = new CommandLineApplication
            {
                Name = "migrate-timeouts"
            };

            var targetOption = new CommandOption($"-t|--{ApplicationOptions.RabbitMqTargetConnectionString}", CommandOptionType.SingleValue)
            {
                Description = "The connection string for the target transport"
            };

            var cutoffTimeOption = new CommandOption($"-c|--{ApplicationOptions.CutoffTime}", CommandOptionType.SingleValue)
            {
                Description = "The cut off time to apply when finding eligible timeouts"
            };

            var abortMigrationOption = new CommandOption($"-f|--{ApplicationOptions.AbortMigration}", CommandOptionType.NoValue)
            {
                Description = "To abort the current migration run."
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
                };

                var timeoutTableOption = new CommandOption($"--{ApplicationOptions.SqlTimeoutTableName}", CommandOptionType.SingleValue)
                {
                    Description = "The table name where timeouts are stored"
                };

                var sourceDialect = new CommandOption($"-d|--{ApplicationOptions.SqlSourceDialect}", CommandOptionType.SingleValue)
                {
                    Description = "The sql dialect to use"
                };

                sqlpCommand.Options.Add(allEndpointsOption);
                sqlpCommand.Options.Add(endpointFilterOption);
                sqlpCommand.Options.Add(targetOption);
                sqlpCommand.Options.Add(cutoffTimeOption);
                sqlpCommand.Options.Add(abortMigrationOption);

                sqlpCommand.Options.Add(sourceOption);
                sqlpCommand.Options.Add(timeoutTableOption);
                sqlpCommand.Options.Add(sourceDialect);


                sqlpCommand.OnExecuteAsync(async (cancellationToken) =>
                {
                    var sourceConnectionString = sourceOption.Value();
                    var targetConnectionString = targetOption.Value();
                    var timeoutTableName = timeoutTableOption.Value();
                    var dialect = SqlDialect.Parse(sourceDialect.Value());

                    if (abortMigrationOption.HasValue())
                    {
                        runParameters.Add(ApplicationOptions.AbortMigration, "");
                    }

                    runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString, targetConnectionString);
                    runParameters.Add(ApplicationOptions.CutoffTime, cutoffTimeOption.ToString());

                    runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                    runParameters.Add(ApplicationOptions.SqlTimeoutTableName, timeoutTableName);
                    runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceDialect.Value());

                    var timeoutStorage = new SqlTimeoutStorage(sourceConnectionString, dialect, timeoutTableName, 1024, "run parameters jason thing goes here");
                    var transportAdapter = new RabbitMqTimeoutCreator(targetConnectionString);

                    var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                    await RunMigration(endpointFilter, runParameters, timeoutStorage, transportAdapter);
                });
            });

            app.Command("ravendb", ravenDBCommand =>
            {
                var serverUrlOption = new CommandOption($"--{ApplicationOptions.RavenServerUrl}", CommandOptionType.SingleValue)
                {
                    Description = "The url to the ravendb server"
                };

                var databaseNameOption = new CommandOption($"--{ApplicationOptions.RavenDatabaseName}", CommandOptionType.SingleValue)
                {
                    Description = "The name of the ravendb database"
                };

                var prefixOption = new CommandOption($"--{ApplicationOptions.RavenTimeoutPrefix}", CommandOptionType.SingleValue)
                {
                    Description = "The prefix used for the document collection containing the timeouts",
                };

                var ravenDbVersion = new CommandOption($"--{ApplicationOptions.RavenVersion}", CommandOptionType.SingleValue)
                {
                    Description = "The version of RavenDB being used in your environment. Only 3.5 and 4 are supported",
                };

                ravenDBCommand.Options.Add(allEndpointsOption);
                ravenDBCommand.Options.Add(endpointFilterOption);
                ravenDBCommand.Options.Add(targetOption);
                ravenDBCommand.Options.Add(cutoffTimeOption);
                ravenDBCommand.Options.Add(abortMigrationOption);

                ravenDBCommand.Options.Add(serverUrlOption);
                ravenDBCommand.Options.Add(databaseNameOption);
                ravenDBCommand.Options.Add(prefixOption);
                ravenDBCommand.Options.Add(ravenDbVersion);

                ravenDBCommand.OnExecuteAsync(async (cancellationToken) =>
                {
                    var serverUrl = serverUrlOption.Value();
                    var databaseName = databaseNameOption.Value();
                    var prefix = prefixOption.Value();
                    var targetConnectionString = targetOption.Value();
                    var ravenVersion = ravenDbVersion.Value() == "3.5"
                        ? RavenDbVersion.ThreeDotFive
                        : RavenDbVersion.Four;

                    runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString, targetConnectionString);
                    runParameters.Add(ApplicationOptions.CutoffTime, cutoffTimeOption.ToString());

                    runParameters.Add(ApplicationOptions.RavenServerUrl, serverUrl);
                    runParameters.Add(ApplicationOptions.RavenDatabaseName, databaseName);
                    runParameters.Add(ApplicationOptions.RavenTimeoutPrefix, prefix);
                    runParameters.Add(ApplicationOptions.RavenVersion, ravenVersion.ToString());

                    var abort = abortMigrationOption.HasValue();
                    var timeoutStorage = new RavenDBTimeoutStorage(serverUrl, databaseName, prefix, ravenVersion);

                    if (abort)
                    {
                        await AbortMigration(timeoutStorage);
                    }
                    else
                    {
                        var transportAdapter = new RabbitMqTimeoutCreator(targetConnectionString);
                        var endpointFilter = ParseEndpointFilter(allEndpointsOption, endpointFilterOption);

                        await RunMigration(endpointFilter, runParameters, timeoutStorage, transportAdapter);
                    }
                });
            });

            app.OnExecute(() =>
            {
                Console.WriteLine("Specify a subcommand");
                app.ShowHelp();
                return 1;
            });

            try
            {
                return app.Execute(args);
            }
            catch (Exception exception)
            {
                Console.Error.WriteLine($"Command failed with exception ({exception.GetType().Name}): {exception.Message}");
                return 1;
            }
        }

        static async Task AbortMigration(ITimeoutStorage timeoutStorage)
        {
            var toolState = await timeoutStorage.GetToolState();
            if (toolState == null)
            {
                await Console.Out.WriteLineAsync("Could not a previous run to abort.");
                return;
            }

            await timeoutStorage.Abort(toolState);
        }

        static Task RunMigration(EndpointFilter endpointFilter, Dictionary<string, string> runParameters, ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutCreator)
        {
            var migrationRunner = new MigrationRunner(timeoutStorage, transportTimeoutCreator);

            var cutOffTime = DateTime.Now.AddDays(-1);
            if (runParameters.TryGetValue(ApplicationOptions.CutoffTime, out var cutOffTimeValue))
            {
                if (!DateTime.TryParse(cutOffTimeValue, out cutOffTime))
                {
                    throw new ArgumentException($"{ApplicationOptions.CutoffTime} is not a valid System.DateTime value.");
                }
            }

            return migrationRunner.Run(cutOffTime, endpointFilter, runParameters);
        }

        static EndpointFilter ParseEndpointFilter(CommandOption allEndpointsOption, CommandOption endpointFilterOption)
        {
            if (allEndpointsOption.HasValue())
            {
                return EndpointFilter.IncludeAll;
            }

            return EndpointFilter.SpecificEndpoint(endpointFilterOption.Value());
        }
    }
}