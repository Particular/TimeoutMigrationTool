namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using McMaster.Extensions.CommandLineUtils;
    using SqlP;
    using RabbitMq;
    using RavenDB;
    using System.Threading.Tasks;

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

            var forceMigrationOption = new CommandOption($"-f|--{ApplicationOptions.ForceMigration}", CommandOptionType.NoValue)
            {
                Description = "To force the migration and start over."
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

                sqlpCommand.Options.Add(targetOption);
                sqlpCommand.Options.Add(cutoffTimeOption);
                sqlpCommand.Options.Add(forceMigrationOption);

                sqlpCommand.Options.Add(sourceOption);
                sqlpCommand.Options.Add(timeoutTableOption);
                sqlpCommand.Options.Add(sourceDialect);


                sqlpCommand.OnExecuteAsync(async (cancellationToken) =>
                {
                    var sourceConnectionString = sourceOption.Value();
                    var targetConnectionString = targetOption.Value();
                    var timeoutTableName = timeoutTableOption.Value();
                    var dialect = SqlDialect.Parse(sourceDialect.Value());

                    if (forceMigrationOption.HasValue())
                    {
                        runParameters.Add(ApplicationOptions.ForceMigration, "");
                    }
                    runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString, targetConnectionString);
                    runParameters.Add(ApplicationOptions.CutoffTime, cutoffTimeOption.ToString());

                    runParameters.Add(ApplicationOptions.SqlSourceConnectionString, sourceConnectionString);
                    runParameters.Add(ApplicationOptions.SqlTimeoutTableName, timeoutTableName);
                    runParameters.Add(ApplicationOptions.SqlSourceDialect, sourceDialect.Value());

                    var timeoutStorage = new SqlTimeoutStorage(sourceConnectionString, dialect, timeoutTableName);
                    var transportAdapter = new RabbitMqTimeoutCreator(targetConnectionString);

                    await RunMigration(runParameters, timeoutStorage, transportAdapter).ConfigureAwait(false);
                });
            });

            app.Command("demo", demoCommand =>
            {
                demoCommand.Options.Add(forceMigrationOption);

                demoCommand.OnExecuteAsync(async (cancellationToken) =>
                {
                    if (forceMigrationOption.HasValue())
                    {
                        runParameters.Add(ApplicationOptions.ForceMigration, "");
                    }

                    var timeoutStorage = new DemoStorage();
                    var transportAdapter = new DemoTimeoutCreator();

                    await RunMigration(runParameters, timeoutStorage, transportAdapter).ConfigureAwait(false);
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

                ravenDBCommand.Options.Add(targetOption);
                ravenDBCommand.Options.Add(cutoffTimeOption);
                ravenDBCommand.Options.Add(forceMigrationOption);

                ravenDBCommand.Options.Add(serverUrlOption);
                ravenDBCommand.Options.Add(databaseNameOption);
                ravenDBCommand.Options.Add(prefixOption);
                ravenDBCommand.Options.Add(ravenDbVersion);

                ravenDBCommand.OnExecuteAsync(async (cancellationToken) =>
                {
                    var serverUrl = serverUrlOption.Value();
                    var databaseName = databaseNameOption.Value();
                    var prefix = prefixOption.Value(); // TODO: make value "TimeoutDatas" the default for the prefix
                    var targetConnectionString = targetOption.Value();
                    var ravenVersion = ravenDbVersion.Value() == "3.5"
                        ? RavenDbVersion.ThreeDotFive
                        : RavenDbVersion.Four;

                    if (forceMigrationOption.HasValue()) //TODO: double check if this is the way to check if an argument with no value is on the command line
                    {
                        runParameters.Add(ApplicationOptions.ForceMigration, "");
                    }

                    runParameters.Add(ApplicationOptions.RabbitMqTargetConnectionString, targetConnectionString);
                    runParameters.Add(ApplicationOptions.CutoffTime, cutoffTimeOption.ToString());

                    runParameters.Add(ApplicationOptions.RavenServerUrl, serverUrl);
                    runParameters.Add(ApplicationOptions.RavenDatabaseName, databaseName);
                    runParameters.Add(ApplicationOptions.RavenTimeoutPrefix, prefix);
                    runParameters.Add(ApplicationOptions.RavenVersion, ravenVersion.ToString());

                    var timeoutStorage = new RavenDBTimeoutStorage(serverUrl, databaseName, prefix, ravenVersion);
                    var transportAdapter = new RabbitMqTimeoutCreator(targetConnectionString);

                    await RunMigration(runParameters, timeoutStorage, transportAdapter).ConfigureAwait(false);
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

        static Task RunMigration(Dictionary<string, string> runParameters, ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutCreator)
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

            var forceMigration = runParameters.ContainsKey(ApplicationOptions.ForceMigration);

            return migrationRunner.Run(cutOffTime, forceMigration, runParameters);
        }
    }
}