﻿namespace Particular.TimeoutMigrationTool
{
    using System;
    using McMaster.Extensions.CommandLineUtils;
    using SqlP;
    using RabbitMq;
    using RavenDB;

    class Program
    {
        static int Main(string[] args)
        {
            var app = new CommandLineApplication
            {
                Name = "migrate-timeouts"
            };

            var targetOption = new CommandOption("-t|--target", CommandOptionType.SingleValue)
            {
                Description = "The connection string for the target transport"
            };

            app.HelpOption(inherited: true);

            app.Command("sqlp", sqlpCommand =>
            {
                var sourceOption = new CommandOption("-s|--source", CommandOptionType.SingleValue)
                {
                    Description = "Connection string for source storage"
                };

                var timeoutTableOption = new CommandOption("--tableName", CommandOptionType.SingleValue)
                {
                    Description = "The table name where timeouts are stored"
                };

                var sourceDialect = new CommandOption("-d|--dialect", CommandOptionType.SingleValue)
                {
                    Description = "The sql dialect to use"
                };

                sqlpCommand.Options.Add(sourceOption);
                sqlpCommand.Options.Add(timeoutTableOption);
                sqlpCommand.Options.Add(sourceDialect);
                sqlpCommand.Options.Add(targetOption);

                sqlpCommand.OnExecuteAsync(async (cancellationToken) =>
                {
                    var sourceConnectionString = sourceOption.Value();
                    var targetConnectionString = targetOption.Value();
                    var timeoutTableName = timeoutTableOption.Value();
                    var dialect = SqlDialect.Parse(sourceDialect.Value());

                    var reader = new SqlTimeoutsReader();
                    var timeoutsFromSql = await reader.ReadTimeoutsFrom(sourceConnectionString, timeoutTableName, dialect, cancellationToken).ConfigureAwait(false);

                    var writer = new RabbitMqWriter();
                    await writer.WriteTimeoutsTo(targetConnectionString, timeoutsFromSql, cancellationToken).ConfigureAwait(false);

                    return 0;
                });
            });

            app.Command("ravendb", ravenDBCommand =>
            {
                var serverUrlOption = new CommandOption("--serverUrl", CommandOptionType.SingleValue)
                {
                    Description = "The url to the ravendb server"
                };

                var databaseNameOption = new CommandOption("--databaseName", CommandOptionType.SingleValue)
                {
                    Description = "The name of the ravendb database"
                };

                var prefixOption = new CommandOption("--prefix", CommandOptionType.SingleValue)
                {
                    Description = "The prefix used for the document collection containing the timeouts",
                };

                ravenDBCommand.Options.Add(serverUrlOption);
                ravenDBCommand.Options.Add(databaseNameOption);
                ravenDBCommand.Options.Add(prefixOption);
                ravenDBCommand.Options.Add(targetOption);

                ravenDBCommand.OnExecuteAsync(async (cancellationToken) =>
                {
                    var serverUrl = serverUrlOption.Value();
                    var databaseName = databaseNameOption.Value();
                    var prefix = prefixOption.Value(); // TODO: make value "TimeoutDatas" the default for the prefix
                    var targetConnectionString = targetOption.Value();

                    // TODO: Default value for cut off time should be maybe 1 week ahead
                    var reader = new RavenDBTimeoutsReader();
                    var timeouts = await reader.ReadTimeoutsFrom(serverUrl, databaseName, prefix,
            DateTime.MinValue, cancellationToken).ConfigureAwait(false);

                    var writer = new RabbitMqWriter();
                    await writer.WriteTimeoutsTo(targetConnectionString, timeouts, cancellationToken).ConfigureAwait(false);

                    return 0;
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
    }
}