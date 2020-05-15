namespace Particular.TimeoutMigrationTool
{
    using System;
    using McMaster.Extensions.CommandLineUtils;
    using TimeoutMigrationTool.SqlP;
    using TimeoutMigrationTool.RabbitMq;

    class Program
    {
        static int Main(string[] args)
        {
            var app = new CommandLineApplication
            {
                Name = "migrate-timeouts"
            };

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

            var targetOption = new CommandOption("-t|--target", CommandOptionType.SingleValue)
            {
                Description = "The connection string for the target transport"
            };

            app.HelpOption(inherited: true);

            app.Command("run", endpointCommand =>
            {
                endpointCommand.Options.Add(sourceOption);
                endpointCommand.Options.Add(timeoutTableOption);
                endpointCommand.Options.Add(sourceDialect);
                endpointCommand.Options.Add(targetOption);

                endpointCommand.OnExecuteAsync(async (cancellationToken) =>
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