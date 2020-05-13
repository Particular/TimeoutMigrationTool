namespace Particular.TimeoutMigrationTool
{
    using System;
    using McMaster.Extensions.CommandLineUtils;
    using TimeoutMigrationTool.SqlP;
    using TimeoutMigrationTool.RabbitMq;

    // usage:
    // migrate-timeouts run -s = "sqlconnectionstring" -t = "rabbitconnectionstring"

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
                Description = "Prefix to prepend before all queues and topics"
            };

            var sourceDialect = new CommandOption("-d|--dialect", CommandOptionType.SingleValue)
            {
                Description = "Prefix to prepend before all queues and topics"
            };

            var targetOption = new CommandOption("-t|--target", CommandOptionType.SingleValue)
            {
                Description = "Prefix to prepend before all queues and topics"
            };

            app.HelpOption(inherited: true);

            app.Command("run", endpointCommand =>
            {
                endpointCommand.Options.Add(sourceOption);
                endpointCommand.Options.Add(sourceDialect);
                endpointCommand.Options.Add(targetOption);

                endpointCommand.OnExecuteAsync(async (cancellationToken) =>
                {
                    var sourceConnectionString = sourceOption.Value();
                    var targetConnectionString = targetOption.Value();
                    var dialect = SqlDialect.Parse(sourceDialect.Value());

                    var reader = new SqlTimeoutsReader();
                    var timeoutsFromSql = await reader.ReadTimeoutsFrom(sourceConnectionString, dialect, cancellationToken).ConfigureAwait(false);

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