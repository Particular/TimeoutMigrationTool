namespace TimeoutMigrationTool.RabbitMq.IntegrationTests
{
    using System;
    using Microsoft.Extensions.Logging;

    public class TestLoggingAdapter : ILogger
    {
        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            //TODO: Write this to the ATTs test logger
            Console.WriteLine(formatter(state, exception));
        }
    }
}