namespace Particular.TimeoutMigrationTool
{
    using System;
    using Microsoft.Extensions.Logging;

    public class ConsoleLogger : ILogger
    {
        public ConsoleLogger(bool verbose)
        {
            this.verbose = verbose;
        }

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
            if(!verbose && logLevel <  LogLevel.Information)
            {
                return;
            }

            Console.WriteLine(formatter(state, exception));
        }

        readonly bool verbose;
    }
}