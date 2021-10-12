namespace TimeoutMigrationTool.Msmq.AcceptanceTests
{
    using Microsoft.Extensions.Logging;
    using NServiceBus.AcceptanceTesting;
    using System;

    public class TestLoggingAdapter : ILogger
    {
        public TestLoggingAdapter(ScenarioContext scenarioContext)
        {
            this.scenarioContext = scenarioContext;
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
            scenarioContext.AddTrace(formatter(state, exception));
        }

        readonly ScenarioContext scenarioContext;
    }
}