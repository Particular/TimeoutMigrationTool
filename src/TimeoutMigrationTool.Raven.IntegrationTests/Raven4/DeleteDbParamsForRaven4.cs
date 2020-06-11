
namespace TimeoutMigrationTool.Raven.IntegrationTests.Raven4
{
    using System;

    public class DeleteDbParamsForRaven4
    {
        public string[] DatabaseNames { get; set; }
        public bool HardDelete { get; set; }
        public TimeSpan TimeToWaitForConfirmation { get; set; } = TimeSpan.FromSeconds(10);
    }
}