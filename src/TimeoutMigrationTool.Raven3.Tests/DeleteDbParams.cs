
using System;

namespace TimeoutMigrationTool.Raven3.Tests
{
    public class DeleteDbParams
    {
        public string[] DatabaseNames { get; set; }
        public bool HardDelete { get; set; }
        public TimeSpan TimeToWaitForConfirmation { get; set; } = TimeSpan.FromSeconds(10);
    }
}