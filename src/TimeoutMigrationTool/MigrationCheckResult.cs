namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Linq;

    public class MigrationCheckResult
    {
        public List<string> Problems { get; set; } = [];

        public bool CanMigrate => !Problems.Any();
    }
}