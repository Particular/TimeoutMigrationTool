namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Linq;

    public class MigrationCheckResult
    {
        public List<string> Problems { get; set; } = new List<string>();

        public bool CanMigrate => !Problems.Any();
    }
}