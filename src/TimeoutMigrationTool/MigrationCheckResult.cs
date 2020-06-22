namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;

    public class MigrationCheckResult
    {
        public List<string> Problems { get; set; } = new List<string>();

        public bool CanMigrate
        {
            get
            {
                return !Problems.Any();
            }
        }
    }
}