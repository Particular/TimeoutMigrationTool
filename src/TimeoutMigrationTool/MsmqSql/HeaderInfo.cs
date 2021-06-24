namespace Particular.TimeoutMigrationTool.MsmqSql
{
    using System;

    [Serializable]
    public class HeaderInfo
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }
}