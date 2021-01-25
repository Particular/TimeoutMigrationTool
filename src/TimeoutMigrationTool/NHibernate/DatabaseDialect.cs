namespace Particular.TimeoutMigrationTool.NHibernate
{
    using System;
    using global::NHibernate.Cfg;

    public abstract class DatabaseDialect
    {
        public static DatabaseDialect Parse(string dialectString)
        {
            if (dialectString.Equals(MsSqlDatabaseDialect.Name, StringComparison.InvariantCultureIgnoreCase))
            {
                return new MsSqlDatabaseDialect();
            }
            if (dialectString.Equals(OracleDatabaseDialect.Name, StringComparison.InvariantCultureIgnoreCase))
            {
                return new OracleDatabaseDialect();
            }

            throw new InvalidOperationException($"{dialectString} is not a supported dialect");
        }

        public abstract void ConfigureDriverAndDialect(Configuration configuration);
        public abstract string GetSqlToBreakStagedTimeoutsIntoBatches(int batchSize);
    }
}