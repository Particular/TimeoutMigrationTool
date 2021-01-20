using NHibernate.Cfg;
using System;
using System.Collections.Generic;
using System.Text;

namespace Particular.TimeoutMigrationTool.NHibernate
{
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
        public abstract string GetSqlTobreakStagedTimeoutsIntoBatches(int batchSize);
    }
}