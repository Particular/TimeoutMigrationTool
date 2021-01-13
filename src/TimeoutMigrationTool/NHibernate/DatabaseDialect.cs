using NHibernate.Cfg;
using System;
using System.Collections.Generic;
using System.Text;

namespace Particular.TimeoutMigrationTool.Nhb
{
    public abstract class DatabaseDialect
    {
        public abstract void ConfigureDriverAndDialect(Configuration configuration);
        public abstract string GetSqlTobreakStagedTimeoutsIntoBatches(int batchSize);
    }
}