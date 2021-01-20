using Particular.TimeoutMigrationTool.Nhb;

namespace Particular.TimeoutMigrationTool.SqlP
{
    using System;
    using McMaster.Extensions.CommandLineUtils;
    using McMaster.Extensions.CommandLineUtils.Validation;
    using System.ComponentModel.DataAnnotations;

    public class NHibernateDialectValidator : IOptionValidator
    {
        public ValidationResult GetValidationResult(CommandOption option, ValidationContext context)
        {
            if (!option.HasValue())
            {
                return new ValidationResult("NHibernate dialect must be specified");
            }

            var dialectString = option.Value();

            if (dialectString.Equals(MsSqlDatabaseDialect.Name, StringComparison.InvariantCultureIgnoreCase) ||
                dialectString.Equals(OracleDatabaseDialect.Name, StringComparison.InvariantCultureIgnoreCase))
            {
                return ValidationResult.Success;
            }

            return new ValidationResult($"{dialectString} is not a supported dialect, use: {MsSqlDatabaseDialect.Name} or {OracleDatabaseDialect.Name}");
        }
    }
}