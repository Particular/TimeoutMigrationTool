﻿namespace Particular.TimeoutMigrationTool.SqlP
{
    using System;
    using McMaster.Extensions.CommandLineUtils;
    using McMaster.Extensions.CommandLineUtils.Validation;
    using System.ComponentModel.DataAnnotations;

    public class SqlDialectValidator : IOptionValidator
    {
        public ValidationResult GetValidationResult(CommandOption option, ValidationContext context)
        {
            if (!option.HasValue())
            {
                return new ValidationResult("SqlDialect must be specified");
            }

            var dialectString = option.Value();

            if (dialectString.Equals(MsSqlServer.Name, StringComparison.InvariantCultureIgnoreCase))
            {
                return ValidationResult.Success;
            }

            return new ValidationResult($"{dialectString} is not a supported dialect, use: {MsSqlServer.Name}");
        }
    }
}