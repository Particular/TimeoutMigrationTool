using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

[AttributeUsage(AttributeTargets.Class)]
public class EnvironmentSpecificTestAttribute : Attribute, IApplyToContext
{
    readonly string[] environmentVariableNames;

    public EnvironmentSpecificTestAttribute(params string[] environmentVariableNames) =>
        this.environmentVariableNames = environmentVariableNames ?? Array.Empty<string>();

    public void ApplyToContext(TestExecutionContext context)
    {
        var notPresentOrWhitespace = new List<string>();

        foreach (var name in environmentVariableNames)
        {
            var value = Environment.GetEnvironmentVariable(name);

            if (string.IsNullOrWhiteSpace(value))
            {
                notPresentOrWhitespace.Add(name);
            }
        }

        if (notPresentOrWhitespace.Count > 0)
        {
            Assert.Ignore($"Ignoring because environment variable(s) not present or white space: {string.Join(", ", notPresentOrWhitespace)}");
        }

        var presentAndNotWhitespace = new List<string>();

        foreach (var name in EnvironmentVariables.Names.Except(environmentVariableNames))
        {
            var value = Environment.GetEnvironmentVariable(name);

            if (!string.IsNullOrWhiteSpace(value))
            {
                presentAndNotWhitespace.Add(name);
            }
        }

        if (presentAndNotWhitespace.Count > 0)
        {
            Assert.Ignore($"Ignoring because environment variable(s) present and not white space: {string.Join(", ", presentAndNotWhitespace)}");
        }
    }
}