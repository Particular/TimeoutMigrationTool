
namespace TimeoutMigrationTool.Msmq.AcceptanceTests
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using NUnit.Framework;

    public class MigrationRunner
    {
        public static void Run(string connectionString)
        {
            var isDebug = TestContext.CurrentContext.TestDirectory.IndexOf(@"\bin\Debug\", StringComparison.OrdinalIgnoreCase) != -1;
            var build = isDebug ? "Debug" : "Release";

            var exePath = Path.GetFullPath(TestContext.CurrentContext.TestDirectory + $@"\..\..\..\..\TimeoutMigrationTool\bin\{build}\net6.0\TimeoutMigrationTool.dll");
            var args = $@"exec {exePath} migrate --allEndpoints sqlp  --source ""{connectionString}"" --dialect MsSqlServer msmq --target ""{connectionString}""";

            var startInfo = new ProcessStartInfo("dotnet", args)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            var process = new Process { StartInfo = startInfo };

            string standardError = null;
            process.ErrorDataReceived += (sender, e) => { standardError += e.Data; };

            process.Start();
            process.BeginErrorReadLine();
            var standardOutput = process.StandardOutput.ReadToEnd();
            process.WaitForExit(60000);

            if (!string.IsNullOrEmpty(standardOutput))
            {
                Console.WriteLine($"{Environment.NewLine}Standard output:{Environment.NewLine}{standardOutput}{Environment.NewLine}");
            }

            if (!string.IsNullOrEmpty(standardError))
            {
                throw new Exception(standardError);
            }
        }
    }
}
