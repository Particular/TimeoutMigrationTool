
namespace TimeoutMigrationTool.Msmq.AcceptanceTests
{
    using System;
    using System.Diagnostics;
    using System.IO;

    public class MigrationRunner
    {
        public static void Run(string connectionString)
        {
            var currentFolder = Directory.GetCurrentDirectory();

            var isDebug = currentFolder.IndexOf(@"\bin\Debug\", StringComparison.InvariantCultureIgnoreCase) != -1;
            var build = isDebug ? "Debug" : "Release";

            var exePath = currentFolder + $@"\..\..\..\..\TimeoutMigrationTool\bin\{build}\net6.0\TimeoutMigrationTool.dll";
            var args = $@"exec {exePath} migrate --allEndpoints sqlp  --source ""{connectionString}"" --dialect MsSqlServer msmq --target ""{connectionString}""";

            var startInfo = new ProcessStartInfo("dotnet", args)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            var process = Process.Start(startInfo);

            Console.WriteLine(process.StandardOutput.ReadToEnd());

            process.WaitForExit(60000);

        }
    }
}
