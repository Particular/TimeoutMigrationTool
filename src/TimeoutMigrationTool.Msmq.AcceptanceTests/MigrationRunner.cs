﻿
namespace TimeoutMigrationTool.Msmq.AcceptanceTests
{
    using System;
    using System.Diagnostics;

    public class MigrationRunner
    {
        public static void Run(string connectionString)
        {
            var currentFolder = AppContext.BaseDirectory;

            var isDebug = currentFolder.IndexOf(@"\bin\Debug\", StringComparison.InvariantCultureIgnoreCase) != -1;
            var build = isDebug ? "Debug" : "Release";

            var exePath = currentFolder + $@"\..\..\..\..\TimeoutMigrationTool\bin\{build}\netcoreapp3.1\TimeoutMigrationTool.exe";
            var args = $@"migrate --allEndpoints sqlp  --source ""{connectionString}"" --dialect MsSqlServer msmq --target ""{connectionString}""";

            var startInfo = new ProcessStartInfo(exePath, args)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            var process = Process.Start(startInfo);

            Console.WriteLine(process.StandardOutput.ReadToEnd());

            process.WaitForExit(30000);

        }
    }
}
