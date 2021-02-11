namespace Particular.TimeoutMigrationTool.SQS
{
    using System;
    using System.Globalization;

    public static class DateTimeHelper
    {
        public static string ToWireFormattedString(DateTime dateTime) => dateTime.ToUniversalTime().ToString(Format, CultureInfo.InvariantCulture);

        public static DateTime ToDateTimeOffset(string wireFormattedString)
        {
            if (wireFormattedString.Length != Format.Length)
            {
                throw new FormatException(ErrorMessage);
            }

            var year = 0;
            var month = 0;
            var day = 0;
            var hour = 0;
            var minute = 0;
            var second = 0;
            var microSecond = 0;

            for (var i = 0; i < Format.Length; i++)
            {
                var digit = wireFormattedString[i];

                switch (Format[i])
                {
                    case 'y':
                        if (digit < '0' || digit > '9')
                        {
                            throw new FormatException(ErrorMessage);
                        }

                        year = (year * 10) + (digit - '0');
                        break;

                    case 'M':
                        if (digit < '0' || digit > '9')
                        {
                            throw new FormatException(ErrorMessage);
                        }

                        month = (month * 10) + (digit - '0');
                        break;

                    case 'd':
                        if (digit < '0' || digit > '9')
                        {
                            throw new FormatException(ErrorMessage);
                        }

                        day = (day * 10) + (digit - '0');
                        break;

                    case 'H':
                        if (digit < '0' || digit > '9')
                        {
                            throw new FormatException(ErrorMessage);
                        }

                        hour = (hour * 10) + (digit - '0');
                        break;

                    case 'm':
                        if (digit < '0' || digit > '9')
                        {
                            throw new FormatException(ErrorMessage);
                        }

                        minute = (minute * 10) + (digit - '0');
                        break;

                    case 's':
                        if (digit < '0' || digit > '9')
                        {
                            throw new FormatException(ErrorMessage);
                        }

                        second = (second * 10) + (digit - '0');
                        break;

                    case 'f':
                        if (digit < '0' || digit > '9')
                        {
                            throw new FormatException(ErrorMessage);
                        }

                        microSecond = (microSecond * 10) + (digit - '0');
                        break;

                    default:
                        break;
                }
            }

            var timestamp = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Utc);
            timestamp = timestamp.AddMicroseconds(microSecond);
            return timestamp;
        }
        private static DateTime AddMicroseconds(this DateTime self, int microseconds) => self.AddTicks(microseconds * TicksPerMicrosecond);

        const string Format = "yyyy-MM-dd HH:mm:ss:ffffff Z";
        const string ErrorMessage = "String was not recognized as a valid DateTime.";
        const int TicksPerMicrosecond = 10;
    }
}