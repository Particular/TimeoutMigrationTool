namespace Particular.TimeoutMigrationTool
{
    public class BatchInfo
    {
        public BatchInfo(int number, BatchState state, int numberOfTimeouts)
        {
            Number = number;
            State = state;
            NumberOfTimeouts = numberOfTimeouts;
        }

        public int Number { get; }

        public BatchState State { get; set; }

        public int NumberOfTimeouts { get; }
    }
}