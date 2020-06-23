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

        public int Number { get; private set; }

        public BatchState State { get; protected set; }

        public int NumberOfTimeouts { get; private set; }
    }
}