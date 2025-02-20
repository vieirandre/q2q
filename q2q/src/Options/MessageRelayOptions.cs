using q2q.Helpers;

namespace q2q.Options;

public class MessageRelayOptions
{
    private int _maxNumberOfMessages = 10;
    private int _waitTimeSeconds = 10;
    private int _batchSize = 10;

    public int MaxNumberOfMessages
    {
        get => _maxNumberOfMessages;
        set => _maxNumberOfMessages = Guards.EnsureInRange(value, nameof(MaxNumberOfMessages), 1, 10);
    }

    public int WaitTimeSeconds
    {
        get => _waitTimeSeconds;
        set => _waitTimeSeconds = Guards.EnsureInRange(value, nameof(WaitTimeSeconds), 0, 20);
    }

    public int PollingDelayMilliseconds { get; set; } = 1000;

    public int BatchSize
    {
        get => _batchSize;
        set => _batchSize = Guards.EnsureInRange(value, nameof(BatchSize), 1, 10);
    }
}
