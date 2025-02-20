using q2q.Helpers;

namespace q2q.Options;

public class MessageRelayOptions
{
    private int _maxNumberOfMessages = 10;
    private int _waitTimeSeconds = 10;
    private int _sendMessageBatchSize = 10;
    private int _deleteMessageBatchSize = 10;

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

    public int SendMessageBatchSize
    {
        get => _sendMessageBatchSize;
        set => _sendMessageBatchSize = Guards.EnsureInRange(value, nameof(SendMessageBatchSize), 1, 10);
    }

    public int DeleteMessageBatchSize
    {
        get => _deleteMessageBatchSize;
        set => _deleteMessageBatchSize = Guards.EnsureInRange(value, nameof(DeleteMessageBatchSize), 1, 10);
    }
}
