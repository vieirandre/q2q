namespace q2q;

public interface Iq2q
{
    Task ForwardMessages(string sourceQueueUrl, string destinationQueueUrl, CancellationToken cancellationToken = default);
}
