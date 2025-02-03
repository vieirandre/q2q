using Amazon.SQS;

namespace q2q;

public class q2q : Iq2q
{
    private readonly IAmazonSQS _sqsClient;

    public q2q()
    {
        _sqsClient = new AmazonSQSClient();
    }

    public Task ForwardMessages(string sourceQueueUrl, string destinationQueueUrl, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
