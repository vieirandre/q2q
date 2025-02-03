using Amazon.SQS;

namespace q2q;

public class q2q
{
    private readonly IAmazonSQS _sqsClient;

    public q2q()
    {
        _sqsClient = new AmazonSQSClient();
    }
}
