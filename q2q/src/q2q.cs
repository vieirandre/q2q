using Amazon.SQS;
using Amazon.SQS.Model;

namespace q2q;

public class q2q : Iq2q
{
    private readonly IAmazonSQS _sqsClient;

    public q2q()
    {
        _sqsClient = new AmazonSQSClient();
    }

    public async Task ForwardMessages(string sourceQueueUrl, string destinationQueueUrl, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var receiveRequest = new ReceiveMessageRequest
            {
                QueueUrl = sourceQueueUrl,
                MaxNumberOfMessages = 10, // into param
                MessageSystemAttributeNames = ["All"],
                MessageAttributeNames = ["All"]
            };

            var receiveResponse = await _sqsClient.ReceiveMessageAsync(receiveRequest, cancellationToken);
        }
    } 
}
