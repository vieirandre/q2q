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

            if (receiveResponse.Messages.Count == 0)
                continue;

            foreach (var message in receiveResponse.Messages)
            {
                try
                {
                    var sendRequest = new SendMessageRequest
                    {
                        QueueUrl = destinationQueueUrl,
                        MessageBody = message.Body,
                        MessageAttributes = message.MessageAttributes
                    };

                    var sendResponse = await _sqsClient.SendMessageAsync(sendRequest, cancellationToken);
                }
                catch (Exception ex)
                {
                    // log
                }
            }
        }
    } 
}
