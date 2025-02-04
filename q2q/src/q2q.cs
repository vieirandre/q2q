using Amazon.SQS;
using Amazon.SQS.Model;

namespace q2q;

public class q2q : Iq2q
{
    private readonly IAmazonSQS _sqsClient;
    private readonly HashSet<string> _sourceQueueMessageIds;

    public q2q()
    {
        _sqsClient = new AmazonSQSClient();
        _sourceQueueMessageIds = [];
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

            receiveResponse.Messages.RemoveAll(msg => _sourceQueueMessageIds.Contains(msg.MessageId));

            foreach (var message in receiveResponse.Messages)
            {
                try
                {
                    _sourceQueueMessageIds.Add(message.MessageId);

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
