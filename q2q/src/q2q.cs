using Amazon.SQS;
using Amazon.SQS.Model;
using q2q.Models;

namespace q2q;

public class q2q(IAmazonSQS? sqsClient = null, q2qOptions? options = null) : Iq2q
{
    private readonly IAmazonSQS _sqsClient = sqsClient ?? new AmazonSQSClient();
    private readonly q2qOptions _q2qOptions = options ?? new q2qOptions();

    private readonly HashSet<string> _sourceQueueMessageIds = [];

    public async Task ForwardMessages(string sourceQueueUrl, string destinationQueueUrl, CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var receiveRequest = new ReceiveMessageRequest
            {
                QueueUrl = sourceQueueUrl,
                MaxNumberOfMessages = _q2qOptions.MaxNumberOfMessages,
                WaitTimeSeconds = _q2qOptions.WaitTimeSeconds,
                MessageSystemAttributeNames = ["All"],
                MessageAttributeNames = ["All"]
            };

            ReceiveMessageResponse receiveResponse;

            try
            {
                receiveResponse = await _sqsClient.ReceiveMessageAsync(receiveRequest, cancellationToken);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error retrieving messages from source queue: {ex.Message}");
                break;
            }

            foreach (var message in receiveResponse.Messages)
            {
                if (_sourceQueueMessageIds.Contains(message.MessageId))
                    continue;

                _sourceQueueMessageIds.Add(message.MessageId);

                var sendRequest = new SendMessageRequest
                {
                    QueueUrl = destinationQueueUrl,
                    MessageBody = message.Body,
                    MessageAttributes = message.MessageAttributes
                };

                try
                {
                    var sendResponse = await _sqsClient.SendMessageAsync(sendRequest, cancellationToken);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error forwarding message id {message.MessageId}: {ex.Message}");
                    continue;
                }
            }
        }
    }
}
