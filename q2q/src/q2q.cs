using Amazon.SQS;
using Amazon.SQS.Model;
using q2q.Models;

namespace q2q;

public class q2q(IAmazonSQS? sqsClient = null, q2qOptions? options = null) : Iq2q
{
    private readonly IAmazonSQS _sqsClient = sqsClient ?? new AmazonSQSClient();
    private readonly q2qOptions _options = options ?? new q2qOptions();

    private readonly HashSet<string> _sourceQueueMessageIds = [];

    public async Task ForwardMessages(string sourceQueueUrl, string destinationQueueUrl, CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var messages = await ReceiveMessages(sourceQueueUrl, cancellationToken);

            if (!messages.Any())
                continue;

            foreach (var message in messages)
            {
                if (_sourceQueueMessageIds.Contains(message.MessageId))
                    continue;

                if (await ForwardMessage(message, destinationQueueUrl, cancellationToken))
                    _sourceQueueMessageIds.Add(message.MessageId);
            }
        }
    }

    private async Task<IEnumerable<Message>> ReceiveMessages(string sourceQueueUrl, CancellationToken cancellationToken)
    {
        var receiveRequest = new ReceiveMessageRequest
        {
            QueueUrl = sourceQueueUrl,
            MaxNumberOfMessages = _options.MaxNumberOfMessages,
            WaitTimeSeconds = _options.WaitTimeSeconds,
            MessageSystemAttributeNames = ["All"],
            MessageAttributeNames = ["All"]
        };

        try
        {
            var response = await _sqsClient.ReceiveMessageAsync(receiveRequest, cancellationToken);
            return response.Messages;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error retrieving messages from source queue: {ex.Message}");
            return [];
        }
    }

    private async Task<bool> ForwardMessage(Message message, string destinationQueueUrl, CancellationToken cancellationToken)
    {
        var sendRequest = new SendMessageRequest
        {
            QueueUrl = destinationQueueUrl,
            MessageBody = message.Body,
            MessageAttributes = message.MessageAttributes
        };

        try
        {
            await _sqsClient.SendMessageAsync(sendRequest, cancellationToken);
            return true;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error forwarding message id {message.MessageId}: {ex.Message}");
            return false;
        }
    }
}
