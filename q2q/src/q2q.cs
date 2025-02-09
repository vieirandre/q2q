using Amazon.SQS;
using Amazon.SQS.Model;
using q2q.Models;

namespace q2q;

public class q2q(IAmazonSQS? sqsClient = null, q2qOptions? options = null) : Iq2q
{
    private readonly IAmazonSQS _sqsClient = sqsClient ?? new AmazonSQSClient();
    private readonly q2qOptions _options = options ?? new q2qOptions();

    private readonly HashSet<string> _sourceQueueMessageIds = [];

    public async Task ForwardMessages(string sourceQueueUrl, string destinationQueueUrl, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var messages = await ReceiveMessages(sourceQueueUrl, cancellationToken);
            var newMessages = messages.Where(m => !_sourceQueueMessageIds.Contains(m.MessageId)).ToList();

            if (newMessages.Count == 0)
            {
                await Task.Delay(_options.PollingDelayMilliseconds, cancellationToken);
                continue;
            }

            await SendMessageBatches(newMessages, destinationQueueUrl, cancellationToken);
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

    private async Task SendMessageBatches(IEnumerable<Message> messages, string destinationQueueUrl, CancellationToken cancellationToken)
    {
        var batches = messages
            .Select((message, index) => new { message, index })
            .GroupBy(x => x.index / 10, x => x.message);

        foreach (var batch in batches)
        {
            var batchEntries = batch.Select(message => new SendMessageBatchRequestEntry
            {
                Id = message.MessageId, // !
                MessageBody = message.Body,
                MessageAttributes = message.MessageAttributes
            });

            var batchRequest = new SendMessageBatchRequest
            {
                QueueUrl = destinationQueueUrl,
                Entries = batchEntries.ToList()
            };

            try
            {
                var response = await _sqsClient.SendMessageBatchAsync(batchRequest, cancellationToken);

                response
                    .Failed
                    .ForEach(failed => Console.Error.WriteLine($"Failed to send message w/ id {failed.Id}: {failed.Message}"));

                response
                    .Successful
                    .ForEach(successful => _sourceQueueMessageIds.Add(successful.Id));
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error sending message batch: {ex.Message}");
            }
        }
    }
}
