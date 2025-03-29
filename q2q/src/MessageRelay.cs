using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using q2q.Options;

namespace q2q;

public class MessageRelay
{
    private readonly IAmazonSQS _sqsClient;
    private readonly MessageRelayOptions _options;
    private readonly ILogger _logger;

    private readonly HashSet<string> _sourceQueueMessageIds = new();

    public MessageRelay(IAmazonSQS? sqsClient = null, MessageRelayOptions? options = null, ILogger? logger = null)
    {
        _sqsClient = sqsClient ?? new AmazonSQSClient();
        _options = options ?? new MessageRelayOptions();
        _logger = logger ?? NullLogger<MessageRelay>.Instance;
    }

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

            var messagesSent = await SendMessages(newMessages, destinationQueueUrl, cancellationToken);

            if (_options.DeleteMessagesFromSource)
                await DeleteMessagesFromSource(messagesSent, sourceQueueUrl, cancellationToken);
        }
    }

    private async Task<IEnumerable<Message>> ReceiveMessages(string sourceQueueUrl, CancellationToken cancellationToken)
    {
        var receiveRequest = new ReceiveMessageRequest
        {
            QueueUrl = sourceQueueUrl,
            MaxNumberOfMessages = _options.MaxNumberOfMessages,
            WaitTimeSeconds = _options.WaitTimeSeconds,
            MessageSystemAttributeNames = new() { "All" },
            MessageAttributeNames = new() { "All" }
        };

        try
        {
            var response = await _sqsClient.ReceiveMessageAsync(receiveRequest, cancellationToken);
            return response.Messages;
        }
        catch (Exception ex)
        {
            _logger.LogError("Error retrieving messages from source queue: {Exception}", ex);
            return Enumerable.Empty<Message>();
        }
    }

    private async Task<IEnumerable<Message>> SendMessages(IEnumerable<Message> messages, string destinationQueueUrl, CancellationToken cancellationToken)
    {
        var messagesSent = new List<Message>();

        var batches = messages
            .Select((message, index) => new { message, index })
            .GroupBy(x => x.index / _options.SendMessageBatchSize, x => x.message);

        foreach (var batch in batches)
        {
            var sendEntries = batch.Select(message => new SendMessageBatchRequestEntry
            {
                Id = message.MessageId, // !
                MessageBody = message.Body,
                MessageAttributes = message.MessageAttributes
            });

            var sendRequest = new SendMessageBatchRequest
            {
                QueueUrl = destinationQueueUrl,
                Entries = sendEntries.ToList()
            };

            try
            {
                var sendResponse = await _sqsClient.SendMessageBatchAsync(sendRequest, cancellationToken);
                HandleResponse(sendResponse);

                var successfulIds = sendResponse.Successful.Select(s => s.Id).ToHashSet();
                messagesSent.AddRange(batch.Where(msg => successfulIds.Contains(msg.MessageId)));
            }
            catch (Exception ex)
            {
                _logger.LogError("Error sending message batch: {Exception}", ex);
            }
        }

        return messagesSent;

        void HandleResponse(SendMessageBatchResponse response)
        {
            response
                .Failed
                .ForEach(failed => _logger.LogError("Failed to send message w/ id {Id}: {Message}", failed.Id, failed.Message));

            response
                .Successful
                .ForEach(successful => _sourceQueueMessageIds.Add(successful.Id));
        }
    }

    private async Task DeleteMessagesFromSource(IEnumerable<Message> messages, string sourceQueueUrl, CancellationToken cancellationToken)
    {
        var batches = messages
            .Select((message, index) => new { message, index })
            .GroupBy(x => x.index / _options.DeleteMessageBatchSize, x => x.message);

        foreach (var batch in batches)
        {
            var deleteEntries = batch.Select(msg => new DeleteMessageBatchRequestEntry
            {
                Id = msg.MessageId,
                ReceiptHandle = msg.ReceiptHandle
            });

            var deleteRequest = new DeleteMessageBatchRequest
            {
                QueueUrl = sourceQueueUrl,
                Entries = deleteEntries.ToList()
            };

            try
            {
                var deleteResponse = await _sqsClient.DeleteMessageBatchAsync(deleteRequest, cancellationToken);
                HandleResponse(deleteResponse);
            }
            catch (Exception ex)
            {
                _logger.LogError("Error deleting message batch: {Exception}", ex);
            }
        }

        void HandleResponse(DeleteMessageBatchResponse response)
        {
            response
                .Failed
                .ForEach(failed => _logger.LogError("Failed to delete message w/ id {Id}: {Message}", failed.Id, failed.Message));
        }
    }
}
