using Amazon.SQS;
using Amazon.SQS.Model;
using Moq;
using q2q.Options;

namespace q2q.Tests;

public class MessageRelayTests
{
    private readonly Mock<IAmazonSQS> _sqsClientMock;

    public MessageRelayTests()
    {
        _sqsClientMock = new Mock<IAmazonSQS>();
    }

    [Fact]
    public void Constructor_GivenNullSqsClient_ShouldThrowArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new MessageRelay(null!));
    }

    [Fact]
    public async Task ForwardMessages_GivenSingleMessageInSourceQueue_ShouldForwardMessage()
    {
        // arrange

        string sourceQueueUrl = "https://sqs-1";
        string destinationQueueUrl = "https://sqs-2";

        var testMessage = new Message
        {
            MessageId = "msg-1",
            Body = "Test msg",
            MessageAttributes = []
        };

        _sqsClientMock.SetupSequence(sqs => sqs.ReceiveMessageAsync(
                It.IsAny<ReceiveMessageRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = [testMessage]
            })
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = []
            });

        _sqsClientMock
            .Setup(sqs => sqs.SendMessageBatchAsync(
                It.IsAny<SendMessageBatchRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns((SendMessageBatchRequest request, CancellationToken token) =>
                Task.FromResult(CreateSuccessfulSendResponse(request, token)));

        var relay = new MessageRelay(_sqsClientMock.Object, new MessageRelayOptions());

        using var cts = new CancellationTokenSource(100);

        // act

        var forwardTask = relay.ForwardMessages(sourceQueueUrl, destinationQueueUrl, cts.Token);
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await forwardTask);

        // assert

        _sqsClientMock.Verify(sqs => sqs.SendMessageBatchAsync(
                It.Is<SendMessageBatchRequest>(req =>
                    req.QueueUrl == destinationQueueUrl &&
                    req.Entries.Any(e => e.Id == testMessage.MessageId)),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ForwardMessages_GivenDuplicateMessagesAndSuccessfulDelete_ShouldOnlyForwardOnce()
    {
        // arrange

        string sourceQueueUrl = "https://sqs-1";
        string destinationQueueUrl = "https://sqs-2";

        var testMessage = new Message
        {
            MessageId = "msg-1",
            Body = "Test msg",
            MessageAttributes = []
        };

        _sqsClientMock.SetupSequence(sqs => sqs.ReceiveMessageAsync(
                It.IsAny<ReceiveMessageRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = [testMessage]
            })
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = [testMessage]
            })
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = []
            });

        _sqsClientMock
            .Setup(sqs => sqs.SendMessageBatchAsync(
                It.IsAny<SendMessageBatchRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns((SendMessageBatchRequest request, CancellationToken token) =>
                Task.FromResult(CreateSuccessfulSendResponse(request, token)));

        _sqsClientMock
            .Setup(sqs => sqs.DeleteMessageBatchAsync(
                It.IsAny<DeleteMessageBatchRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns((DeleteMessageBatchRequest request, CancellationToken token) =>
                Task.FromResult(CreateSuccessfulDeleteResponse(request, token)));

        var relay = new MessageRelay(_sqsClientMock.Object, new MessageRelayOptions
        {
            DeleteMessagesFromSource = true
        });

        using var cts = new CancellationTokenSource(100);

        // act

        var forwardTask = relay.ForwardMessages(sourceQueueUrl, destinationQueueUrl, cts.Token);
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await forwardTask);

        // assert

        _sqsClientMock.Verify(sqs => sqs.SendMessageBatchAsync(
                It.Is<SendMessageBatchRequest>(req =>
                    req.QueueUrl == destinationQueueUrl &&
                    req.Entries.Any(e => e.Id == testMessage.MessageId)),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ForwardMessages_GivenDuplicateMessagesAndFailedDelete_ShouldForwardMoreThanOnce()
    {
        // arrange

        string sourceQueueUrl = "https://sqs-1";
        string destinationQueueUrl = "https://sqs-2";

        var testMessage = new Message
        {
            MessageId = "msg-1",
            ReceiptHandle = "receipt-1",
            Body = "Test msg",
            MessageAttributes = []
        };

        _sqsClientMock.SetupSequence(sqs => sqs.ReceiveMessageAsync(
                It.IsAny<ReceiveMessageRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = [testMessage]
            })
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = [testMessage]
            })
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = []
            });

        _sqsClientMock
            .Setup(sqs => sqs.SendMessageBatchAsync(
                It.IsAny<SendMessageBatchRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns((SendMessageBatchRequest request, CancellationToken token) =>
                Task.FromResult(CreateSuccessfulSendResponse(request, token)));

        _sqsClientMock
            .Setup(sqs => sqs.DeleteMessageBatchAsync(
                It.IsAny<DeleteMessageBatchRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteMessageBatchResponse
            {
                Failed =
                [
                    new BatchResultErrorEntry
                    {
                        Id = testMessage.MessageId,
                        Message = "delete failed"
                    }
                ]
            });

        var relay = new MessageRelay(_sqsClientMock.Object, new MessageRelayOptions
        {
            DeleteMessagesFromSource = true
        });

        using var cts = new CancellationTokenSource(100);

        // act

        var forwardTask = relay.ForwardMessages(sourceQueueUrl, destinationQueueUrl, cts.Token);
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await forwardTask);

        // assert

        _sqsClientMock.Verify(sqs => sqs.SendMessageBatchAsync(
                It.Is<SendMessageBatchRequest>(req =>
                    req.QueueUrl == destinationQueueUrl &&
                    req.Entries.Any(e => e.Id == testMessage.MessageId)),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    private static SendMessageBatchResponse CreateSuccessfulSendResponse(SendMessageBatchRequest request, CancellationToken token)
    {
        var response = new SendMessageBatchResponse();

        foreach (var entry in request.Entries)
        {
            response.Successful.Add(new SendMessageBatchResultEntry
            {
                Id = entry.Id
            });
        }

        return response;
    }

    private static DeleteMessageBatchResponse CreateSuccessfulDeleteResponse(DeleteMessageBatchRequest request, CancellationToken token)
    {
        var response = new DeleteMessageBatchResponse();

        foreach (var entry in request.Entries)
        {
            response.Successful.Add(new DeleteMessageBatchResultEntry
            {
                Id = entry.Id
            });
        }

        return response;
    }
}
