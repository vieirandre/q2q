using Amazon.SQS;
using Amazon.SQS.Model;
using Moq;
using q2q.Options;

namespace q2q.Tests;

public class MessageRelayTests
{
    private readonly Mock<IAmazonSQS> _sqsClientMock;
    private readonly Mock<MessageRelayOptions> _options;

    public MessageRelayTests()
    {
        _sqsClientMock = new Mock<IAmazonSQS>();
        _options = new Mock<MessageRelayOptions>();
    }

    //[Fact]
    //public async Task Test1()
    //{
    //    string sourceUrl = "//";
    //    string destUrl = "/";

    //    var x = new MessageRelay();

    //    await x.ForwardMessages(sourceUrl, destUrl, default);
    //}

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

        _sqsClientMock.Setup(sqs => sqs.SendMessageBatchAsync(
                It.IsAny<SendMessageBatchRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((SendMessageBatchRequest request, CancellationToken token) =>
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
            });

        var relay = new MessageRelay(_sqsClientMock.Object, _options.Object);

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
    public async Task ForwardMessages_GivenDuplicateMessages_ShouldOnlyForwardOnce()
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

        _sqsClientMock.Setup(sqs => sqs.SendMessageBatchAsync(
                It.IsAny<SendMessageBatchRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((SendMessageBatchRequest request, CancellationToken token) =>
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
            });

        var relay = new MessageRelay(_sqsClientMock.Object, _options.Object);

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
}