using Amazon.SQS;
using Amazon.SQS.Model;
using Moq;
using q2q.Models;

namespace q2q.Tests;

public class q2qTests
{
    private readonly Mock<IAmazonSQS> _sqsClientMock;
    private readonly Mock<q2qOptions> _options;

    public q2qTests()
    {
        _sqsClientMock = new Mock<IAmazonSQS>();
        _options = new Mock<q2qOptions>();
    }

    //[Fact]
    //public async Task Test1()
    //{
    //    string sourceUrl = "//";
    //    string destUrl = "/";

    //    var x = new q2q();

    //    await x.ForwardMessages(sourceUrl, destUrl, default);
    //}

    [Fact]
    public async Task ForwardMessages_ForwardsNewMessages()
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

        var q2q = new q2q(_sqsClientMock.Object, _options.Object);

        using var cts = new CancellationTokenSource(200);

        // act

        var forwardTask = q2q.ForwardMessages(sourceQueueUrl, destinationQueueUrl, cts.Token);
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