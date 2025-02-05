using Amazon.SQS;
using Amazon.SQS.Model;
using Moq;
using q2q.Models;

namespace q2q.Tests;

public class q2qTests
{
    [Fact]
    public async Task Test1()
    {
        string sourceUrl = "//";
        string destUrl = "/";

        var x = new q2q();

        await x.ForwardMessages(sourceUrl, destUrl, default);
    }

    [Fact]
    public async Task ForwardMessages_ProcessesMessage()
    {
        // arrange

        string sourceQueueUrl = "https://sqs-1";
        string destinationQueueUrl = "https://sqs-2";

        var message = new Message
        {
            MessageId = "msg-1",
            Body = "Test msg",
            MessageAttributes = []
        };

        var receiveResponseWithMessage = new ReceiveMessageResponse
        {
            Messages = [message]
        };

        var emptyReceiveResponse = new ReceiveMessageResponse
        {
            Messages = []
        };

        var mockSqsClient = new Mock<IAmazonSQS>();

        mockSqsClient
            .SetupSequence(client => client.ReceiveMessageAsync(
                It.IsAny<ReceiveMessageRequest>(), 
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(receiveResponseWithMessage)
            .ReturnsAsync(emptyReceiveResponse);

        mockSqsClient
            .Setup(client => client.SendMessageAsync(
                It.IsAny<SendMessageRequest>(), 
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new SendMessageResponse());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        var q2q = new q2q(mockSqsClient.Object, new q2qOptions());

        // act

        await q2q.ForwardMessages(sourceQueueUrl, destinationQueueUrl, cts.Token);

        // assert

        mockSqsClient
            .Verify(client => client.SendMessageAsync(
                It.Is<SendMessageRequest>(req => req.QueueUrl == destinationQueueUrl && req.MessageBody == message.Body), 
                It.IsAny<CancellationToken>()), 
            Times.Once);
    }
}