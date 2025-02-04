namespace q2q.Tests;

public class UnitTest1
{
    [Fact]
    public async Task Test1()
    {
        string sourceUrl = "//";
        string destUrl = "/";

        var x = new q2q();

        await x.ForwardMessages(sourceUrl, destUrl, default);
    }
}