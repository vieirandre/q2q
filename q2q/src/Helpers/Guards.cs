namespace q2q.Helpers;

public static class Guards
{
    public static int EnsureInRange(int value, string parameterName, int min, int max)
    {
        if (value < min || value > max)
            throw new ArgumentOutOfRangeException(parameterName, value, $"{parameterName} must be between {min} and {max}");

        return value;
    }
}