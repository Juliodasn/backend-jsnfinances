namespace JsnFinances.Api.Admin;

public sealed class ForbiddenAccessException : Exception
{
    public ForbiddenAccessException(string message) : base(message)
    {
    }
}
