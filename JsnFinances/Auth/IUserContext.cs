namespace JsnFinances.Api.Auth;

public interface IUserContext
{
    Guid GetUserId(HttpContext context);
    string? GetUserEmail(HttpContext context);
}
