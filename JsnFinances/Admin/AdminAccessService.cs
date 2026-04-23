using JsnFinances.Api.Auth;
using JsnFinances.Api.Data;
using JsnFinances.Api.Domain;

namespace JsnFinances.Api.Admin;

public sealed class AdminAccessService
{
    private readonly IConfiguration _configuration;
    private readonly JsnFinancesDb _db;

    public AdminAccessService(IConfiguration configuration, JsnFinancesDb db)
    {
        _configuration = configuration;
        _db = db;
    }

    public IReadOnlyList<string> AllowedEmails => ReadAllowedEmails();

    public async Task<AdminIdentityDto> RequireAdminAsync(HttpContext context, IUserContext userContext)
    {
        var userId = userContext.GetUserId(context);
        var tokenEmail = NormalizeEmail(userContext.GetUserEmail(context));
        var allowedEmails = AllowedEmails.Select(NormalizeEmail).Where(x => !string.IsNullOrWhiteSpace(x)).ToArray();

        if (allowedEmails.Length == 0)
        {
            throw new ForbiddenAccessException("Nenhum e-mail administrador foi configurado no backend.");
        }

        if (string.IsNullOrWhiteSpace(tokenEmail) || !allowedEmails.Contains(tokenEmail, StringComparer.OrdinalIgnoreCase))
        {
            throw new ForbiddenAccessException("Acesso negado. Esta conta não tem permissão para acessar o Portal Admin.");
        }

        var requireVerifiedEmailRaw = _configuration["Admin:RequireVerifiedEmail"];
        var requireVerifiedEmail = !bool.TryParse(requireVerifiedEmailRaw, out var parsedRequireVerifiedEmail) || parsedRequireVerifiedEmail;
        var verification = await _db.GetAdminUserVerificationAsync(userId, tokenEmail);

        if (verification is null)
        {
            throw new ForbiddenAccessException("Acesso negado. Usuário administrador não encontrado no Supabase Auth.");
        }

        if (!string.Equals(verification.Email, tokenEmail, StringComparison.OrdinalIgnoreCase))
        {
            throw new ForbiddenAccessException("Acesso negado. O e-mail do token não confere com o usuário autenticado no Supabase.");
        }

        if (requireVerifiedEmail && !verification.EmailConfirmed)
        {
            throw new ForbiddenAccessException("Acesso negado. Confirme o e-mail administrador antes de acessar o Portal Admin.");
        }

        await _db.InsertAdminAccessLogAsync(
            userId,
            tokenEmail,
            context.Request.Path.Value ?? string.Empty,
            context.Request.Method,
            context.Connection.RemoteIpAddress?.ToString(),
            context.Request.Headers["User-Agent"].FirstOrDefault());

        return new AdminIdentityDto(
            userId,
            tokenEmail,
            true,
            verification.EmailConfirmed,
            allowedEmails,
            requireVerifiedEmail);
    }

    private IReadOnlyList<string> ReadAllowedEmails()
    {
        var fromSection = _configuration.GetSection("Admin:AllowedEmails")
            .GetChildren()
            .Select(x => x.Value)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Cast<string>()
            .ToList();

        var fromEnv = _configuration["ADMIN_ALLOWED_EMAILS"];
        if (!string.IsNullOrWhiteSpace(fromEnv))
        {
            fromSection.AddRange(fromEnv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));
        }

        return fromSection
            .Select(NormalizeEmail)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string NormalizeEmail(string? email)
        => (email ?? string.Empty).Trim().ToLowerInvariant();
}
