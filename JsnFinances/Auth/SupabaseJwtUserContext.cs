using System.Net.Http.Headers;
using System.Text.Json;

namespace JsnFinances.Api.Auth;

public sealed class SupabaseJwtUserContext : IUserContext
{
    private const string ContextCacheKey = "__SUPABASE_VERIFIED_USER__";
    private static readonly HttpClient Http = new();

    private readonly IConfiguration _configuration;

    public SupabaseJwtUserContext(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public Guid GetUserId(HttpContext context)
    {
        var user = GetOrVerifyUser(context);

        if (!Guid.TryParse(user.Id, out var userId))
        {
            throw new UnauthorizedAccessException("Token inválido: usuário do Supabase não é um UUID válido.");
        }

        return userId;
    }

    public string? GetUserEmail(HttpContext context)
    {
        return GetOrVerifyUser(context).Email;
    }

    private SupabaseVerifiedUser GetOrVerifyUser(HttpContext context)
    {
        if (context.Items.TryGetValue(ContextCacheKey, out var cached) &&
            cached is SupabaseVerifiedUser cachedUser)
        {
            return cachedUser;
        }

        var token = GetBearerToken(context);
        var projectUrl = GetRequiredConfig("Supabase:ProjectUrl");
        var publishableKey =
            _configuration["Supabase:PublishableKey"] ??
            _configuration["Supabase:AnonKey"] ??
            _configuration["Supabase:ApiKey"];

        if (string.IsNullOrWhiteSpace(publishableKey) ||
            publishableKey.Contains("SUA_", StringComparison.OrdinalIgnoreCase) ||
            publishableKey.Contains("SEU_", StringComparison.OrdinalIgnoreCase))
        {
            throw new UnauthorizedAccessException("Supabase:PublishableKey não configurado no backend.");
        }

        var url = $"{projectUrl.TrimEnd('/')}/auth/v1/user";

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Headers.TryAddWithoutValidation("apikey", publishableKey);

        using var response = Http.Send(request);

        if (!response.IsSuccessStatusCode)
        {
            throw new UnauthorizedAccessException("Token Supabase inválido ou sessão não reconhecida pelo Supabase Auth.");
        }

        var json = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        var id = root.TryGetProperty("id", out var idProperty)
            ? idProperty.GetString()
            : null;

        var email = root.TryGetProperty("email", out var emailProperty)
            ? emailProperty.GetString()
            : null;

        if (string.IsNullOrWhiteSpace(id))
        {
            throw new UnauthorizedAccessException("Token Supabase válido, mas usuário não retornou ID.");
        }

        var verifiedUser = new SupabaseVerifiedUser(id, email);
        context.Items[ContextCacheKey] = verifiedUser;

        return verifiedUser;
    }

    private string GetRequiredConfig(string key)
    {
        var value = _configuration[key];

        if (string.IsNullOrWhiteSpace(value) ||
            value.Contains("SUA_", StringComparison.OrdinalIgnoreCase) ||
            value.Contains("SEU_", StringComparison.OrdinalIgnoreCase))
        {
            throw new UnauthorizedAccessException($"{key} não configurado no backend.");
        }

        return value;
    }

    private static string GetBearerToken(HttpContext context)
    {
        var authorization = context.Request.Headers.Authorization.FirstOrDefault();

        if (string.IsNullOrWhiteSpace(authorization) ||
            !authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            throw new UnauthorizedAccessException("Sessão não identificada. Envie o token Supabase no header Authorization: Bearer <token>.");
        }

        return authorization["Bearer ".Length..].Trim();
    }

    private sealed record SupabaseVerifiedUser(string Id, string? Email);
}
