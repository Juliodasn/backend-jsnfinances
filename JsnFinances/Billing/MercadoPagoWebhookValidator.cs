using System.Security.Cryptography;
using System.Text;

namespace JsnFinances.Api.Billing;

public sealed class MercadoPagoWebhookValidator
{
    private readonly IConfiguration _configuration;

    public MercadoPagoWebhookValidator(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public bool IsSignatureRequired
    {
        get
        {
            var raw = _configuration["Billing:MercadoPago:RequireWebhookSignature"];
            return !bool.TryParse(raw, out var required) || required;
        }
    }

    public bool Validate(HttpContext context, string? dataId)
    {
        var secret = _configuration["Billing:MercadoPago:WebhookSecret"];
        if (string.IsNullOrWhiteSpace(secret) || secret.Contains("SEU_WEBHOOK_SECRET", StringComparison.OrdinalIgnoreCase))
        {
            return !IsSignatureRequired;
        }

        var xSignature = context.Request.Headers["x-signature"].FirstOrDefault();
        var xRequestId = context.Request.Headers["x-request-id"].FirstOrDefault();

        if (string.IsNullOrWhiteSpace(xSignature) || string.IsNullOrWhiteSpace(xRequestId) || string.IsNullOrWhiteSpace(dataId))
        {
            return false;
        }

        string? ts = null;
        string? v1 = null;
        foreach (var part in xSignature.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var pieces = part.Split('=', 2, StringSplitOptions.TrimEntries);
            if (pieces.Length != 2) continue;

            if (pieces[0] == "ts") ts = pieces[1];
            if (pieces[0] == "v1") v1 = pieces[1];
        }

        if (string.IsNullOrWhiteSpace(ts) || string.IsNullOrWhiteSpace(v1)) return false;

        var manifest = $"id:{dataId};request-id:{xRequestId};ts:{ts};";
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(manifest));
        var expectedHex = Convert.ToHexString(hash).ToLowerInvariant();

        return string.Equals(expectedHex, v1, StringComparison.OrdinalIgnoreCase);
    }
}
