using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using JsnFinances.Api.Domain;

namespace JsnFinances.Api.Billing;

public sealed class MercadoPagoClient
{
    private readonly HttpClient _httpClient;
    private readonly IConfiguration _configuration;

    public MercadoPagoClient(HttpClient httpClient, IConfiguration configuration)
    {
        _httpClient = httpClient;
        _configuration = configuration;
    }

    public async Task<MercadoPagoCreatePixPaymentResult> CreatePixPaymentAsync(
        BillingPlanDto plan,
        Guid userId,
        Guid internalChargeId,
        string payerEmail)
    {
        EnsureConfigured();

        var notificationUrl = _configuration["Billing:MercadoPago:NotificationUrl"];
        var expirationMinutes = int.TryParse(_configuration["Billing:PixExpirationMinutes"], out var configuredMinutes)
            ? Math.Clamp(configuredMinutes, 10, 1440)
            : 60;
        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(expirationMinutes);

        var payload = new Dictionary<string, object?>
        {
            ["transaction_amount"] = plan.Valor,
            ["description"] = $"JSN Finances - {plan.Nome}",
            ["payment_method_id"] = "pix",
            ["external_reference"] = internalChargeId.ToString(),
            ["date_of_expiration"] = expiresAt.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK"),
            ["payer"] = new { email = payerEmail },
            ["metadata"] = new Dictionary<string, object?>
            {
                ["user_id"] = userId.ToString(),
                ["plan_code"] = plan.Code,
                ["internal_charge_id"] = internalChargeId.ToString()
            }
        };

        if (!string.IsNullOrWhiteSpace(notificationUrl))
        {
            payload["notification_url"] = notificationUrl;
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, "v1/payments")
        {
            Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
        };
        request.Headers.TryAddWithoutValidation("X-Idempotency-Key", internalChargeId.ToString());
        AddAuthorization(request);

        using var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Mercado Pago retornou erro ao criar PIX: {(int)response.StatusCode} - {json}");
        }

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var id = GetString(root, "id");
        var status = NormalizePaymentStatus(GetString(root, "status"));
        var externalReference = GetString(root, "external_reference") ?? internalChargeId.ToString();
        var dateOfExpiration = GetDateTimeOffset(root, "date_of_expiration") ?? expiresAt;
        var paidAt = GetDateTimeOffset(root, "date_approved");
        var transactionData = root.TryGetProperty("point_of_interaction", out var poi)
            && poi.TryGetProperty("transaction_data", out var data)
                ? data
                : default;
        var qrCode = transactionData.ValueKind == JsonValueKind.Object ? GetString(transactionData, "qr_code") : null;
        var qrCodeBase64 = transactionData.ValueKind == JsonValueKind.Object ? GetString(transactionData, "qr_code_base64") : null;
        var ticketUrl = transactionData.ValueKind == JsonValueKind.Object ? GetString(transactionData, "ticket_url") : null;

        if (string.IsNullOrWhiteSpace(id))
        {
            throw new InvalidOperationException("Mercado Pago não retornou o id da cobrança PIX.");
        }

        return new MercadoPagoCreatePixPaymentResult(
            id,
            status,
            externalReference,
            qrCode,
            qrCodeBase64,
            ticketUrl,
            dateOfExpiration,
            paidAt,
            json);
    }

    public async Task<MercadoPagoPaymentResult> GetPaymentAsync(string providerPaymentId)
    {
        EnsureConfigured();

        using var request = new HttpRequestMessage(HttpMethod.Get, $"v1/payments/{Uri.EscapeDataString(providerPaymentId)}");
        AddAuthorization(request);

        using var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Mercado Pago retornou erro ao consultar pagamento: {(int)response.StatusCode} - {json}");
        }

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var id = GetString(root, "id") ?? providerPaymentId;
        var status = NormalizePaymentStatus(GetString(root, "status"));
        var externalReference = GetString(root, "external_reference");
        var amount = GetDecimal(root, "transaction_amount") ?? GetDecimal(root, "total_paid_amount");
        var currency = GetString(root, "currency_id");
        var paidAt = GetDateTimeOffset(root, "date_approved") ?? GetDateTimeOffset(root, "date_created");

        return new MercadoPagoPaymentResult(
            id,
            status,
            externalReference,
            amount,
            currency,
            paidAt,
            json);
    }

    private void AddAuthorization(HttpRequestMessage request)
    {
        var token = _configuration["Billing:MercadoPago:AccessToken"];
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
    }

    private void EnsureConfigured()
    {
        var token = _configuration["Billing:MercadoPago:AccessToken"];
        if (string.IsNullOrWhiteSpace(token) || token.Contains("SEU_ACCESS_TOKEN", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Configure Billing:MercadoPago:AccessToken com seu Access Token do Mercado Pago.");
        }
    }

    private static string? GetString(JsonElement root, string propertyName)
        => root.ValueKind == JsonValueKind.Object && root.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null
            ? property.ToString()
            : null;

    private static decimal? GetDecimal(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var property) || property.ValueKind == JsonValueKind.Null) return null;
        if (property.ValueKind == JsonValueKind.Number && property.TryGetDecimal(out var value)) return value;
        return decimal.TryParse(property.ToString(), System.Globalization.NumberStyles.Number, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static DateTimeOffset? GetDateTimeOffset(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var property) || property.ValueKind == JsonValueKind.Null) return null;
        return DateTimeOffset.TryParse(property.ToString(), out var value) ? value : null;
    }

    private static string NormalizePaymentStatus(string? status)
    {
        var normalized = (status ?? string.Empty).Trim().ToLowerInvariant();
        return normalized switch
        {
            "approved" or "authorized" or "active" => "approved",
            "pending" or "pending_payment" or "in_process" => "pending",
            "cancelled" or "canceled" or "cancel" => "canceled",
            "rejected" => "rejected",
            "expired" => "expired",
            _ => string.IsNullOrWhiteSpace(normalized) ? "pending" : normalized
        };
    }
}

public sealed record MercadoPagoCreatePixPaymentResult(
    string Id,
    string Status,
    string ExternalReference,
    string? QrCode,
    string? QrCodeBase64,
    string? TicketUrl,
    DateTimeOffset? ExpiresAt,
    DateTimeOffset? PaidAt,
    string RawPayload);

public sealed record MercadoPagoPaymentResult(
    string Id,
    string? Status,
    string? ExternalReference,
    decimal? Amount,
    string? Currency,
    DateTimeOffset? PaidAt,
    string RawPayload);
