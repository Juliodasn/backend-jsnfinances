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

    public async Task<MercadoPagoCreateSubscriptionResult> CreateAuthorizedSubscriptionAsync(
        BillingPlanDto plan,
        Guid userId,
        string payerEmail,
        string cardTokenId,
        string? meliSessionId)
    {
        EnsureConfigured();

        var backUrl = _configuration["Billing:CheckoutReturnUrl"];
        if (string.IsNullOrWhiteSpace(backUrl))
        {
            throw new InvalidOperationException("Configure Billing:CheckoutReturnUrl com a URL do frontend para retorno do Mercado Pago.");
        }

        var notificationUrl = _configuration["Billing:MercadoPago:NotificationUrl"];

        var payload = new Dictionary<string, object?>
        {
            ["reason"] = $"JSN Finances - Plano {plan.Nome}",
            ["external_reference"] = userId.ToString(),
            ["payer_email"] = payerEmail,
            ["card_token_id"] = cardTokenId,
            ["auto_recurring"] = new
            {
                frequency = plan.Frequency,
                frequency_type = plan.FrequencyType,
                transaction_amount = plan.Valor,
                currency_id = plan.Moeda
            },
            ["back_url"] = backUrl,
            ["status"] = "authorized"
        };

        if (!string.IsNullOrWhiteSpace(notificationUrl))
        {
            payload["notification_url"] = notificationUrl;
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, "preapproval")
        {
            Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
        };

        AddAuthorization(request);
        AddOptionalHeaders(request, meliSessionId);

        using var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Mercado Pago retornou erro ao criar assinatura: {(int)response.StatusCode} - {json}");
        }

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var id = GetString(root, "id");
        var initPoint = GetString(root, "init_point") ?? GetString(root, "sandbox_init_point");
        var status = GetString(root, "status") ?? "pending";

        if (string.IsNullOrWhiteSpace(id))
        {
            throw new InvalidOperationException("Mercado Pago não retornou o id da assinatura.");
        }

        return new MercadoPagoCreateSubscriptionResult(id, initPoint, status, json);
    }

    public async Task<MercadoPagoSubscriptionResult> GetSubscriptionAsync(string providerSubscriptionId)
    {
        EnsureConfigured();

        using var request = new HttpRequestMessage(HttpMethod.Get, $"preapproval/{Uri.EscapeDataString(providerSubscriptionId)}");
        AddAuthorization(request);

        using var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Mercado Pago retornou erro ao consultar assinatura: {(int)response.StatusCode} - {json}");
        }

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var id = GetString(root, "id") ?? providerSubscriptionId;
        var status = GetString(root, "status") ?? "pending";
        var initPoint = GetString(root, "init_point") ?? GetString(root, "sandbox_init_point");
        var externalReference = GetString(root, "external_reference");
        var nextPaymentDate = GetDateTimeOffset(root, "next_payment_date");
        var amount = root.TryGetProperty("auto_recurring", out var recurring)
            ? GetDecimal(recurring, "transaction_amount")
            : null;

        return new MercadoPagoSubscriptionResult(
            id,
            status,
            initPoint,
            externalReference,
            nextPaymentDate,
            amount,
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
        var status = GetString(root, "status");
        var externalReference = GetString(root, "external_reference");
        var amount = GetDecimal(root, "transaction_amount") ?? GetDecimal(root, "total_paid_amount");
        var currency = GetString(root, "currency_id");
        var paidAt = GetDateTimeOffset(root, "date_approved") ?? GetDateTimeOffset(root, "date_created");
        var preapprovalId = GetString(root, "preapproval_id")
            ?? ReadNestedString(root, "metadata", "preapproval_id")
            ?? ReadNestedString(root, "metadata", "provider_subscription_id");

        return new MercadoPagoPaymentResult(
            id,
            status,
            externalReference,
            preapprovalId,
            amount,
            currency,
            paidAt,
            json);
    }

    public async Task CancelSubscriptionAsync(string providerSubscriptionId)
    {
        EnsureConfigured();

        var payload = new { status = "cancelled" };
        using var request = new HttpRequestMessage(HttpMethod.Put, $"preapproval/{Uri.EscapeDataString(providerSubscriptionId)}")
        {
            Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
        };
        AddAuthorization(request);

        using var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Mercado Pago retornou erro ao cancelar assinatura: {(int)response.StatusCode} - {json}");
        }
    }

    private void AddAuthorization(HttpRequestMessage request)
    {
        var token = _configuration["Billing:MercadoPago:AccessToken"];
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
    }

    private void AddOptionalHeaders(HttpRequestMessage request, string? meliSessionId)
    {
        var xScope = _configuration["Billing:MercadoPago:XScope"];
        if (!string.IsNullOrWhiteSpace(xScope))
        {
            request.Headers.TryAddWithoutValidation("X-scope", xScope);
        }

        if (!string.IsNullOrWhiteSpace(meliSessionId))
        {
            request.Headers.TryAddWithoutValidation("X-meli-session-id", meliSessionId);
        }
    }

    private void EnsureConfigured()
    {
        var token = _configuration["Billing:MercadoPago:AccessToken"];
        if (string.IsNullOrWhiteSpace(token) || token.Contains("SEU_ACCESS_TOKEN", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Configure Billing:MercadoPago:AccessToken com seu Access Token do Mercado Pago.");
        }
    }

    private static string? ReadNestedString(JsonElement root, string objectPropertyName, string nestedPropertyName)
    {
        if (!root.TryGetProperty(objectPropertyName, out var nested) || nested.ValueKind != JsonValueKind.Object) return null;
        return GetString(nested, nestedPropertyName);
    }

    private static string? GetString(JsonElement root, string propertyName)
        => root.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null
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
}

public sealed record MercadoPagoCreateSubscriptionResult(
    string Id,
    string? InitPoint,
    string Status,
    string RawPayload);

public sealed record MercadoPagoSubscriptionResult(
    string Id,
    string Status,
    string? InitPoint,
    string? ExternalReference,
    DateTimeOffset? NextPaymentDate,
    decimal? Amount,
    string RawPayload);

public sealed record MercadoPagoPaymentResult(
    string Id,
    string? Status,
    string? ExternalReference,
    string? ProviderSubscriptionId,
    decimal? Amount,
    string? Currency,
    DateTimeOffset? PaidAt,
    string RawPayload);
