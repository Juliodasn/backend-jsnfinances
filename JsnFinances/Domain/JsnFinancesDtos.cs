using System.Text.Json.Serialization;

namespace JsnFinances.Api.Domain;

public sealed record MovimentacaoDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("descricao")] string Descricao,
    [property: JsonPropertyName("categoria")] string? Categoria,
    [property: JsonPropertyName("valor")] decimal Valor,
    [property: JsonPropertyName("data_movimentacao")] DateOnly DataMovimentacao,
    [property: JsonPropertyName("observacao")] string? Observacao,
    [property: JsonPropertyName("id_conta")] Guid? IdConta = null
);

public sealed record CategoriaDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("nome")] string Nome,
    [property: JsonPropertyName("tipo")] string Tipo,
    [property: JsonPropertyName("descricao")] string? Descricao,
    [property: JsonPropertyName("cor")] string? Cor,
    [property: JsonPropertyName("icone")] string? Icone,
    [property: JsonPropertyName("ativo")] bool Ativo
);

public sealed record MetaDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("titulo")] string Titulo,
    [property: JsonPropertyName("categoria")] string? Categoria,
    [property: JsonPropertyName("descricao")] string? Descricao,
    [property: JsonPropertyName("valor_objetivo")] decimal ValorObjetivo,
    [property: JsonPropertyName("valor_guardado")] decimal ValorGuardado,
    [property: JsonPropertyName("data_limite")] DateOnly? DataLimite,
    [property: JsonPropertyName("cor")] string? Cor,
    [property: JsonPropertyName("icone")] string? Icone
);

public sealed record OrcamentoDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("categoria")] string Categoria,
    [property: JsonPropertyName("limite_mensal")] decimal LimiteMensal
);

public sealed record ContaDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("nome")] string Nome,
    [property: JsonPropertyName("tipo")] string Tipo,
    [property: JsonPropertyName("saldo_inicial")] decimal SaldoInicial,
    [property: JsonPropertyName("cor")] string? Cor,
    [property: JsonPropertyName("ativo")] bool Ativo
);

public sealed record TransferenciaDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("id_conta_origem")] Guid IdContaOrigem,
    [property: JsonPropertyName("id_conta_destino")] Guid IdContaDestino,
    [property: JsonPropertyName("valor")] decimal Valor,
    [property: JsonPropertyName("data_transferencia")] DateOnly DataTransferencia,
    [property: JsonPropertyName("observacao")] string? Observacao
);

public sealed record MovimentoRequest(
    string Reason,
    string? Category,
    decimal Value,
    DateOnly Date,
    string? Notes,
    Guid? AccountId = null
);

public sealed record ParcelamentoRequest(
    string Reason,
    string? Category,
    decimal Value,
    DateOnly Date,
    string? Notes,
    int Parcelas,
    Guid? AccountId = null
);

public sealed record CategoriaRequest(string Nome, string Tipo, string? Descricao, string? Cor, string? Icone, bool Ativo = true);
public sealed record MetaRequest(string Titulo, string? Categoria, string? Descricao, decimal ValorObjetivo, decimal ValorGuardado, DateOnly? DataLimite, string? Cor, string? Icone);
public sealed record GuardarValorMetaRequest(decimal Valor);
public sealed record OrcamentoRequest(string Categoria, decimal LimiteMensal);
public sealed record ContaRequest(string Nome, string Tipo, decimal SaldoInicial, string? Cor, bool Ativo = true);
public sealed record TransferenciaRequest(Guid IdContaOrigem, Guid IdContaDestino, decimal Valor, DateOnly DataTransferencia, string? Observacao);

public sealed record MovimentacoesResponse(IReadOnlyList<MovimentacaoDto> Entradas, IReadOnlyList<MovimentacaoDto> Saidas);
public sealed record CategoryTotalDto(string Categoria, decimal Total, decimal Percentual);
public sealed record MonthlyTotalDto(int Ano, int Mes, decimal Entradas, decimal Saidas, decimal Saldo);
public sealed record ReportResumoDto(
    DateOnly DataInicio,
    DateOnly DataFim,
    decimal Entradas,
    decimal Saidas,
    decimal Saldo,
    IReadOnlyList<CategoryTotalDto> Categorias,
    IReadOnlyList<MonthlyTotalDto> Meses
);

public sealed record InsightDto(string Titulo, string Mensagem, string Tom);


public sealed record MovimentacoesQuery(
    DateOnly? Inicio,
    DateOnly? Fim,
    int Limit = 1000,
    int Offset = 0
);


public sealed record ValidationIssueDto(
    [property: JsonPropertyName("campo")] string Campo,
    [property: JsonPropertyName("mensagem")] string Mensagem
);

public sealed record ValidationResultDto(
    [property: JsonPropertyName("valido")] bool Valido,
    [property: JsonPropertyName("mensagem")] string Mensagem,
    [property: JsonPropertyName("erros")] IReadOnlyList<ValidationIssueDto> Erros
);

public sealed record ParcelaPreviewDto(
    [property: JsonPropertyName("numero")] int Numero,
    [property: JsonPropertyName("total")] int Total,
    [property: JsonPropertyName("descricao")] string Descricao,
    [property: JsonPropertyName("valor")] decimal Valor,
    [property: JsonPropertyName("data")] DateOnly Data,
    [property: JsonPropertyName("observacao")] string? Observacao
);

public sealed record DashboardResumoDto(
    [property: JsonPropertyName("data_inicio")] DateOnly DataInicio,
    [property: JsonPropertyName("data_fim")] DateOnly DataFim,
    [property: JsonPropertyName("entradas")] decimal Entradas,
    [property: JsonPropertyName("saidas")] decimal Saidas,
    [property: JsonPropertyName("saldo")] decimal Saldo,
    [property: JsonPropertyName("transacoes")] int Transacoes,
    [property: JsonPropertyName("maior_entrada")] decimal MaiorEntrada,
    [property: JsonPropertyName("maior_saida")] decimal MaiorSaida,
    [property: JsonPropertyName("media_diaria")] decimal MediaDiaria
);

public sealed record OrcamentoUsoDto(
    [property: JsonPropertyName("categoria")] string Categoria,
    [property: JsonPropertyName("limite")] decimal Limite,
    [property: JsonPropertyName("usado")] decimal Usado,
    [property: JsonPropertyName("restante")] decimal Restante,
    [property: JsonPropertyName("percentual")] decimal Percentual,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("mensagem")] string Mensagem
);

public sealed record MetaResumoDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("titulo")] string Titulo,
    [property: JsonPropertyName("valor_objetivo")] decimal ValorObjetivo,
    [property: JsonPropertyName("valor_guardado")] decimal ValorGuardado,
    [property: JsonPropertyName("valor_restante")] decimal ValorRestante,
    [property: JsonPropertyName("progresso")] decimal Progresso,
    [property: JsonPropertyName("status")] string Status
);



public sealed record ContaSaldoDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("nome")] string Nome,
    [property: JsonPropertyName("tipo")] string Tipo,
    [property: JsonPropertyName("saldo_inicial")] decimal SaldoInicial,
    [property: JsonPropertyName("cor")] string? Cor,
    [property: JsonPropertyName("ativo")] bool Ativo,
    [property: JsonPropertyName("total_entradas")] decimal TotalEntradas,
    [property: JsonPropertyName("total_saidas")] decimal TotalSaidas,
    [property: JsonPropertyName("total_transferencias_entrada")] decimal TotalTransferenciasEntrada,
    [property: JsonPropertyName("total_transferencias_saida")] decimal TotalTransferenciasSaida,
    [property: JsonPropertyName("saldo_atual")] decimal SaldoAtual
);

public sealed record VisaoAnualMesDto(
    [property: JsonPropertyName("ano")] int Ano,
    [property: JsonPropertyName("mes")] int Mes,
    [property: JsonPropertyName("entradas")] decimal Entradas,
    [property: JsonPropertyName("saidas")] decimal Saidas,
    [property: JsonPropertyName("saldo")] decimal Saldo,
    [property: JsonPropertyName("quantidade_entradas")] int QuantidadeEntradas,
    [property: JsonPropertyName("quantidade_saidas")] int QuantidadeSaidas
);

public sealed record CalendarioMovimentacaoDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("tipo")] string Tipo,
    [property: JsonPropertyName("descricao")] string Descricao,
    [property: JsonPropertyName("categoria")] string? Categoria,
    [property: JsonPropertyName("valor")] decimal Valor,
    [property: JsonPropertyName("data_movimentacao")] DateOnly DataMovimentacao,
    [property: JsonPropertyName("observacao")] string? Observacao,
    [property: JsonPropertyName("id_conta")] Guid? IdConta
);



public sealed record PagedResultDto<T>(
    [property: JsonPropertyName("items")] IReadOnlyList<T> Items,
    [property: JsonPropertyName("total")] int Total,
    [property: JsonPropertyName("page")] int Page,
    [property: JsonPropertyName("page_size")] int PageSize,
    [property: JsonPropertyName("total_pages")] int TotalPages
);

public sealed record TransacaoPaginadaDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("tipo")] string Tipo,
    [property: JsonPropertyName("descricao")] string Descricao,
    [property: JsonPropertyName("categoria")] string? Categoria,
    [property: JsonPropertyName("valor")] decimal Valor,
    [property: JsonPropertyName("data_movimentacao")] DateOnly DataMovimentacao,
    [property: JsonPropertyName("observacao")] string? Observacao,
    [property: JsonPropertyName("id_conta")] Guid? IdConta
);



public sealed record PerfilDto(
    [property: JsonPropertyName("id_usuario")] Guid IdUsuario,
    [property: JsonPropertyName("nome_completo")] string? NomeCompleto,
    [property: JsonPropertyName("data_nascimento")] DateOnly? DataNascimento,
    [property: JsonPropertyName("telefone")] string? Telefone,
    [property: JsonPropertyName("url_avatar")] string? UrlAvatar
);

public sealed record PerfilRequest(
    [property: JsonPropertyName("nome_completo")] string? NomeCompleto,
    [property: JsonPropertyName("data_nascimento")] DateOnly? DataNascimento,
    [property: JsonPropertyName("telefone")] string? Telefone,
    [property: JsonPropertyName("url_avatar")] string? UrlAvatar
);

public sealed record PreferenciasUsuarioDto(
    [property: JsonPropertyName("tema")] string Tema,
    [property: JsonPropertyName("cor_principal")] string CorPrincipal,
    [property: JsonPropertyName("moeda")] string Moeda,
    [property: JsonPropertyName("idioma")] string Idioma,
    [property: JsonPropertyName("modo_nucleo")] bool ModoNucleo,
    [property: JsonPropertyName("filtro_ano")] string? FiltroAno,
    [property: JsonPropertyName("filtro_mes")] string? FiltroMes,
    [property: JsonPropertyName("filtro_inicio")] DateOnly? FiltroInicio,
    [property: JsonPropertyName("filtro_fim")] DateOnly? FiltroFim,
    [property: JsonPropertyName("helpers_ocultos")] string[] HelpersOcultos,
    [property: JsonPropertyName("helpers_vistos")] string[] HelpersVistos
);

public sealed record PreferenciasUsuarioRequest(
    [property: JsonPropertyName("tema")] string? Tema,
    [property: JsonPropertyName("cor_principal")] string? CorPrincipal,
    [property: JsonPropertyName("moeda")] string? Moeda,
    [property: JsonPropertyName("idioma")] string? Idioma,
    [property: JsonPropertyName("modo_nucleo")] bool? ModoNucleo,
    [property: JsonPropertyName("filtro_ano")] string? FiltroAno,
    [property: JsonPropertyName("filtro_mes")] string? FiltroMes,
    [property: JsonPropertyName("filtro_inicio")] DateOnly? FiltroInicio,
    [property: JsonPropertyName("filtro_fim")] DateOnly? FiltroFim,
    [property: JsonPropertyName("helpers_ocultos")] string[]? HelpersOcultos,
    [property: JsonPropertyName("helpers_vistos")] string[]? HelpersVistos
);


public sealed record BillingPlanDto(
    [property: JsonPropertyName("code")] string Code,
    [property: JsonPropertyName("nome")] string Nome,
    [property: JsonPropertyName("descricao")] string Descricao,
    [property: JsonPropertyName("valor")] decimal Valor,
    [property: JsonPropertyName("moeda")] string Moeda,
    [property: JsonPropertyName("frequency")] int Frequency,
    [property: JsonPropertyName("frequency_type")] string FrequencyType,
    [property: JsonPropertyName("destaque")] bool Destaque
);

public sealed record BillingStatusDto(
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("has_access")] bool HasAccess,
    [property: JsonPropertyName("is_trial")] bool IsTrial,
    [property: JsonPropertyName("trial_started_at")] DateTimeOffset? TrialStartedAt,
    [property: JsonPropertyName("trial_ends_at")] DateTimeOffset? TrialEndsAt,
    [property: JsonPropertyName("current_period_end")] DateTimeOffset? CurrentPeriodEnd,
    [property: JsonPropertyName("trial_days_left")] int TrialDaysLeft,
    [property: JsonPropertyName("plan_code")] string? PlanCode,
    [property: JsonPropertyName("provider_subscription_id")] string? ProviderSubscriptionId,
    [property: JsonPropertyName("init_point")] string? InitPoint,
    [property: JsonPropertyName("message")] string Message
);


public sealed record CreatePixChargeRequest(
    [property: JsonPropertyName("plan_code")] string PlanCode,
    [property: JsonPropertyName("payer_email")] string? PayerEmail = null
);

public sealed record PixChargeDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("user_id")] Guid UserId,
    [property: JsonPropertyName("plan_code")] string PlanCode,
    [property: JsonPropertyName("amount")] decimal Amount,
    [property: JsonPropertyName("currency")] string Currency,
    [property: JsonPropertyName("mercado_pago_payment_id")] string? MercadoPagoPaymentId,
    [property: JsonPropertyName("mercado_pago_external_reference")] string MercadoPagoExternalReference,
    [property: JsonPropertyName("payment_status")] string PaymentStatus,
    [property: JsonPropertyName("qr_code")] string? QrCode,
    [property: JsonPropertyName("qr_code_base64")] string? QrCodeBase64,
    [property: JsonPropertyName("pix_copy_paste")] string? PixCopyPaste,
    [property: JsonPropertyName("ticket_url")] string? TicketUrl,
    [property: JsonPropertyName("expires_at")] DateTimeOffset? ExpiresAt,
    [property: JsonPropertyName("paid_at")] DateTimeOffset? PaidAt,
    [property: JsonPropertyName("access_until")] DateTimeOffset? AccessUntil,
    [property: JsonPropertyName("created_at")] DateTimeOffset CreatedAt,
    [property: JsonPropertyName("updated_at")] DateTimeOffset UpdatedAt
);

public sealed record AdminIdentityDto(
    [property: JsonPropertyName("id_usuario")] Guid IdUsuario,
    [property: JsonPropertyName("email")] string Email,
    [property: JsonPropertyName("is_admin")] bool IsAdmin,
    [property: JsonPropertyName("email_confirmed")] bool EmailConfirmed,
    [property: JsonPropertyName("allowed_emails")] IReadOnlyList<string> AllowedEmails,
    [property: JsonPropertyName("require_verified_email")] bool RequireVerifiedEmail
);

public sealed record AdminUserVerificationDto(
    [property: JsonPropertyName("id_usuario")] Guid IdUsuario,
    [property: JsonPropertyName("email")] string Email,
    [property: JsonPropertyName("email_confirmed")] bool EmailConfirmed
);

public sealed record AdminDashboardDto(
    [property: JsonPropertyName("total_users")] int TotalUsers,
    [property: JsonPropertyName("trialing_users")] int TrialingUsers,
    [property: JsonPropertyName("active_subscriptions")] int ActiveSubscriptions,
    [property: JsonPropertyName("pending_subscriptions")] int PendingSubscriptions,
    [property: JsonPropertyName("expired_trials")] int ExpiredTrials,
    [property: JsonPropertyName("canceled_subscriptions")] int CanceledSubscriptions,
    [property: JsonPropertyName("total_payments")] int TotalPayments,
    [property: JsonPropertyName("approved_payments")] int ApprovedPayments,
    [property: JsonPropertyName("revenue_total")] decimal RevenueTotal,
    [property: JsonPropertyName("monthly_recurring_revenue")] decimal MonthlyRecurringRevenue,
    [property: JsonPropertyName("annual_recurring_revenue")] decimal AnnualRecurringRevenue,
    [property: JsonPropertyName("trial_to_paid_conversion")] decimal TrialToPaidConversion,
    [property: JsonPropertyName("average_usage_days")] decimal AverageUsageDays,
    [property: JsonPropertyName("last_billing_event_at")] DateTimeOffset? LastBillingEventAt,
    [property: JsonPropertyName("generated_at")] DateTimeOffset GeneratedAt
);

public sealed record AdminUserDto(
    [property: JsonPropertyName("id_usuario")] Guid IdUsuario,
    [property: JsonPropertyName("nome")] string? Nome,
    [property: JsonPropertyName("email")] string Email,
    [property: JsonPropertyName("created_at")] DateTimeOffset CreatedAt,
    [property: JsonPropertyName("last_sign_in_at")] DateTimeOffset? LastSignInAt,
    [property: JsonPropertyName("email_confirmed")] bool EmailConfirmed,
    [property: JsonPropertyName("subscription_status")] string SubscriptionStatus,
    [property: JsonPropertyName("plan_code")] string? PlanCode,
    [property: JsonPropertyName("trial_started_at")] DateTimeOffset? TrialStartedAt,
    [property: JsonPropertyName("trial_ends_at")] DateTimeOffset? TrialEndsAt,
    [property: JsonPropertyName("current_period_end")] DateTimeOffset? CurrentPeriodEnd,
    [property: JsonPropertyName("last_activity_at")] DateTimeOffset? LastActivityAt,
    [property: JsonPropertyName("days_using")] int DaysUsing,
    [property: JsonPropertyName("total_entries")] int TotalEntries,
    [property: JsonPropertyName("total_exits")] int TotalExits,
    [property: JsonPropertyName("total_categories")] int TotalCategories,
    [property: JsonPropertyName("total_goals")] int TotalGoals
);

public sealed record AdminSubscriptionDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("id_usuario")] Guid IdUsuario,
    [property: JsonPropertyName("nome")] string? Nome,
    [property: JsonPropertyName("email")] string? Email,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("plan_code")] string? PlanCode,
    [property: JsonPropertyName("plan_name")] string? PlanName,
    [property: JsonPropertyName("plan_value")] decimal? PlanValue,
    [property: JsonPropertyName("provider")] string Provider,
    [property: JsonPropertyName("provider_subscription_id")] string? ProviderSubscriptionId,
    [property: JsonPropertyName("provider_payer_email")] string? ProviderPayerEmail,
    [property: JsonPropertyName("trial_started_at")] DateTimeOffset? TrialStartedAt,
    [property: JsonPropertyName("trial_ends_at")] DateTimeOffset? TrialEndsAt,
    [property: JsonPropertyName("current_period_end")] DateTimeOffset? CurrentPeriodEnd,
    [property: JsonPropertyName("last_synced_at")] DateTimeOffset? LastSyncedAt,
    [property: JsonPropertyName("created_at")] DateTimeOffset CreatedAt,
    [property: JsonPropertyName("updated_at")] DateTimeOffset UpdatedAt
);

public sealed record AdminPaymentDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("id_usuario")] Guid IdUsuario,
    [property: JsonPropertyName("nome")] string? Nome,
    [property: JsonPropertyName("email")] string? Email,
    [property: JsonPropertyName("provider")] string Provider,
    [property: JsonPropertyName("provider_payment_id")] string? ProviderPaymentId,
    [property: JsonPropertyName("provider_subscription_id")] string? ProviderSubscriptionId,
    [property: JsonPropertyName("status")] string? Status,
    [property: JsonPropertyName("amount")] decimal? Amount,
    [property: JsonPropertyName("currency")] string? Currency,
    [property: JsonPropertyName("paid_at")] DateTimeOffset? PaidAt,
    [property: JsonPropertyName("created_at")] DateTimeOffset CreatedAt
);

public sealed record AdminBillingEventDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("provider")] string Provider,
    [property: JsonPropertyName("event_type")] string EventType,
    [property: JsonPropertyName("event_action")] string? EventAction,
    [property: JsonPropertyName("resource_id")] string? ResourceId,
    [property: JsonPropertyName("request_id")] string? RequestId,
    [property: JsonPropertyName("signature_valid")] bool SignatureValid,
    [property: JsonPropertyName("processed_at")] DateTimeOffset ProcessedAt
);

public sealed record AdminAccessLogDto(
    [property: JsonPropertyName("id")] Guid Id,
    [property: JsonPropertyName("id_usuario")] Guid IdUsuario,
    [property: JsonPropertyName("email")] string Email,
    [property: JsonPropertyName("path")] string Path,
    [property: JsonPropertyName("method")] string Method,
    [property: JsonPropertyName("ip_address")] string? IpAddress,
    [property: JsonPropertyName("user_agent")] string? UserAgent,
    [property: JsonPropertyName("accessed_at")] DateTimeOffset AccessedAt
);


public sealed record DashboardComparisonDto(
    [property: JsonPropertyName("titulo")] string Titulo,
    [property: JsonPropertyName("periodo_anterior_label")] string PeriodoAnteriorLabel,
    [property: JsonPropertyName("entradas_percentual")] string EntradasPercentual,
    [property: JsonPropertyName("entradas_texto")] string EntradasTexto,
    [property: JsonPropertyName("saidas_percentual")] string SaidasPercentual,
    [property: JsonPropertyName("saidas_texto")] string SaidasTexto,
    [property: JsonPropertyName("saldo_percentual")] string SaldoPercentual,
    [property: JsonPropertyName("saldo_texto")] string SaldoTexto,
    [property: JsonPropertyName("insight")] string Insight
);

public sealed record DashboardSmartInsightDto(
    [property: JsonPropertyName("icon")] string Icon,
    [property: JsonPropertyName("title")] string Title,
    [property: JsonPropertyName("text")] string Text,
    [property: JsonPropertyName("tone")] string Tone
);

public sealed record DashboardHomeDto(
    [property: JsonPropertyName("resumo")] DashboardResumoDto Resumo,
    [property: JsonPropertyName("resumo_anterior")] DashboardResumoDto ResumoAnterior,
    [property: JsonPropertyName("comparacao")] DashboardComparisonDto Comparacao,
    [property: JsonPropertyName("saldo_total_contas")] decimal SaldoTotalContas,
    [property: JsonPropertyName("ultimas_entradas")] PagedResultDto<MovimentacaoDto> UltimasEntradas,
    [property: JsonPropertyName("ultimas_saidas")] PagedResultDto<MovimentacaoDto> UltimasSaidas,
    [property: JsonPropertyName("saldo_mensal")] IReadOnlyList<VisaoAnualMesDto> SaldoMensal,
    [property: JsonPropertyName("smart_insights")] IReadOnlyList<DashboardSmartInsightDto> SmartInsights,
    [property: JsonPropertyName("insight_periodo_label")] string InsightPeriodoLabel
);

public sealed record RelatorioSummaryDto(
    [property: JsonPropertyName("entradas")] decimal Entradas,
    [property: JsonPropertyName("saidas")] decimal Saidas,
    [property: JsonPropertyName("saldo")] decimal Saldo,
    [property: JsonPropertyName("media_diaria")] decimal MediaDiaria,
    [property: JsonPropertyName("total_transacoes")] int TotalTransacoes
);

public sealed record RelatorioCategoriaDto(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("value")] decimal Value,
    [property: JsonPropertyName("percentage")] decimal Percentage
);

public sealed record RelatorioBucketDto(
    [property: JsonPropertyName("label")] string Label,
    [property: JsonPropertyName("entries")] decimal Entries,
    [property: JsonPropertyName("exits")] decimal Exits,
    [property: JsonPropertyName("balance")] decimal Balance
);

public sealed record RelatoriosOverviewDto(
    [property: JsonPropertyName("summary_current")] RelatorioSummaryDto SummaryCurrent,
    [property: JsonPropertyName("summary_previous")] RelatorioSummaryDto SummaryPrevious,
    [property: JsonPropertyName("categories")] IReadOnlyList<RelatorioCategoriaDto> Categories,
    [property: JsonPropertyName("latest_transactions")] IReadOnlyList<TransacaoPaginadaDto> LatestTransactions,
    [property: JsonPropertyName("buckets")] IReadOnlyList<RelatorioBucketDto> Buckets,
    [property: JsonPropertyName("annual_rows")] IReadOnlyList<VisaoAnualMesDto> AnnualRows,
    [property: JsonPropertyName("donut_total")] decimal DonutTotal
);
