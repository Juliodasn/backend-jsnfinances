using JsnFinances.Api.Data;
using JsnFinances.Api.Domain;

namespace JsnFinances.Api.Services;

public sealed class JsnFinancesRulesService
{
    private readonly JsnFinancesDb _db;

    public JsnFinancesRulesService(JsnFinancesDb db)
    {
        _db = db;
    }

    public async Task<IReadOnlyList<MovimentacaoDto>> CriarSaidaParceladaAsync(Guid userId, ParcelamentoRequest request)
    {
        if (request.Parcelas < 2 || request.Parcelas > 60)
        {
            throw new ArgumentException("A quantidade de parcelas deve ficar entre 2 e 60.");
        }

        if (request.Value <= 0)
        {
            throw new ArgumentException("O valor total da despesa deve ser maior que zero.");
        }

        var parcelas = BuildParcelas(request).ToList();
        await _db.InsertMovimentacoesAsync("saidas", userId, parcelas);
        return await _db.ListMovimentacoesAsync("saidas", userId);
    }

    public async Task<ReportResumoDto> CriarResumoAsync(Guid userId, DateOnly? inicio, DateOnly? fim)
    {
        var dataFim = fim ?? DateOnly.FromDateTime(DateTime.Today);
        var dataInicio = inicio ?? new DateOnly(dataFim.Year, dataFim.Month, 1);

        if (dataInicio > dataFim)
        {
            (dataInicio, dataFim) = (dataFim, dataInicio);
        }

        var entradasPeriodo = await _db.ListMovimentacoesAsync("entradas", userId, dataInicio, dataFim, 2000, 0);
        var saidasPeriodo = await _db.ListMovimentacoesAsync("saidas", userId, dataInicio, dataFim, 2000, 0);

        var totalEntradas = entradasPeriodo.Sum(x => x.Valor);
        var totalSaidas = saidasPeriodo.Sum(x => x.Valor);
        var categorias = entradasPeriodo.Concat(saidasPeriodo)
            .GroupBy(x => string.IsNullOrWhiteSpace(x.Categoria) ? "Outros" : x.Categoria!)
            .Select(g => new CategoryTotalDto(g.Key, g.Sum(x => x.Valor), Percent(g.Sum(x => x.Valor), totalEntradas + totalSaidas)))
            .OrderByDescending(x => x.Total)
            .ToList();

        var meses = entradasPeriodo.Select(x => x.DataMovimentacao).Concat(saidasPeriodo.Select(x => x.DataMovimentacao))
            .Select(x => new { x.Year, x.Month })
            .Distinct()
            .OrderBy(x => x.Year)
            .ThenBy(x => x.Month)
            .Select(x =>
            {
                var entradaMes = entradasPeriodo.Where(e => e.DataMovimentacao.Year == x.Year && e.DataMovimentacao.Month == x.Month).Sum(e => e.Valor);
                var saidaMes = saidasPeriodo.Where(s => s.DataMovimentacao.Year == x.Year && s.DataMovimentacao.Month == x.Month).Sum(s => s.Valor);
                return new MonthlyTotalDto(x.Year, x.Month, entradaMes, saidaMes, entradaMes - saidaMes);
            })
            .ToList();

        return new ReportResumoDto(dataInicio, dataFim, totalEntradas, totalSaidas, totalEntradas - totalSaidas, categorias, meses);
    }

    public async Task<IReadOnlyList<InsightDto>> CriarInsightsAsync(Guid userId, DateOnly? inicio, DateOnly? fim)
    {
        var resumo = await CriarResumoAsync(userId, inicio, fim);
        var metas = await _db.ListMetasAsync(userId);
        var insights = new List<InsightDto>();

        var maiorCategoria = resumo.Categorias.OrderByDescending(x => x.Total).FirstOrDefault();
        if (maiorCategoria is not null)
        {
            insights.Add(new InsightDto("Maior movimentação por categoria", $"A categoria {maiorCategoria.Categoria} movimentou {maiorCategoria.Total:C} no período.", "info"));
        }

        insights.Add(new InsightDto(
            "Saldo do período",
            resumo.Saldo >= 0
                ? $"Seu saldo está positivo em {resumo.Saldo:C}."
                : $"Seu saldo está negativo em {Math.Abs(resumo.Saldo):C}.",
            resumo.Saldo >= 0 ? "positive" : "negative"));

        var metaDestaque = metas
            .Where(x => x.ValorObjetivo > 0)
            .OrderByDescending(x => x.ValorGuardado / x.ValorObjetivo)
            .FirstOrDefault();

        if (metaDestaque is not null)
        {
            var progresso = Math.Min(metaDestaque.ValorGuardado / metaDestaque.ValorObjetivo * 100, 100);
            insights.Add(new InsightDto("Meta em destaque", $"Você está a {progresso:0}% da meta {metaDestaque.Titulo}.", progresso >= 80 ? "positive" : "info"));
        }

        return insights;
    }


    public ValidationResultDto ValidarMovimento(MovimentoRequest request)
    {
        var erros = new List<ValidationIssueDto>();

        if (string.IsNullOrWhiteSpace(request.Reason))
        {
            erros.Add(new ValidationIssueDto("descricao", "Informe uma descrição para o lançamento."));
        }

        if (request.Value <= 0)
        {
            erros.Add(new ValidationIssueDto("valor", "O valor precisa ser maior que zero."));
        }

        if (request.Date == default)
        {
            erros.Add(new ValidationIssueDto("data", "Informe uma data válida."));
        }

        if (request.Date > DateOnly.FromDateTime(DateTime.Today.AddYears(5)))
        {
            erros.Add(new ValidationIssueDto("data", "A data está muito distante no futuro."));
        }

        if (request.Category is not null && request.Category.Length > 80)
        {
            erros.Add(new ValidationIssueDto("categoria", "A categoria deve ter no máximo 80 caracteres."));
        }

        var valido = erros.Count == 0;
        return new ValidationResultDto(
            valido,
            valido ? "Lançamento válido." : "Corrija os campos destacados antes de salvar.",
            erros);
    }

    public IReadOnlyList<ParcelaPreviewDto> CriarPreviewParcelamento(ParcelamentoRequest request)
    {
        if (request.Parcelas < 2 || request.Parcelas > 60)
        {
            throw new ArgumentException("A quantidade de parcelas deve ficar entre 2 e 60.");
        }

        if (request.Value <= 0)
        {
            throw new ArgumentException("O valor total da despesa deve ser maior que zero.");
        }

        return BuildParcelas(request)
            .Select((parcela, index) => new ParcelaPreviewDto(
                index + 1,
                request.Parcelas,
                parcela.Reason,
                parcela.Value,
                parcela.Date,
                parcela.Notes))
            .ToList();
    }

    public async Task<DashboardResumoDto> CriarDashboardResumoAsync(Guid userId, DateOnly? inicio, DateOnly? fim)
    {
        var dataFim = fim ?? DateOnly.FromDateTime(DateTime.Today);
        var dataInicio = inicio ?? new DateOnly(dataFim.Year, dataFim.Month, 1);

        if (dataInicio > dataFim)
        {
            (dataInicio, dataFim) = (dataFim, dataInicio);
        }

        var entradas = await _db.ListMovimentacoesAsync("entradas", userId, dataInicio, dataFim, 2000, 0);
        var saidas = await _db.ListMovimentacoesAsync("saidas", userId, dataInicio, dataFim, 2000, 0);

        var totalEntradas = entradas.Sum(x => x.Valor);
        var totalSaidas = saidas.Sum(x => x.Valor);
        var dias = Math.Max(1, dataFim.DayNumber - dataInicio.DayNumber + 1);

        return new DashboardResumoDto(
            dataInicio,
            dataFim,
            totalEntradas,
            totalSaidas,
            totalEntradas - totalSaidas,
            entradas.Count + saidas.Count,
            entradas.Count == 0 ? 0 : entradas.Max(x => x.Valor),
            saidas.Count == 0 ? 0 : saidas.Max(x => x.Valor),
            Math.Round((totalEntradas - totalSaidas) / dias, 2));
    }

    public async Task<IReadOnlyList<OrcamentoUsoDto>> CriarUsoOrcamentosAsync(Guid userId, DateOnly? inicio, DateOnly? fim)
    {
        var dataFim = fim ?? DateOnly.FromDateTime(DateTime.Today);
        var dataInicio = inicio ?? new DateOnly(dataFim.Year, dataFim.Month, 1);

        if (dataInicio > dataFim)
        {
            (dataInicio, dataFim) = (dataFim, dataInicio);
        }

        var orcamentos = await _db.ListOrcamentosAsync(userId);
        var saidas = await _db.ListMovimentacoesAsync("saidas", userId, dataInicio, dataFim, 2000, 0);

        return orcamentos
            .Select(orcamento =>
            {
                var usado = saidas
                    .Where(saida => string.Equals(saida.Categoria ?? "Outros", orcamento.Categoria, StringComparison.OrdinalIgnoreCase))
                    .Sum(saida => saida.Valor);

                var restante = Math.Max(orcamento.LimiteMensal - usado, 0);
                var percentual = Percent(usado, orcamento.LimiteMensal);
                var status = usado > orcamento.LimiteMensal
                    ? "exceeded"
                    : percentual >= 80
                        ? "warning"
                        : "ok";

                var mensagem = status switch
                {
                    "exceeded" => $"Você ultrapassou {Math.Abs(orcamento.LimiteMensal - usado):C} no orçamento de {orcamento.Categoria}.",
                    "warning" => $"Você já usou {percentual:0}% do orçamento de {orcamento.Categoria}.",
                    _ => $"Orçamento de {orcamento.Categoria} dentro do limite."
                };

                return new OrcamentoUsoDto(orcamento.Categoria, orcamento.LimiteMensal, usado, restante, percentual, status, mensagem);
            })
            .OrderByDescending(x => x.Percentual)
            .ToList();
    }

    public async Task<IReadOnlyList<MetaResumoDto>> CriarResumoMetasAsync(Guid userId)
    {
        var metas = await _db.ListMetasAsync(userId);

        return metas
            .Select(meta =>
            {
                var restante = Math.Max(meta.ValorObjetivo - meta.ValorGuardado, 0);
                var progresso = meta.ValorObjetivo <= 0 ? 0 : Math.Min(meta.ValorGuardado / meta.ValorObjetivo * 100, 100);
                var status = progresso >= 100
                    ? "completed"
                    : progresso >= 80
                        ? "near"
                        : "active";

                return new MetaResumoDto(meta.Id, meta.Titulo, meta.ValorObjetivo, meta.ValorGuardado, restante, Math.Round(progresso, 2), status);
            })
            .OrderByDescending(x => x.Progresso)
            .ToList();
    }


    public async Task<IReadOnlyList<VisaoAnualMesDto>> CriarVisaoAnualAsync(Guid userId, int? ano)
    {
        var year = ano ?? DateTime.Today.Year;
        var inicio = new DateOnly(year, 1, 1);
        var fim = new DateOnly(year, 12, 31);

        var entradas = await _db.ListMovimentacoesAsync("entradas", userId, inicio, fim, 2000, 0);
        var saidas = await _db.ListMovimentacoesAsync("saidas", userId, inicio, fim, 2000, 0);

        return Enumerable.Range(1, 12)
            .Select(mes =>
            {
                var entradasMes = entradas.Where(x => x.DataMovimentacao.Month == mes).ToList();
                var saidasMes = saidas.Where(x => x.DataMovimentacao.Month == mes).ToList();
                var totalEntradas = entradasMes.Sum(x => x.Valor);
                var totalSaidas = saidasMes.Sum(x => x.Valor);

                return new VisaoAnualMesDto(
                    year,
                    mes,
                    totalEntradas,
                    totalSaidas,
                    totalEntradas - totalSaidas,
                    entradasMes.Count,
                    saidasMes.Count);
            })
            .ToList();
    }

    public async Task<IReadOnlyList<CalendarioMovimentacaoDto>> CriarCalendarioMensalAsync(Guid userId, int? ano, int? mes, DateOnly? dataInicio = null, DateOnly? dataFim = null)
    {
        DateOnly inicio;
        DateOnly fim;

        if (dataInicio.HasValue && dataFim.HasValue)
        {
            inicio = dataInicio.Value;
            fim = dataFim.Value;

            if (inicio > fim)
            {
                (inicio, fim) = (fim, inicio);
            }
        }
        else
        {
            var hoje = DateTime.Today;
            var year = ano ?? hoje.Year;
            var month = mes ?? hoje.Month;

            if (month < 1 || month > 12)
            {
                throw new ArgumentException("Informe um mês entre 1 e 12.");
            }

            inicio = new DateOnly(year, month, 1);
            fim = inicio.AddMonths(1).AddDays(-1);
        }

        return await _db.ListCalendarioMovimentacoesAsync(userId, inicio, fim);
    }

    public async Task<DashboardHomeDto> CriarDashboardHomeAsync(Guid userId, DateOnly? inicio, DateOnly? fim)
    {
        var (dataInicio, dataFim) = NormalizeRange(inicio, fim);
        var (inicioAnterior, fimAnterior) = GetPreviousRange(dataInicio, dataFim);

        var resumoAtualTask = CriarDashboardResumoAsync(userId, dataInicio, dataFim);
        var resumoAnteriorTask = CriarDashboardResumoAsync(userId, inicioAnterior, fimAnterior);
        var ultimasEntradasTask = _db.ListMovimentacoesPagedAsync("entradas", userId, dataInicio, dataFim, 1, 5);
        var ultimasSaidasTask = _db.ListMovimentacoesPagedAsync("saidas", userId, dataInicio, dataFim, 1, 5);
        var saldoMensalTask = CriarVisaoAnualAsync(userId, dataInicio.Year);
        var contasTask = _db.ListContasSaldoAsync(userId);
        var backendInsightsTask = CriarInsightsAsync(userId, dataInicio, dataFim);
        var currentEntriesTask = _db.ListMovimentacoesAsync("entradas", userId, dataInicio, dataFim, 2000, 0);
        var currentExitsTask = _db.ListMovimentacoesAsync("saidas", userId, dataInicio, dataFim, 2000, 0);
        var previousExitsTask = _db.ListMovimentacoesAsync("saidas", userId, inicioAnterior, fimAnterior, 2000, 0);
        var metasTask = _db.ListMetasAsync(userId);
        var orcamentosTask = _db.ListOrcamentosAsync(userId);

        await Task.WhenAll(
            resumoAtualTask,
            resumoAnteriorTask,
            ultimasEntradasTask,
            ultimasSaidasTask,
            saldoMensalTask,
            contasTask,
            backendInsightsTask,
            currentEntriesTask,
            currentExitsTask,
            previousExitsTask,
            metasTask,
            orcamentosTask);

        var resumoAtual = await resumoAtualTask;
        var resumoAnterior = await resumoAnteriorTask;
        var saldoMensal = await saldoMensalTask;
        var smartInsights = BuildDashboardSmartInsights(
            await backendInsightsTask,
            await currentEntriesTask,
            await currentExitsTask,
            await previousExitsTask,
            await metasTask,
            await orcamentosTask,
            saldoMensal,
            dataInicio,
            dataFim);

        return new DashboardHomeDto(
            resumoAtual,
            resumoAnterior,
            BuildDashboardComparison(dataInicio, dataFim, resumoAtual, resumoAnterior),
            (await contasTask)
                .Where(conta => conta.Ativo)
                .Sum(conta => conta.SaldoAtual),
            await ultimasEntradasTask,
            await ultimasSaidasTask,
            saldoMensal,
            smartInsights,
            BuildInsightPeriodLabel(dataInicio, dataFim));
    }

    public async Task<RelatoriosOverviewDto> CriarRelatoriosOverviewAsync(Guid userId, DateOnly? inicio, DateOnly? fim)
    {
        var (dataInicio, dataFim) = NormalizeRange(inicio, fim);
        var (inicioAnterior, fimAnterior) = GetPreviousRange(dataInicio, dataFim);

        var summaryCurrentTask = CriarDashboardResumoAsync(userId, dataInicio, dataFim);
        var summaryPreviousTask = CriarDashboardResumoAsync(userId, inicioAnterior, fimAnterior);
        var annualRowsTask = CriarVisaoAnualAsync(userId, dataInicio.Year);
        var transacoesTask = _db.ListTransacoesPagedAsync(userId, dataInicio, dataFim, 1, 5000);

        await Task.WhenAll(summaryCurrentTask, summaryPreviousTask, annualRowsTask, transacoesTask);

        var summaryCurrent = ToRelatorioSummary(await summaryCurrentTask);
        var summaryPrevious = ToRelatorioSummary(await summaryPreviousTask);
        var annualRows = await annualRowsTask;
        var transacoes = (await transacoesTask).Items;

        var totalCategorias = transacoes.Sum(item => item.Valor);
        var categories = transacoes
            .GroupBy(item => string.IsNullOrWhiteSpace(item.Categoria) ? "Outros" : item.Categoria!)
            .Select(group => new RelatorioCategoriaDto(
                group.Key,
                group.Sum(item => item.Valor),
                Percent(group.Sum(item => item.Valor), totalCategorias)))
            .OrderByDescending(item => item.Value)
            .ToList();

        var latest = transacoes
            .OrderByDescending(item => item.DataMovimentacao)
            .ThenByDescending(item => item.Id)
            .Take(8)
            .ToList();

        var buckets = IsWholeYearRange(dataInicio, dataFim)
            ? annualRows.Select(item => new RelatorioBucketDto(
                GetShortMonthName(item.Mes),
                item.Entradas,
                item.Saidas,
                item.Saldo)).ToList()
            : BuildReportBuckets(transacoes, dataInicio, dataFim);

        return new RelatoriosOverviewDto(
            summaryCurrent,
            summaryPrevious,
            categories,
            latest,
            buckets,
            annualRows,
            categories.Sum(item => item.Value));
    }

    private static (DateOnly Inicio, DateOnly Fim) NormalizeRange(DateOnly? inicio, DateOnly? fim)
    {
        var dataFim = fim ?? DateOnly.FromDateTime(DateTime.Today);
        var dataInicio = inicio ?? new DateOnly(dataFim.Year, dataFim.Month, 1);

        if (dataInicio > dataFim)
        {
            (dataInicio, dataFim) = (dataFim, dataInicio);
        }

        return (dataInicio, dataFim);
    }

    private static (DateOnly Inicio, DateOnly Fim) GetPreviousRange(DateOnly inicio, DateOnly fim)
    {
        var totalDias = Math.Max(1, fim.DayNumber - inicio.DayNumber + 1);
        var fimAnterior = inicio.AddDays(-1);
        var inicioAnterior = fimAnterior.AddDays(-(totalDias - 1));
        return (inicioAnterior, fimAnterior);
    }

    private DashboardComparisonDto BuildDashboardComparison(DateOnly inicio, DateOnly fim, DashboardResumoDto atual, DashboardResumoDto anterior)
    {
        var isCustom = !IsWholeMonthRange(inicio, fim) && !IsWholeYearRange(inicio, fim);
        var label = isCustom ? "período anterior" : IsWholeYearRange(inicio, fim) ? "ano anterior" : "mês anterior";
        var titulo = isCustom
            ? "Intervalo selecionado vs período anterior"
            : IsWholeYearRange(inicio, fim)
                ? $"Ano {inicio.Year} vs ano anterior"
                : $"{GetMonthName(inicio.Month)} de {inicio.Year} vs mês anterior";

        return new DashboardComparisonDto(
            titulo,
            label,
            FormatSmartComparisonPercent(atual.Entradas, anterior.Entradas),
            GetSmartComparisonText(atual.Entradas, anterior.Entradas, label),
            FormatSmartComparisonPercent(atual.Saidas, anterior.Saidas),
            GetSmartComparisonText(atual.Saidas, anterior.Saidas, label),
            FormatSmartComparisonPercent(atual.Saldo, anterior.Saldo),
            GetSmartComparisonText(atual.Saldo, anterior.Saldo, label),
            BuildSmartComparisonInsight(atual, anterior, label));
    }

    private IReadOnlyList<DashboardSmartInsightDto> BuildDashboardSmartInsights(
        IReadOnlyList<InsightDto> backendInsights,
        IReadOnlyList<MovimentacaoDto> currentEntries,
        IReadOnlyList<MovimentacaoDto> currentExits,
        IReadOnlyList<MovimentacaoDto> previousExits,
        IReadOnlyList<MetaDto> metas,
        IReadOnlyList<OrcamentoDto> orcamentos,
        IReadOnlyList<VisaoAnualMesDto> annualRows,
        DateOnly inicio,
        DateOnly fim)
    {
        var insights = backendInsights
            .Take(2)
            .Where(item => !string.IsNullOrWhiteSpace(item.Mensagem))
            .Select(item => new DashboardSmartInsightDto(
                GetBackendInsightIcon(item.Tom),
                item.Titulo ?? "Insight",
                item.Mensagem,
                NormalizeDashboardTone(item.Tom)))
            .ToList();

        var totalEntries = currentEntries.Sum(item => item.Valor);
        var totalExits = currentExits.Sum(item => item.Valor);
        var balance = totalEntries - totalExits;
        var topExpense = currentExits
            .GroupBy(item => string.IsNullOrWhiteSpace(item.Categoria) ? "Outros" : item.Categoria!)
            .Select(group => new { Categoria = group.Key, Total = group.Sum(item => item.Valor) })
            .OrderByDescending(item => item.Total)
            .FirstOrDefault();
        var growth = GetCategoryGrowthInsight(currentExits, previousExits);
        var annualBalance = annualRows.Sum(item => item.Saldo);
        var bestGoal = metas
            .Where(meta => meta.ValorObjetivo > 0)
            .OrderByDescending(GetGoalProgress)
            .FirstOrDefault();
        var budgetAlert = GetBudgetAlertInsight(orcamentos, currentExits);

        if (topExpense is not null)
        {
            insights.Add(new DashboardSmartInsightDto(
                "💸",
                "Maior gasto do período",
                $"Seu maior gasto foi {topExpense.Categoria}, com {topExpense.Total:C} no período selecionado.",
                "negative"));
        }

        if (growth.HasValue)
        {
            var growthValue = growth.Value;
            insights.Add(new DashboardSmartInsightDto(
                "📈",
                "Categoria em alta",
                $"Você gastou {growthValue.Item2:0.0}% a mais em {growthValue.Item1} em relação à base anterior.",
                "warning"));
        }
        else if (totalExits > 0)
        {
            insights.Add(new DashboardSmartInsightDto(
                "✅",
                "Gastos sob controle",
                "Nenhuma categoria teve aumento relevante em relação à base anterior.",
                "positive"));
        }

        insights.Add(new DashboardSmartInsightDto(
            annualBalance >= 0 ? "🟢" : "🔴",
            $"Saldo anual de {inicio.Year}",
            $"Seu saldo anual está {(annualBalance >= 0 ? "positivo" : "negativo")} em {Math.Abs(annualBalance):C}.",
            annualBalance >= 0 ? "positive" : "negative"));

        if (bestGoal is not null)
        {
            var progress = GetGoalProgress(bestGoal);
            insights.Add(new DashboardSmartInsightDto(
                "🎯",
                "Meta em destaque",
                $"Você está a {Math.Round(progress)}% da meta {bestGoal.Titulo}. Falta {Math.Max(bestGoal.ValorObjetivo - bestGoal.ValorGuardado, 0):C}.",
                progress >= 80 ? "positive" : "neutral"));
        }

        if (budgetAlert is not null)
        {
            insights.Add(budgetAlert);
        }

        if (!insights.Any())
        {
            insights.Add(new DashboardSmartInsightDto(
                "✨",
                "Tudo pronto para insights",
                "Cadastre entradas, saídas e metas para o JSN Finances gerar análises mais inteligentes.",
                "neutral"));
        }

        if (balance < 0 && !insights.Any(item => item.Title == "Atenção ao saldo"))
        {
            insights.Insert(0, new DashboardSmartInsightDto(
                "🔴",
                "Atenção ao saldo",
                $"No período selecionado, as saídas superaram as entradas em {Math.Abs(balance):C}.",
                "negative"));
        }

        return insights.Take(6).ToList();
    }

    private static DashboardSmartInsightDto? GetBudgetAlertInsight(
        IReadOnlyList<OrcamentoDto> orcamentos,
        IReadOnlyList<MovimentacaoDto> currentExits)
    {
        var alert = orcamentos
            .Select(orcamento =>
            {
                var limite = orcamento.LimiteMensal;
                var gasto = currentExits
                    .Where(saida => string.Equals(saida.Categoria ?? "Outros", orcamento.Categoria, StringComparison.OrdinalIgnoreCase))
                    .Sum(saida => saida.Valor);
                var percent = limite > 0 ? gasto / limite * 100 : 0;
                return new { orcamento.Categoria, Limite = limite, Gasto = gasto, Percent = percent };
            })
            .Where(item => item.Limite > 0 && item.Percent >= 80)
            .OrderByDescending(item => item.Percent)
            .FirstOrDefault();

        if (alert is null)
        {
            return null;
        }

        var exceeded = alert.Percent >= 100;
        return new DashboardSmartInsightDto(
            exceeded ? "⚠️" : "🔔",
            "Alerta de orçamento",
            exceeded
                ? $"A categoria {alert.Categoria} ultrapassou o orçamento em {(alert.Gasto - alert.Limite):C}."
                : $"A categoria {alert.Categoria} já usou {alert.Percent:0.0}% do orçamento mensal.",
            exceeded ? "negative" : "warning");
    }

    private static (string Category, decimal Percent, decimal Diff)? GetCategoryGrowthInsight(
        IReadOnlyList<MovimentacaoDto> currentExits,
        IReadOnlyList<MovimentacaoDto> previousExits)
    {
        var currentGrouped = currentExits
            .GroupBy(item => string.IsNullOrWhiteSpace(item.Categoria) ? "Outros" : item.Categoria!)
            .ToDictionary(group => group.Key, group => group.Sum(item => item.Valor));
        var previousGrouped = previousExits
            .GroupBy(item => string.IsNullOrWhiteSpace(item.Categoria) ? "Outros" : item.Categoria!)
            .ToDictionary(group => group.Key, group => group.Sum(item => item.Valor));

        var growth = currentGrouped
            .Select(item =>
            {
                var previousValue = previousGrouped.TryGetValue(item.Key, out var value) ? value : 0;
                var diff = item.Value - previousValue;
                var percent = previousValue > 0 ? diff / previousValue * 100 : (item.Value > 0 ? 100 : 0);
                return new { item.Key, Percent = percent, Diff = diff };
            })
            .Where(item => item.Diff > 0)
            .OrderByDescending(item => item.Diff)
            .FirstOrDefault();

        return growth is null ? null : (growth.Key, Math.Round(growth.Percent, 1), growth.Diff);
    }

    private static RelatorioSummaryDto ToRelatorioSummary(DashboardResumoDto resumo)
        => new(resumo.Entradas, resumo.Saidas, resumo.Saldo, resumo.MediaDiaria, resumo.Transacoes);

    private IReadOnlyList<RelatorioBucketDto> BuildReportBuckets(
        IReadOnlyList<TransacaoPaginadaDto> rows,
        DateOnly inicio,
        DateOnly fim)
    {
        var totalDias = Math.Max(1, fim.DayNumber - inicio.DayNumber + 1);
        var bucketCount = Math.Min(6, totalDias);
        var diasPorBucket = (int)Math.Ceiling(totalDias / (double)bucketCount);
        var buckets = new List<RelatorioBucketDto>();

        for (var index = 0; index < bucketCount; index++)
        {
            var bucketStart = inicio.AddDays(index * diasPorBucket);
            var rawEnd = bucketStart.AddDays(diasPorBucket - 1);
            var bucketEnd = rawEnd > fim ? fim : rawEnd;
            var entries = rows
                .Where(item => item.Tipo == "entrada" && item.DataMovimentacao >= bucketStart && item.DataMovimentacao <= bucketEnd)
                .Sum(item => item.Valor);
            var exits = rows
                .Where(item => item.Tipo == "saida" && item.DataMovimentacao >= bucketStart && item.DataMovimentacao <= bucketEnd)
                .Sum(item => item.Valor);

            buckets.Add(new RelatorioBucketDto(
                $"{bucketEnd:dd/MM}",
                entries,
                exits,
                entries - exits));
        }

        return buckets;
    }

    private static string BuildInsightPeriodLabel(DateOnly inicio, DateOnly fim)
    {
        if (IsWholeYearRange(inicio, fim))
        {
            return $"ano {inicio.Year}";
        }

        if (IsWholeMonthRange(inicio, fim))
        {
            return $"{GetMonthName(inicio.Month)} de {inicio.Year}";
        }

        return $"intervalo de {inicio:dd/MM/yyyy} até {fim:dd/MM/yyyy}";
    }

    private static bool IsWholeYearRange(DateOnly inicio, DateOnly fim)
        => inicio.Day == 1 && inicio.Month == 1 && fim.Month == 12 && fim.Day == 31 && inicio.Year == fim.Year;

    private static bool IsWholeMonthRange(DateOnly inicio, DateOnly fim)
        => inicio.Year == fim.Year && inicio.Month == fim.Month && inicio.Day == 1 && fim.Day == DateTime.DaysInMonth(fim.Year, fim.Month);

    private static string GetMonthName(int month)
        => month switch
        {
            1 => "Janeiro",
            2 => "Fevereiro",
            3 => "Março",
            4 => "Abril",
            5 => "Maio",
            6 => "Junho",
            7 => "Julho",
            8 => "Agosto",
            9 => "Setembro",
            10 => "Outubro",
            11 => "Novembro",
            12 => "Dezembro",
            _ => "Período"
        };

    private static string GetShortMonthName(int month)
        => month switch
        {
            1 => "Jan",
            2 => "Fev",
            3 => "Mar",
            4 => "Abr",
            5 => "Mai",
            6 => "Jun",
            7 => "Jul",
            8 => "Ago",
            9 => "Set",
            10 => "Out",
            11 => "Nov",
            12 => "Dez",
            _ => "--"
        };

    private static string FormatSmartComparisonPercent(decimal atual, decimal anterior)
    {
        if (anterior == 0 && atual == 0)
        {
            return "0,0%";
        }

        if (anterior == 0)
        {
            return atual > 0 ? "+100,0%" : "0,0%";
        }

        var change = (atual - anterior) / Math.Abs(anterior) * 100;
        var sign = change > 0 ? "+" : change < 0 ? "-" : string.Empty;
        return $"{sign}{Math.Abs(change):0.0}%".Replace('.', ',');
    }

    private static string GetSmartComparisonText(decimal atual, decimal anterior, string label)
    {
        if (anterior == 0 && atual == 0)
        {
            return $"Sem variação em relação ao {label}.";
        }

        if (anterior == 0)
        {
            return $"Sem base anterior; houve movimentação neste período em relação ao {label}.";
        }

        if (atual > anterior)
        {
            return $"Acima do {label}.";
        }

        if (atual < anterior)
        {
            return $"Abaixo do {label}.";
        }

        return $"No mesmo nível do {label}.";
    }

    private static string BuildSmartComparisonInsight(DashboardResumoDto atual, DashboardResumoDto anterior, string label)
    {
        if (atual.Transacoes == 0 && anterior.Transacoes == 0)
        {
            return "Ainda não há movimentações suficientes para gerar um insight.";
        }

        var expenseDiff = atual.Saidas - anterior.Saidas;
        var entryDiff = atual.Entradas - anterior.Entradas;
        var balanceDiff = atual.Saldo - anterior.Saldo;

        if (expenseDiff < 0)
        {
            return $"Você gastou {Math.Abs(expenseDiff):C} a menos que no {label}. Ótimo sinal para o seu controle financeiro.";
        }

        if (expenseDiff > 0)
        {
            return $"Você gastou {expenseDiff:C} a mais que no {label}. Vale revisar as categorias que mais cresceram.";
        }

        if (entryDiff > 0)
        {
            return $"Suas entradas aumentaram {entryDiff:C} em relação ao {label}. Bom momento para fortalecer suas metas.";
        }

        if (balanceDiff > 0)
        {
            return $"Seu saldo melhorou {balanceDiff:C} em relação ao {label}. Continue acompanhando essa evolução.";
        }

        if (balanceDiff < 0)
        {
            return $"Seu saldo caiu {Math.Abs(balanceDiff):C} em relação ao {label}. Tente revisar os maiores gastos do período.";
        }

        return $"Seu resultado ficou estável em relação ao {label}.";
    }

    private static decimal GetGoalProgress(MetaDto meta)
    {
        if (meta.ValorObjetivo <= 0)
        {
            return 0;
        }

        return Math.Min(meta.ValorGuardado / meta.ValorObjetivo * 100, 100);
    }

    private static string NormalizeDashboardTone(string? tone)
        => tone switch
        {
            "positive" => "positive",
            "negative" => "negative",
            "warning" => "warning",
            _ => "neutral"
        };

    private static string GetBackendInsightIcon(string? tone)
        => NormalizeDashboardTone(tone) switch
        {
            "negative" => "🔴",
            "positive" => "🟢",
            "warning" => "⚠️",
            _ => "✨"
        };

    private static IEnumerable<MovimentoRequest> BuildParcelas(ParcelamentoRequest request)
    {
        var totalCents = (int)Math.Round(request.Value * 100, MidpointRounding.AwayFromZero);
        var baseCents = totalCents / request.Parcelas;
        var remainder = totalCents % request.Parcelas;

        for (var i = 0; i < request.Parcelas; i++)
        {
            var cents = baseCents + (i < remainder ? 1 : 0);
            var valorParcela = cents / 100m;
            var numero = i + 1;
            var data = request.Date.AddMonths(i);
            var observacao = string.Join(" ", new[]
            {
                $"Parcelamento automático: parcela {numero}/{request.Parcelas}.",
                $"Valor total original: {request.Value:C}.",
                request.Notes
            }.Where(x => !string.IsNullOrWhiteSpace(x)));

            yield return new MovimentoRequest(
                $"{request.Reason.Trim()} ({numero}/{request.Parcelas})",
                request.Category,
                valorParcela,
                data,
                observacao,
                request.AccountId);
        }
    }

    private static decimal Percent(decimal value, decimal total)
        => total <= 0 ? 0 : Math.Round(value / total * 100, 2);
}
