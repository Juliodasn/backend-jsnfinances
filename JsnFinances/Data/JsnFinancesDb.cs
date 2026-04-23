using System.Globalization;
using System.Text;
using JsnFinances.Api.Domain;
using Npgsql;
using NpgsqlTypes;

namespace JsnFinances.Api.Data;

public sealed partial class JsnFinancesDb
{
    private readonly NpgsqlDataSource _dataSource;

    public JsnFinancesDb(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    public async Task<IReadOnlyList<MovimentacaoDto>> ListMovimentacoesAsync(
        string tabela,
        Guid userId,
        DateOnly? inicio = null,
        DateOnly? fim = null,
        int limit = 1000,
        int offset = 0,
        Guid? idConta = null)
    {
        EnsureTabelaFinanceira(tabela);

        limit = Math.Clamp(limit, 1, 5000);
        offset = Math.Max(offset, 0);

        await using var connection = await _dataSource.OpenConnectionAsync();
        var hasContaColumn = await ColumnExistsAsync(connection, tabela, "id_conta");
        var contaSelect = hasContaColumn ? "id_conta" : "null::uuid as id_conta";
        var contaFilter = hasContaColumn ? "and (@idConta is null or id_conta = @idConta)" : string.Empty;

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            select id, descricao, categoria, valor, data_movimentacao, observacao, {contaSelect}
            from public.{tabela}
            where id_usuario = @userId
              and (@inicio is null or data_movimentacao >= @inicio)
              and (@fim is null or data_movimentacao <= @fim)
              {contaFilter}
            order by data_movimentacao desc, criado_em desc
            limit @limit offset @offset
            """;
        Add(command, "userId", userId);
        Add(command, "inicio", inicio, NpgsqlDbType.Date);
        Add(command, "fim", fim, NpgsqlDbType.Date);
        Add(command, "idConta", hasContaColumn ? idConta : null, NpgsqlDbType.Uuid);
        Add(command, "limit", limit);
        Add(command, "offset", offset);

        var rows = new List<MovimentacaoDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new MovimentacaoDto(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetDecimal(3),
                reader.GetFieldValue<DateOnly>(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetGuid(6)
            ));
        }

        return rows;
    }

    public async Task<int> CountMovimentacoesAsync(
        string tabela,
        Guid userId,
        DateOnly? inicio = null,
        DateOnly? fim = null,
        Guid? idConta = null)
    {
        EnsureTabelaFinanceira(tabela);

        await using var connection = await _dataSource.OpenConnectionAsync();
        var hasContaColumn = await ColumnExistsAsync(connection, tabela, "id_conta");
        var contaFilter = hasContaColumn ? "and (@idConta is null or id_conta = @idConta)" : string.Empty;

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            select count(*)::int
            from public.{tabela}
            where id_usuario = @userId
              and (@inicio is null or data_movimentacao >= @inicio)
              and (@fim is null or data_movimentacao <= @fim)
              {contaFilter}
            """;
        Add(command, "userId", userId);
        Add(command, "inicio", inicio, NpgsqlDbType.Date);
        Add(command, "fim", fim, NpgsqlDbType.Date);
        Add(command, "idConta", hasContaColumn ? idConta : null, NpgsqlDbType.Uuid);

        return Convert.ToInt32(await command.ExecuteScalarAsync() ?? 0);
    }

    public async Task<PagedResultDto<MovimentacaoDto>> ListMovimentacoesPagedAsync(
        string tabela,
        Guid userId,
        DateOnly? inicio,
        DateOnly? fim,
        int page,
        int pageSize,
        Guid? idConta = null)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 5000);

        var total = await CountMovimentacoesAsync(tabela, userId, inicio, fim, idConta);
        var totalPages = Math.Max(1, (int)Math.Ceiling(total / (double)pageSize));
        page = Math.Min(page, totalPages);
        var offset = (page - 1) * pageSize;

        var items = await ListMovimentacoesAsync(tabela, userId, inicio, fim, pageSize, offset, idConta);

        return new PagedResultDto<MovimentacaoDto>(items, total, page, pageSize, totalPages);
    }

    public async Task<int> CountCategoriasAsync(Guid userId, string? search = null, string? tipo = null)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();

        var normalizedTipo = NormalizeTipoCategoria(tipo);
        var hasTipo = !string.IsNullOrWhiteSpace(tipo) && normalizedTipo is "entrada" or "saida" or "ambos";

        command.CommandText = """
            select count(*)::int
            from public.categorias
            where id_usuario = @userId
              and (@search is null
                   or nome ilike '%' || @search || '%'
                   or coalesce(descricao, '') ilike '%' || @search || '%')
              and (@hasTipo = false or tipo = @tipo)
            """;

        Add(command, "userId", userId);
        Add(command, "search", NullIfWhiteSpace(search));
        Add(command, "hasTipo", hasTipo);
        Add(command, "tipo", normalizedTipo);

        return Convert.ToInt32(await command.ExecuteScalarAsync() ?? 0);
    }

    public async Task<PagedResultDto<CategoriaDto>> ListCategoriasPagedAsync(
        Guid userId,
        int page,
        int pageSize,
        string? search = null,
        string? tipo = null)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);

        var normalizedTipo = NormalizeTipoCategoria(tipo);
        var hasTipo = !string.IsNullOrWhiteSpace(tipo) && normalizedTipo is "entrada" or "saida" or "ambos";

        var total = await CountCategoriasAsync(userId, search, tipo);
        var totalPages = Math.Max(1, (int)Math.Ceiling(total / (double)pageSize));
        page = Math.Min(page, totalPages);
        var offset = (page - 1) * pageSize;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, nome, tipo, descricao, cor, icone, ativo
            from public.categorias
            where id_usuario = @userId
              and (@search is null
                   or nome ilike '%' || @search || '%'
                   or coalesce(descricao, '') ilike '%' || @search || '%')
              and (@hasTipo = false or tipo = @tipo)
            order by nome asc
            limit @limit offset @offset
            """;
        Add(command, "userId", userId);
        Add(command, "search", NullIfWhiteSpace(search));
        Add(command, "hasTipo", hasTipo);
        Add(command, "tipo", normalizedTipo);
        Add(command, "limit", pageSize);
        Add(command, "offset", offset);

        var rows = new List<CategoriaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new CategoriaDto(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetBoolean(6)
            ));
        }

        return new PagedResultDto<CategoriaDto>(rows, total, page, pageSize, totalPages);
    }

    public async Task<int> CountTransacoesAsync(
        Guid userId,
        DateOnly? inicio = null,
        DateOnly? fim = null,
        string? tipo = null,
        string? categoria = null,
        Guid? idConta = null)
    {
        var includeEntrada = string.IsNullOrWhiteSpace(tipo) || tipo == "all" || tipo == "entrada" || tipo == "entry";
        var includeSaida = string.IsNullOrWhiteSpace(tipo) || tipo == "all" || tipo == "saida" || tipo == "exit";

        var total = 0;

        if (includeEntrada)
        {
            total += await CountMovimentacoesAsync("entradas", userId, inicio, fim, idConta);
        }

        if (includeSaida)
        {
            total += await CountMovimentacoesAsync("saidas", userId, inicio, fim, idConta);
        }

        if (!string.IsNullOrWhiteSpace(categoria) && categoria != "all")
        {
            // Quando existe filtro por categoria, usamos a consulta unificada abaixo para contar corretamente.
            var paged = await ListTransacoesPagedAsync(userId, inicio, fim, 1, 1, tipo, categoria, idConta);
            return paged.Total;
        }

        return total;
    }

    public async Task<PagedResultDto<TransacaoPaginadaDto>> ListTransacoesPagedAsync(
        Guid userId,
        DateOnly? inicio,
        DateOnly? fim,
        int page,
        int pageSize,
        string? tipo = null,
        string? categoria = null,
        Guid? idConta = null)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 5000);
        var offset = (page - 1) * pageSize;

        var normalizedTipo = string.IsNullOrWhiteSpace(tipo) ? "all" : tipo.Trim().ToLowerInvariant();
        var normalizedCategoria = NullIfWhiteSpace(categoria);
        var filterCategoria = normalizedCategoria is not null && normalizedCategoria != "all";

        await using var connection = await _dataSource.OpenConnectionAsync();

        await using var countCommand = connection.CreateCommand();
        countCommand.CommandText = """
            select count(*)::int
            from (
              select id_usuario, data_movimentacao, categoria, id_conta, 'entrada' as tipo
              from public.entradas
              union all
              select id_usuario, data_movimentacao, categoria, id_conta, 'saida' as tipo
              from public.saidas
            ) t
            where t.id_usuario = @userId
              and (@inicio is null or t.data_movimentacao >= @inicio)
              and (@fim is null or t.data_movimentacao <= @fim)
              and (@idConta is null or t.id_conta = @idConta)
              and (@categoria is null or t.categoria = @categoria)
              and (@tipo = 'all'
                   or (@tipo in ('entrada', 'entry') and t.tipo = 'entrada')
                   or (@tipo in ('saida', 'exit') and t.tipo = 'saida'))
            """;
        Add(countCommand, "userId", userId);
        Add(countCommand, "inicio", inicio, NpgsqlDbType.Date);
        Add(countCommand, "fim", fim, NpgsqlDbType.Date);
        Add(countCommand, "idConta", idConta, NpgsqlDbType.Uuid);
        Add(countCommand, "categoria", filterCategoria ? normalizedCategoria : null, NpgsqlDbType.Text);
        Add(countCommand, "tipo", normalizedTipo);

        var total = Convert.ToInt32(await countCommand.ExecuteScalarAsync() ?? 0);
        var totalPages = Math.Max(1, (int)Math.Ceiling(total / (double)pageSize));
        page = Math.Min(page, totalPages);
        offset = (page - 1) * pageSize;

        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, tipo, descricao, categoria, valor, data_movimentacao, observacao, id_conta
            from (
              select id, 'entrada' as tipo, descricao, categoria, valor, data_movimentacao, observacao, id_conta, criado_em, id_usuario
              from public.entradas
              union all
              select id, 'saida' as tipo, descricao, categoria, valor, data_movimentacao, observacao, id_conta, criado_em, id_usuario
              from public.saidas
            ) t
            where t.id_usuario = @userId
              and (@inicio is null or t.data_movimentacao >= @inicio)
              and (@fim is null or t.data_movimentacao <= @fim)
              and (@idConta is null or t.id_conta = @idConta)
              and (@categoria is null or t.categoria = @categoria)
              and (@tipo = 'all'
                   or (@tipo in ('entrada', 'entry') and t.tipo = 'entrada')
                   or (@tipo in ('saida', 'exit') and t.tipo = 'saida'))
            order by t.data_movimentacao desc, t.criado_em desc
            limit @limit offset @offset
            """;
        Add(command, "userId", userId);
        Add(command, "inicio", inicio, NpgsqlDbType.Date);
        Add(command, "fim", fim, NpgsqlDbType.Date);
        Add(command, "idConta", idConta, NpgsqlDbType.Uuid);
        Add(command, "categoria", filterCategoria ? normalizedCategoria : null, NpgsqlDbType.Text);
        Add(command, "tipo", normalizedTipo);
        Add(command, "limit", pageSize);
        Add(command, "offset", offset);

        var rows = new List<TransacaoPaginadaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new TransacaoPaginadaDto(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetDecimal(4),
                reader.GetFieldValue<DateOnly>(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetGuid(7)
            ));
        }

        return new PagedResultDto<TransacaoPaginadaDto>(rows, total, page, pageSize, totalPages);
    }

    public async Task<Guid> InsertMovimentacaoAsync(string tabela, Guid userId, MovimentoRequest request)
    {
        EnsureTabelaFinanceira(tabela);
        ValidateMovimento(request.Reason, request.Value, request.Date);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            insert into public.{tabela} (id_usuario, descricao, categoria, valor, data_movimentacao, observacao, id_conta)
            values (@userId, @descricao, @categoria, @valor, @data, @observacao, @idConta)
            returning id
            """;
        Add(command, "userId", userId);
        Add(command, "descricao", request.Reason.Trim());
        Add(command, "categoria", NormalizeCategoria(request.Category));
        Add(command, "valor", request.Value);
        Add(command, "data", request.Date);
        Add(command, "observacao", NullIfWhiteSpace(request.Notes));
        Add(command, "idConta", request.AccountId, NpgsqlDbType.Uuid);

        return (Guid)(await command.ExecuteScalarAsync() ?? Guid.Empty);
    }

    public async Task InsertMovimentacoesAsync(string tabela, Guid userId, IEnumerable<MovimentoRequest> requests)
    {
        EnsureTabelaFinanceira(tabela);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        foreach (var request in requests)
        {
            ValidateMovimento(request.Reason, request.Value, request.Date);

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"""
                insert into public.{tabela} (id_usuario, descricao, categoria, valor, data_movimentacao, observacao, id_conta)
                values (@userId, @descricao, @categoria, @valor, @data, @observacao, @idConta)
                """;
            Add(command, "userId", userId);
            Add(command, "descricao", request.Reason.Trim());
            Add(command, "categoria", NormalizeCategoria(request.Category));
            Add(command, "valor", request.Value);
            Add(command, "data", request.Date);
            Add(command, "observacao", NullIfWhiteSpace(request.Notes));
            Add(command, "idConta", request.AccountId, NpgsqlDbType.Uuid);
            await command.ExecuteNonQueryAsync();
        }

        await transaction.CommitAsync();
    }

    public async Task UpdateMovimentacaoAsync(string tabela, Guid userId, Guid id, MovimentoRequest request)
    {
        EnsureTabelaFinanceira(tabela);
        ValidateMovimento(request.Reason, request.Value, request.Date);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            update public.{tabela}
               set descricao = @descricao,
                   categoria = @categoria,
                   valor = @valor,
                   data_movimentacao = @data,
                   observacao = @observacao,
                   id_conta = @idConta
             where id = @id and id_usuario = @userId
            """;
        Add(command, "id", id);
        Add(command, "userId", userId);
        Add(command, "descricao", request.Reason.Trim());
        Add(command, "categoria", NormalizeCategoria(request.Category));
        Add(command, "valor", request.Value);
        Add(command, "data", request.Date);
        Add(command, "observacao", NullIfWhiteSpace(request.Notes));
        Add(command, "idConta", request.AccountId, NpgsqlDbType.Uuid);

        if (await command.ExecuteNonQueryAsync() == 0)
        {
            throw new InvalidOperationException("Lançamento não encontrado para o usuário atual.");
        }
    }

    public async Task DeleteMovimentacaoAsync(string tabela, Guid userId, Guid id)
    {
        EnsureTabelaFinanceira(tabela);
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"delete from public.{tabela} where id = @id and id_usuario = @userId";
        Add(command, "id", id);
        Add(command, "userId", userId);

        if (await command.ExecuteNonQueryAsync() == 0)
        {
            throw new InvalidOperationException("Lançamento não encontrado para o usuário atual.");
        }
    }

    public async Task<IReadOnlyList<CategoriaDto>> ListCategoriasAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, nome, tipo, descricao, cor, icone, ativo
            from public.categorias
            where id_usuario = @userId
            order by nome asc
            """;
        Add(command, "userId", userId);

        var rows = new List<CategoriaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new CategoriaDto(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetBoolean(6)
            ));
        }

        return rows;
    }

    public async Task<Guid> InsertCategoriaAsync(Guid userId, CategoriaRequest request)
    {
        var tipo = NormalizeTipoCategoria(request.Tipo);
        if (string.IsNullOrWhiteSpace(request.Nome)) throw new ArgumentException("Informe o nome da categoria.");

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.categorias (id_usuario, nome, tipo, descricao, cor, icone, ativo)
            values (@userId, @nome, @tipo, @descricao, @cor, @icone, @ativo)
            returning id
            """;
        Add(command, "userId", userId);
        Add(command, "nome", request.Nome.Trim());
        Add(command, "tipo", tipo);
        Add(command, "descricao", NullIfWhiteSpace(request.Descricao));
        Add(command, "cor", NullIfWhiteSpace(request.Cor));
        Add(command, "icone", NullIfWhiteSpace(request.Icone));
        Add(command, "ativo", request.Ativo);

        return (Guid)(await command.ExecuteScalarAsync() ?? Guid.Empty);
    }

    public async Task UpdateCategoriaAsync(Guid userId, Guid id, CategoriaRequest request)
    {
        var tipo = NormalizeTipoCategoria(request.Tipo);
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        string? oldName;
        await using (var getCommand = connection.CreateCommand())
        {
            getCommand.Transaction = transaction;
            getCommand.CommandText = "select nome from public.categorias where id = @id and id_usuario = @userId";
            Add(getCommand, "id", id);
            Add(getCommand, "userId", userId);
            oldName = await getCommand.ExecuteScalarAsync() as string;
        }

        if (oldName is null)
        {
            throw new InvalidOperationException("Categoria não encontrada para o usuário atual.");
        }

        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = """
                update public.categorias
                   set nome = @nome,
                       tipo = @tipo,
                       descricao = @descricao,
                       cor = @cor,
                       icone = @icone,
                       ativo = @ativo
                 where id = @id and id_usuario = @userId
                """;
            Add(command, "id", id);
            Add(command, "userId", userId);
            Add(command, "nome", request.Nome.Trim());
            Add(command, "tipo", tipo);
            Add(command, "descricao", NullIfWhiteSpace(request.Descricao));
            Add(command, "cor", NullIfWhiteSpace(request.Cor));
            Add(command, "icone", NullIfWhiteSpace(request.Icone));
            Add(command, "ativo", request.Ativo);
            await command.ExecuteNonQueryAsync();
        }

        if (!string.Equals(oldName, request.Nome.Trim(), StringComparison.OrdinalIgnoreCase))
        {
            await RenameCategoriaReferencesAsync(connection, transaction, userId, oldName, request.Nome.Trim());
        }

        await transaction.CommitAsync();
    }

    public async Task DeleteCategoriaAsync(Guid userId, Guid id)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "delete from public.categorias where id = @id and id_usuario = @userId";
        Add(command, "id", id);
        Add(command, "userId", userId);

        if (await command.ExecuteNonQueryAsync() == 0)
        {
            throw new InvalidOperationException("Categoria não encontrada para o usuário atual.");
        }
    }

    public async Task<IReadOnlyList<MetaDto>> ListMetasAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, titulo, categoria, descricao, valor_objetivo, valor_guardado, data_limite, cor, icone
            from public.metas
            where id_usuario = @userId
            order by criado_em desc
            """;
        Add(command, "userId", userId);

        var rows = new List<MetaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(ReadMeta(reader));
        }

        return rows;
    }

    public async Task<Guid> InsertMetaAsync(Guid userId, MetaRequest request)
    {
        ValidateMeta(request);
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.metas (id_usuario, titulo, categoria, descricao, valor_objetivo, valor_guardado, data_limite, cor, icone)
            values (@userId, @titulo, @categoria, @descricao, @valorObjetivo, @valorGuardado, @dataLimite, @cor, @icone)
            returning id
            """;
        AddMetaParams(command, userId, request);
        return (Guid)(await command.ExecuteScalarAsync() ?? Guid.Empty);
    }

    public async Task UpdateMetaAsync(Guid userId, Guid id, MetaRequest request)
    {
        ValidateMeta(request);
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            update public.metas
               set titulo = @titulo,
                   categoria = @categoria,
                   descricao = @descricao,
                   valor_objetivo = @valorObjetivo,
                   valor_guardado = @valorGuardado,
                   data_limite = @dataLimite,
                   cor = @cor,
                   icone = @icone
             where id = @id and id_usuario = @userId
            """;
        Add(command, "id", id);
        AddMetaParams(command, userId, request);
        if (await command.ExecuteNonQueryAsync() == 0)
        {
            throw new InvalidOperationException("Meta não encontrada para o usuário atual.");
        }
    }

    public async Task<MetaDto> AddValueToMetaAsync(Guid userId, Guid id, decimal valor)
    {
        if (valor <= 0) throw new ArgumentException("Informe um valor maior que zero para guardar na meta.");

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        MetaDto? meta;
        await using (var getCommand = connection.CreateCommand())
        {
            getCommand.Transaction = transaction;
            getCommand.CommandText = """
                select id, titulo, categoria, descricao, valor_objetivo, valor_guardado, data_limite, cor, icone
                from public.metas
                where id = @id and id_usuario = @userId
                for update
                """;
            Add(getCommand, "id", id);
            Add(getCommand, "userId", userId);
            await using var reader = await getCommand.ExecuteReaderAsync();
            meta = await reader.ReadAsync() ? ReadMeta(reader) : null;
        }

        if (meta is null) throw new InvalidOperationException("Meta não encontrada para o usuário atual.");

        var restante = Math.Max(meta.ValorObjetivo - meta.ValorGuardado, 0);
        var valorAplicado = Math.Min(valor, restante);
        var novoValor = meta.ValorGuardado + valorAplicado;

        await using (var updateCommand = connection.CreateCommand())
        {
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = "update public.metas set valor_guardado = @valor where id = @id and id_usuario = @userId";
            Add(updateCommand, "valor", novoValor);
            Add(updateCommand, "id", id);
            Add(updateCommand, "userId", userId);
            await updateCommand.ExecuteNonQueryAsync();
        }

        await transaction.CommitAsync();
        return meta with { ValorGuardado = novoValor };
    }

    public async Task DeleteMetaAsync(Guid userId, Guid id)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "delete from public.metas where id = @id and id_usuario = @userId";
        Add(command, "id", id);
        Add(command, "userId", userId);

        if (await command.ExecuteNonQueryAsync() == 0)
        {
            throw new InvalidOperationException("Meta não encontrada para o usuário atual.");
        }
    }

    public async Task<IReadOnlyList<OrcamentoDto>> ListOrcamentosAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, categoria, limite_mensal
            from public.orcamentos_categoria
            where id_usuario = @userId
            order by categoria asc
            """;
        Add(command, "userId", userId);

        var rows = new List<OrcamentoDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new OrcamentoDto(reader.GetGuid(0), reader.GetString(1), reader.GetDecimal(2)));
        }
        return rows;
    }

    public async Task UpsertOrcamentoAsync(Guid userId, OrcamentoRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Categoria)) throw new ArgumentException("Informe a categoria do orçamento.");
        if (request.LimiteMensal <= 0) throw new ArgumentException("Informe um limite mensal maior que zero.");

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.orcamentos_categoria (id_usuario, categoria, limite_mensal)
            values (@userId, @categoria, @limite)
            on conflict (id_usuario, categoria)
            do update set limite_mensal = excluded.limite_mensal
            """;
        Add(command, "userId", userId);
        Add(command, "categoria", request.Categoria.Trim());
        Add(command, "limite", request.LimiteMensal);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DeleteOrcamentoAsync(Guid userId, string categoria)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "delete from public.orcamentos_categoria where id_usuario = @userId and categoria = @categoria";
        Add(command, "userId", userId);
        Add(command, "categoria", categoria);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<ContaDto>> ListContasAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, nome, tipo, saldo_inicial, cor, ativo
            from public.contas
            where id_usuario = @userId
            order by nome asc
            """;
        Add(command, "userId", userId);

        var rows = new List<ContaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ContaDto(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetDecimal(3), reader.IsDBNull(4) ? null : reader.GetString(4), reader.GetBoolean(5)));
        }
        return rows;
    }

    public async Task<Guid> UpsertContaAsync(Guid userId, Guid? id, ContaRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Nome)) throw new ArgumentException("Informe o nome da conta.");
        var contaId = id ?? Guid.NewGuid();

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.contas (id, id_usuario, nome, tipo, saldo_inicial, cor, ativo)
            values (@id, @userId, @nome, @tipo, @saldo, @cor, @ativo)
            on conflict (id)
            do update set nome = excluded.nome,
                          tipo = excluded.tipo,
                          saldo_inicial = excluded.saldo_inicial,
                          cor = excluded.cor,
                          ativo = excluded.ativo
            """;
        Add(command, "id", contaId);
        Add(command, "userId", userId);
        Add(command, "nome", request.Nome.Trim());
        Add(command, "tipo", request.Tipo.Trim());
        Add(command, "saldo", request.SaldoInicial);
        Add(command, "cor", NullIfWhiteSpace(request.Cor));
        Add(command, "ativo", request.Ativo);
        await command.ExecuteNonQueryAsync();
        return contaId;
    }

    public async Task<Guid> InsertTransferenciaAsync(Guid userId, TransferenciaRequest request)
    {
        if (request.IdContaOrigem == request.IdContaDestino) throw new ArgumentException("Escolha contas diferentes para transferir.");
        if (request.Valor <= 0) throw new ArgumentException("Informe um valor maior que zero para transferir.");

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.transferencias_contas (id_usuario, id_conta_origem, id_conta_destino, valor, data_transferencia, observacao)
            values (@userId, @origem, @destino, @valor, @data, @observacao)
            returning id
            """;
        Add(command, "userId", userId);
        Add(command, "origem", request.IdContaOrigem);
        Add(command, "destino", request.IdContaDestino);
        Add(command, "valor", request.Valor);
        Add(command, "data", request.DataTransferencia);
        Add(command, "observacao", NullIfWhiteSpace(request.Observacao));
        return (Guid)(await command.ExecuteScalarAsync() ?? Guid.Empty);
    }

    public async Task<IReadOnlyList<TransferenciaDto>> ListTransferenciasAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id, id_conta_origem, id_conta_destino, valor, data_transferencia, observacao
            from public.transferencias_contas
            where id_usuario = @userId
            order by data_transferencia desc, criado_em desc
            """;
        Add(command, "userId", userId);

        var rows = new List<TransferenciaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new TransferenciaDto(reader.GetGuid(0), reader.GetGuid(1), reader.GetGuid(2), reader.GetDecimal(3), reader.GetFieldValue<DateOnly>(4), reader.IsDBNull(5) ? null : reader.GetString(5)));
        }
        return rows;
    }


    public async Task<IReadOnlyList<ContaSaldoDto>> ListContasSaldoAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select
                c.id,
                c.nome,
                c.tipo,
                c.saldo_inicial,
                c.cor,
                c.ativo,
                coalesce(e.total_entradas, 0) as total_entradas,
                coalesce(s.total_saidas, 0) as total_saidas,
                coalesce(tin.total_transferencias_entrada, 0) as total_transferencias_entrada,
                coalesce(tout.total_transferencias_saida, 0) as total_transferencias_saida,
                (
                    c.saldo_inicial
                    + coalesce(e.total_entradas, 0)
                    - coalesce(s.total_saidas, 0)
                    + coalesce(tin.total_transferencias_entrada, 0)
                    - coalesce(tout.total_transferencias_saida, 0)
                ) as saldo_atual
            from public.contas c
            left join (
                select id_conta, id_usuario, sum(valor) as total_entradas
                from public.entradas
                where id_conta is not null
                group by id_conta, id_usuario
            ) e on e.id_conta = c.id and e.id_usuario = c.id_usuario
            left join (
                select id_conta, id_usuario, sum(valor) as total_saidas
                from public.saidas
                where id_conta is not null
                group by id_conta, id_usuario
            ) s on s.id_conta = c.id and s.id_usuario = c.id_usuario
            left join (
                select id_conta_destino as id_conta, id_usuario, sum(valor) as total_transferencias_entrada
                from public.transferencias_contas
                group by id_conta_destino, id_usuario
            ) tin on tin.id_conta = c.id and tin.id_usuario = c.id_usuario
            left join (
                select id_conta_origem as id_conta, id_usuario, sum(valor) as total_transferencias_saida
                from public.transferencias_contas
                group by id_conta_origem, id_usuario
            ) tout on tout.id_conta = c.id and tout.id_usuario = c.id_usuario
            where c.id_usuario = @userId
            order by c.nome asc
            """;
        Add(command, "userId", userId);

        var rows = new List<ContaSaldoDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ContaSaldoDto(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetDecimal(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetBoolean(5),
                reader.GetDecimal(6),
                reader.GetDecimal(7),
                reader.GetDecimal(8),
                reader.GetDecimal(9),
                reader.GetDecimal(10)));
        }

        return rows;
    }

    public async Task DeleteContaAsync(Guid userId, Guid id)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            delete from public.contas
            where id = @id and id_usuario = @userId
            """;
        Add(command, "id", id);
        Add(command, "userId", userId);

        if (await command.ExecuteNonQueryAsync() == 0)
        {
            throw new InvalidOperationException("Conta não encontrada para o usuário atual.");
        }
    }

    public async Task<IReadOnlyList<CalendarioMovimentacaoDto>> ListCalendarioMovimentacoesAsync(Guid userId, DateOnly dataInicio, DateOnly dataFim)
    {
        var entradas = await ListMovimentacoesAsync("entradas", userId, dataInicio, dataFim, 2000, 0);
        var saidas = await ListMovimentacoesAsync("saidas", userId, dataInicio, dataFim, 2000, 0);
        var transferencias = new List<CalendarioMovimentacaoDto>();

        await using (var connection = await _dataSource.OpenConnectionAsync())
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                select
                    t.id,
                    t.valor,
                    t.data_transferencia,
                    t.observacao,
                    coalesce(origem.nome, 'Conta origem') as conta_origem,
                    coalesce(destino.nome, 'Conta destino') as conta_destino
                from public.transferencias_contas t
                left join public.contas origem on origem.id = t.id_conta_origem and origem.id_usuario = t.id_usuario
                left join public.contas destino on destino.id = t.id_conta_destino and destino.id_usuario = t.id_usuario
                where t.id_usuario = @userId
                  and t.data_transferencia between @inicio and @fim
                order by t.data_transferencia desc, t.criado_em desc
                """;
            Add(command, "userId", userId);
            Add(command, "inicio", dataInicio, NpgsqlDbType.Date);
            Add(command, "fim", dataFim, NpgsqlDbType.Date);

            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var id = reader.GetGuid(0);
                var valor = reader.GetDecimal(1);
                var data = reader.GetFieldValue<DateOnly>(2);
                var observacao = reader.IsDBNull(3) ? null : reader.GetString(3);
                var origem = reader.GetString(4);
                var destino = reader.GetString(5);

                transferencias.Add(new CalendarioMovimentacaoDto(
                    id,
                    "transferencia",
                    $"Transferência: {origem} → {destino}",
                    "Transferência",
                    valor,
                    data,
                    string.IsNullOrWhiteSpace(observacao) ? "Movimentação entre contas próprias. Não altera o saldo total." : observacao,
                    null));
            }
        }

        return entradas
            .Select(x => new CalendarioMovimentacaoDto(x.Id, "entrada", x.Descricao, x.Categoria, x.Valor, x.DataMovimentacao, x.Observacao, x.IdConta))
            .Concat(saidas.Select(x => new CalendarioMovimentacaoDto(x.Id, "saida", x.Descricao, x.Categoria, x.Valor, x.DataMovimentacao, x.Observacao, x.IdConta)))
            .Concat(transferencias)
            .OrderByDescending(x => x.DataMovimentacao)
            .ThenBy(x => x.Tipo)
            .ToList();
    }

    private async Task RenameCategoriaReferencesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid userId, string oldName, string newName)
    {
        foreach (var tabela in new[] { "entradas", "saidas" })
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"update public.{tabela} set categoria = @newName where id_usuario = @userId and categoria = @oldName";
            Add(command, "userId", userId);
            Add(command, "oldName", oldName);
            Add(command, "newName", newName);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static MovimentacaoDto ReadMovimentacao(NpgsqlDataReader reader)
    {
        return new MovimentacaoDto(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.GetDecimal(3),
            reader.GetFieldValue<DateOnly>(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetGuid(6)
        );
    }

    private static MetaDto ReadMeta(NpgsqlDataReader reader)
    {
        return new MetaDto(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.GetDecimal(4),
            reader.GetDecimal(5),
            reader.IsDBNull(6) ? null : reader.GetFieldValue<DateOnly>(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetString(8)
        );
    }

    private static void AddMetaParams(NpgsqlCommand command, Guid userId, MetaRequest request)
    {
        Add(command, "userId", userId);
        Add(command, "titulo", request.Titulo.Trim());
        Add(command, "categoria", string.IsNullOrWhiteSpace(request.Categoria) ? "Geral" : request.Categoria.Trim());
        Add(command, "descricao", NullIfWhiteSpace(request.Descricao));
        Add(command, "valorObjetivo", request.ValorObjetivo);
        Add(command, "valorGuardado", request.ValorGuardado);
        Add(command, "dataLimite", request.DataLimite);
        Add(command, "cor", NullIfWhiteSpace(request.Cor) ?? "#22b56b");
        Add(command, "icone", NullIfWhiteSpace(request.Icone) ?? "plane");
    }


    private async Task EnsurePerfilSchemaAsync()
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            create extension if not exists pgcrypto;

            create table if not exists public.perfis (
                id_usuario uuid primary key,
                nome_completo text null,
                data_nascimento date null,
                telefone text null,
                url_avatar text null,
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now())
            );

            alter table public.perfis add column if not exists id_usuario uuid;
            alter table public.perfis add column if not exists nome_completo text null;
            alter table public.perfis add column if not exists data_nascimento date null;
            alter table public.perfis add column if not exists telefone text null;
            alter table public.perfis add column if not exists url_avatar text null;
            alter table public.perfis add column if not exists criado_em timestamptz not null default timezone('utc', now());
            alter table public.perfis add column if not exists atualizado_em timestamptz not null default timezone('utc', now());

            do $$
            begin
                if exists (
                    select 1
                    from information_schema.columns
                    where table_schema = 'public'
                      and table_name = 'perfis'
                      and column_name = 'id'
                ) then
                    execute 'update public.perfis set id_usuario = id where id_usuario is null';
                end if;
            end $$;

            update public.perfis
               set nome_completo = coalesce(nome_completo, ''),
                   criado_em = coalesce(criado_em, timezone('utc', now())),
                   atualizado_em = coalesce(atualizado_em, timezone('utc', now()))
             where nome_completo is null
                or criado_em is null
                or atualizado_em is null;

            create unique index if not exists ux_perfis_id_usuario
            on public.perfis (id_usuario);
            """;
        await command.ExecuteNonQueryAsync();
    }

    private async Task EnsurePreferenciasSchemaAsync()
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            create extension if not exists pgcrypto;

            create table if not exists public.preferencias_usuario (
                id_usuario uuid primary key,
                tema text not null default 'claro',
                cor_principal text not null default 'purple',
                moeda text not null default 'BRL',
                idioma text not null default 'pt-BR',
                modo_nucleo boolean not null default false,
                filtro_ano text null,
                filtro_mes text null,
                filtro_inicio date null,
                filtro_fim date null,
                criado_em timestamptz not null default timezone('utc', now()),
                atualizado_em timestamptz not null default timezone('utc', now())
            );

            alter table public.preferencias_usuario add column if not exists id_usuario uuid;
            alter table public.preferencias_usuario add column if not exists tema text not null default 'claro';
            alter table public.preferencias_usuario add column if not exists cor_principal text not null default 'purple';
            alter table public.preferencias_usuario add column if not exists moeda text not null default 'BRL';
            alter table public.preferencias_usuario add column if not exists idioma text not null default 'pt-BR';
            alter table public.preferencias_usuario add column if not exists modo_nucleo boolean not null default false;
            alter table public.preferencias_usuario add column if not exists filtro_ano text null;
            alter table public.preferencias_usuario add column if not exists filtro_mes text null;
            alter table public.preferencias_usuario add column if not exists filtro_inicio date null;
            alter table public.preferencias_usuario add column if not exists filtro_fim date null;
            alter table public.preferencias_usuario add column if not exists criado_em timestamptz not null default timezone('utc', now());
            alter table public.preferencias_usuario add column if not exists atualizado_em timestamptz not null default timezone('utc', now());

            update public.preferencias_usuario
               set tema = coalesce(nullif(trim(tema), ''), 'claro'),
                   cor_principal = coalesce(nullif(trim(cor_principal), ''), 'purple'),
                   moeda = coalesce(nullif(trim(moeda), ''), 'BRL'),
                   idioma = coalesce(nullif(trim(idioma), ''), 'pt-BR'),
                   modo_nucleo = coalesce(modo_nucleo, false),
                   criado_em = coalesce(criado_em, timezone('utc', now())),
                   atualizado_em = coalesce(atualizado_em, timezone('utc', now()))
             where tema is null
                or trim(tema) = ''
                or cor_principal is null
                or trim(cor_principal) = ''
                or moeda is null
                or trim(moeda) = ''
                or idioma is null
                or trim(idioma) = ''
                or modo_nucleo is null
                or criado_em is null
                or atualizado_em is null;

            create unique index if not exists ux_preferencias_usuario_id_usuario
            on public.preferencias_usuario (id_usuario);
            """;
        await command.ExecuteNonQueryAsync();
    }


    public async Task<PerfilDto?> GetPerfilAsync(Guid userId)
    {
        await EnsurePerfilSchemaAsync();

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id_usuario, nome_completo, data_nascimento, telefone, url_avatar
            from public.perfis
            where id_usuario = @userId
            """;
        Add(command, "userId", userId);

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return new PerfilDto(
            reader.GetGuid(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetFieldValue<DateOnly>(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4));
    }

    public async Task<PerfilDto> UpsertPerfilAsync(Guid userId, PerfilRequest request)
    {
        await EnsurePerfilSchemaAsync();

        await using var connection = await _dataSource.OpenConnectionAsync();
        var hasLegacyIdColumn = await ColumnExistsAsync(connection, "perfis", "id");

        await using var command = connection.CreateCommand();
        command.CommandText = hasLegacyIdColumn
            ? """
              insert into public.perfis (id, id_usuario, nome_completo, data_nascimento, telefone, url_avatar)
              values (@userId, @userId, @nome, @nascimento, @telefone, @avatar)
              on conflict (id_usuario)
              do update set nome_completo = excluded.nome_completo,
                            data_nascimento = excluded.data_nascimento,
                            telefone = excluded.telefone,
                            url_avatar = excluded.url_avatar,
                            atualizado_em = timezone('utc', now())
              returning id_usuario, nome_completo, data_nascimento, telefone, url_avatar
              """
            : """
              insert into public.perfis (id_usuario, nome_completo, data_nascimento, telefone, url_avatar)
              values (@userId, @nome, @nascimento, @telefone, @avatar)
              on conflict (id_usuario)
              do update set nome_completo = excluded.nome_completo,
                            data_nascimento = excluded.data_nascimento,
                            telefone = excluded.telefone,
                            url_avatar = excluded.url_avatar,
                            atualizado_em = timezone('utc', now())
              returning id_usuario, nome_completo, data_nascimento, telefone, url_avatar
              """;
        Add(command, "userId", userId);
        Add(command, "nome", NullIfWhiteSpace(request.NomeCompleto), NpgsqlDbType.Text);
        Add(command, "nascimento", request.DataNascimento, NpgsqlDbType.Date);
        Add(command, "telefone", NullIfWhiteSpace(request.Telefone), NpgsqlDbType.Text);
        Add(command, "avatar", NullIfWhiteSpace(request.UrlAvatar), NpgsqlDbType.Text);

        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        return new PerfilDto(
            reader.GetGuid(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetFieldValue<DateOnly>(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4));
    }

    public async Task<PreferenciasUsuarioDto> GetPreferenciasAsync(Guid userId)
    {
        await EnsurePreferenciasSchemaAsync();
        await EnsurePreferenciasAsync(userId, null);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select tema, cor_principal, moeda, idioma, modo_nucleo, filtro_ano, filtro_mes, filtro_inicio, filtro_fim
            from public.preferencias_usuario
            where id_usuario = @userId
            """;
        Add(command, "userId", userId);

        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        return ReadPreferencias(reader);
    }

    public async Task<PreferenciasUsuarioDto> UpsertPreferenciasAsync(Guid userId, PreferenciasUsuarioRequest request)
    {
        await EnsurePreferenciasSchemaAsync();
        await EnsurePreferenciasAsync(userId, request);
        return await GetPreferenciasAsync(userId);
    }

    private async Task EnsurePreferenciasAsync(Guid userId, PreferenciasUsuarioRequest? request)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();

        if (request is null)
        {
            command.CommandText = """
                insert into public.preferencias_usuario
                  (id_usuario, tema, cor_principal, moeda, idioma, modo_nucleo, filtro_ano, filtro_mes, filtro_inicio, filtro_fim)
                values
                  (@userId, 'claro', 'purple', 'BRL', 'pt-BR', false, null, null, null, null)
                on conflict (id_usuario) do nothing
                """;
            Add(command, "userId", userId);
            await command.ExecuteNonQueryAsync();
            return;
        }

        command.CommandText = """
            insert into public.preferencias_usuario
              (id_usuario, tema, cor_principal, moeda, idioma, modo_nucleo, filtro_ano, filtro_mes, filtro_inicio, filtro_fim)
            values
              (@userId, @tema, @cor, @moeda, @idioma, @modo, @ano, @mes, @inicio, @fim)
            on conflict (id_usuario)
            do update set tema = coalesce(excluded.tema, preferencias_usuario.tema),
                          cor_principal = coalesce(excluded.cor_principal, preferencias_usuario.cor_principal),
                          moeda = coalesce(excluded.moeda, preferencias_usuario.moeda),
                          idioma = coalesce(excluded.idioma, preferencias_usuario.idioma),
                          modo_nucleo = excluded.modo_nucleo,
                          filtro_ano = coalesce(excluded.filtro_ano, preferencias_usuario.filtro_ano),
                          filtro_mes = coalesce(excluded.filtro_mes, preferencias_usuario.filtro_mes),
                          filtro_inicio = coalesce(excluded.filtro_inicio, preferencias_usuario.filtro_inicio),
                          filtro_fim = coalesce(excluded.filtro_fim, preferencias_usuario.filtro_fim),
                          atualizado_em = timezone('utc', now())
            """;
        Add(command, "userId", userId);
        Add(command, "tema", NullIfWhiteSpace(request.Tema) ?? "claro", NpgsqlDbType.Text);
        Add(command, "cor", NullIfWhiteSpace(request.CorPrincipal) ?? "purple", NpgsqlDbType.Text);
        Add(command, "moeda", NullIfWhiteSpace(request.Moeda) ?? "BRL", NpgsqlDbType.Text);
        Add(command, "idioma", NullIfWhiteSpace(request.Idioma) ?? "pt-BR", NpgsqlDbType.Text);
        Add(command, "modo", request.ModoNucleo ?? false, NpgsqlDbType.Boolean);
        Add(command, "ano", NullIfWhiteSpace(request.FiltroAno), NpgsqlDbType.Text);
        Add(command, "mes", NullIfWhiteSpace(request.FiltroMes), NpgsqlDbType.Text);
        Add(command, "inicio", request.FiltroInicio, NpgsqlDbType.Date);
        Add(command, "fim", request.FiltroFim, NpgsqlDbType.Date);
        await command.ExecuteNonQueryAsync();
    }

    private static PreferenciasUsuarioDto ReadPreferencias(NpgsqlDataReader reader)
        => new(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetBoolean(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetFieldValue<DateOnly>(7),
            reader.IsDBNull(8) ? null : reader.GetFieldValue<DateOnly>(8));

    private static void ValidateMeta(MetaRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Titulo)) throw new ArgumentException("Informe o título da meta.");
        if (request.ValorObjetivo <= 0) throw new ArgumentException("O valor objetivo deve ser maior que zero.");
        if (request.ValorGuardado < 0) throw new ArgumentException("O valor guardado não pode ser negativo.");
        if (request.ValorGuardado > request.ValorObjetivo) throw new ArgumentException("O valor guardado não pode ser maior que o objetivo.");
    }

    private static void ValidateMovimento(string reason, decimal value, DateOnly date)
    {
        if (string.IsNullOrWhiteSpace(reason)) throw new ArgumentException("Informe a descrição do lançamento.");
        if (value <= 0) throw new ArgumentException("O valor do lançamento deve ser maior que zero.");
        if (date.Year < 2000) throw new ArgumentException("Informe uma data válida para o lançamento.");
    }

    private static string NormalizeTipoCategoria(string? tipo)
    {
        var normalized = NormalizeKey(tipo ?? "ambos");

        return normalized switch
        {
            "entrada" or "entradas" or "receita" or "receitas" or "income" or "entry" or "in" => "entrada",
            "saida" or "saidas" or "despesa" or "despesas" or "expense" or "exit" or "out" => "saida",
            "ambos" or "both" or "all" or "todos" or "entrada e saida" or "receita e despesa" => "ambos",
            _ => "ambos"
        };
    }

    private static string NormalizeKey(string value)
    {
        var normalized = value.Trim().ToLowerInvariant().Normalize(NormalizationForm.FormD);
        var builder = new StringBuilder();

        foreach (var character in normalized)
        {
            if (CharUnicodeInfo.GetUnicodeCategory(character) != UnicodeCategory.NonSpacingMark)
            {
                builder.Append(character);
            }
        }

        return builder
            .ToString()
            .Replace("/", " ")
            .Replace("\\", " ")
            .Replace("_", " ")
            .Replace("-", " ")
            .Trim();
    }

    private static string NormalizeCategoria(string? categoria)
        => string.IsNullOrWhiteSpace(categoria) ? "Outros" : categoria.Trim();

    private static string? NullIfWhiteSpace(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static void EnsureTabelaFinanceira(string tabela)
    {
        if (tabela is not ("entradas" or "saidas"))
        {
            throw new ArgumentException("Tabela financeira inválida.");
        }
    }

    private static async Task<bool> ColumnExistsAsync(NpgsqlConnection connection, string tableName, string columnName)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select exists (
                select 1
                from information_schema.columns
                where table_schema = 'public'
                  and table_name = @tableName
                  and column_name = @columnName
            )
            """;
        Add(command, "tableName", tableName);
        Add(command, "columnName", columnName);

        return Convert.ToBoolean(await command.ExecuteScalarAsync());
    }

    private static void Add(NpgsqlCommand command, string name, object? value, NpgsqlDbType? dbType = null)
    {
        var parameter = dbType.HasValue
            ? command.Parameters.Add(name, dbType.Value)
            : command.Parameters.AddWithValue(name, value ?? DBNull.Value);

        parameter.Value = value ?? DBNull.Value;
    }
}
