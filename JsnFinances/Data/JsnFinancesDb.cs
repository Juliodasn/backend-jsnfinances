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

    public async Task EnsureApplicationSchemaAsync()
    {
        await using var connection = await _dataSource.OpenConnectionAsync();

        await using (var lockCommand = connection.CreateCommand())
        {
            lockCommand.CommandTimeout = 90;
            lockCommand.CommandText = "select pg_advisory_lock(hashtext('jsn_finances_schema_init'))";
            await lockCommand.ExecuteNonQueryAsync();
        }

        try
        {
            await EnsurePixBillingSchemaAsync(connection);
            await EnsurePerfilSchemaAsync(connection);
            await EnsurePreferenciasSchemaAsync(connection);
            await EnsureOnboardingSchemaAsync(connection);
            await EnsureAdminSchemaAsync(connection);
        }
        finally
        {
            await using var unlockCommand = connection.CreateCommand();
            unlockCommand.CommandTimeout = 90;
            unlockCommand.CommandText = "select pg_advisory_unlock(hashtext('jsn_finances_schema_init'))";
            await unlockCommand.ExecuteNonQueryAsync();
        }
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
        var hasSubcategoriaColumn = await ColumnExistsAsync(connection, tabela, "id_subcategoria");
        var contaSelect = hasContaColumn ? "m.id_conta" : "null::uuid as id_conta";
        var subcategoriaSelect = hasSubcategoriaColumn ? "m.id_subcategoria, sc.nome as subcategoria_nome" : "null::uuid as id_subcategoria, null::text as subcategoria_nome";
        var subcategoriaJoin = hasSubcategoriaColumn ? "left join public.subcategorias sc on sc.id = m.id_subcategoria and sc.id_usuario = m.id_usuario" : string.Empty;
        var contaFilter = hasContaColumn ? "and (@idConta is null or m.id_conta = @idConta)" : string.Empty;

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            select m.id, m.descricao, m.categoria, {subcategoriaSelect}, m.valor, m.data_movimentacao, m.observacao, {contaSelect}
            from public.{tabela} m
            {subcategoriaJoin}
            where m.id_usuario = @userId
              and (@inicio is null or m.data_movimentacao >= @inicio)
              and (@fim is null or m.data_movimentacao <= @fim)
              {contaFilter}
            order by m.data_movimentacao desc, m.criado_em desc
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
                reader.IsDBNull(3) ? null : reader.GetGuid(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetDecimal(5),
                reader.GetFieldValue<DateOnly>(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetGuid(8)
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
            select t.id, t.tipo, t.descricao, t.categoria, t.id_subcategoria, sc.nome as subcategoria_nome, t.valor, t.data_movimentacao, t.observacao, t.id_conta
            from (
              select id, 'entrada' as tipo, descricao, categoria, id_subcategoria, valor, data_movimentacao, observacao, id_conta, criado_em, id_usuario
              from public.entradas
              union all
              select id, 'saida' as tipo, descricao, categoria, id_subcategoria, valor, data_movimentacao, observacao, id_conta, criado_em, id_usuario
              from public.saidas
            ) t
            left join public.subcategorias sc on sc.id = t.id_subcategoria and sc.id_usuario = t.id_usuario
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
                reader.IsDBNull(4) ? null : reader.GetGuid(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetDecimal(6),
                reader.GetFieldValue<DateOnly>(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetGuid(9)
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
            insert into public.{tabela} (id_usuario, descricao, categoria, id_subcategoria, valor, data_movimentacao, observacao, id_conta)
            values (@userId, @descricao, @categoria, @idSubcategoria, @valor, @data, @observacao, @idConta)
            returning id
            """;
        Add(command, "userId", userId);
        Add(command, "descricao", request.Reason.Trim());
        Add(command, "categoria", NormalizeCategoria(request.Category));
        Add(command, "idSubcategoria", request.SubcategoryId, NpgsqlDbType.Uuid);
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
                insert into public.{tabela} (id_usuario, descricao, categoria, id_subcategoria, valor, data_movimentacao, observacao, id_conta)
                values (@userId, @descricao, @categoria, @idSubcategoria, @valor, @data, @observacao, @idConta)
                """;
            Add(command, "userId", userId);
            Add(command, "descricao", request.Reason.Trim());
            Add(command, "categoria", NormalizeCategoria(request.Category));
            Add(command, "idSubcategoria", request.SubcategoryId, NpgsqlDbType.Uuid);
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
                   id_subcategoria = @idSubcategoria,
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
        Add(command, "idSubcategoria", request.SubcategoryId, NpgsqlDbType.Uuid);
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

    public async Task<IReadOnlyList<SubcategoriaDto>> ListSubcategoriasAsync(Guid userId, Guid? idCategoria = null)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select sc.id, sc.id_categoria, sc.nome, sc.descricao, sc.cor, sc.icone, sc.ativo, c.nome, c.tipo
            from public.subcategorias sc
            inner join public.categorias c on c.id = sc.id_categoria and c.id_usuario = sc.id_usuario
            where sc.id_usuario = @userId
              and (@idCategoria is null or sc.id_categoria = @idCategoria)
            order by c.nome asc, sc.nome asc
            """;
        Add(command, "userId", userId);
        Add(command, "idCategoria", idCategoria, NpgsqlDbType.Uuid);

        var rows = new List<SubcategoriaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new SubcategoriaDto(
                reader.GetGuid(0),
                reader.GetGuid(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetBoolean(6),
                reader.GetString(7),
                reader.GetString(8)));
        }

        return rows;
    }

    public async Task<Guid> InsertSubcategoriaAsync(Guid userId, SubcategoriaRequest request)
    {
        if (request.IdCategoria == Guid.Empty) throw new ArgumentException("Selecione a categoria da subcategoria.");
        if (string.IsNullOrWhiteSpace(request.Nome)) throw new ArgumentException("Informe o nome da subcategoria.");

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.subcategorias (id_usuario, id_categoria, nome, descricao, cor, icone, ativo)
            select @userId, c.id, @nome, @descricao, @cor, @icone, @ativo
            from public.categorias c
            where c.id = @idCategoria and c.id_usuario = @userId
            returning id
            """;
        Add(command, "userId", userId);
        Add(command, "idCategoria", request.IdCategoria, NpgsqlDbType.Uuid);
        Add(command, "nome", request.Nome.Trim());
        Add(command, "descricao", NullIfWhiteSpace(request.Descricao));
        Add(command, "cor", NullIfWhiteSpace(request.Cor));
        Add(command, "icone", NullIfWhiteSpace(request.Icone));
        Add(command, "ativo", request.Ativo);

        var result = await command.ExecuteScalarAsync();
        if (result is null)
        {
            throw new InvalidOperationException("Categoria não encontrada para criar a subcategoria.");
        }

        return (Guid)result;
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
        await using var transaction = await connection.BeginTransactionAsync();

        await using var existsCommand = connection.CreateCommand();
        existsCommand.Transaction = transaction;
        existsCommand.CommandText = """
            select 1
            from public.contas
            where id = @id and id_usuario = @userId
            limit 1
            """;
        Add(existsCommand, "id", id);
        Add(existsCommand, "userId", userId);

        if (await existsCommand.ExecuteScalarAsync() is null)
        {
            throw new InvalidOperationException("Conta não encontrada para o usuário atual.");
        }

        await using (var unlinkEntradas = connection.CreateCommand())
        {
            unlinkEntradas.Transaction = transaction;
            unlinkEntradas.CommandText = """
                update public.entradas
                set id_conta = null
                where id_usuario = @userId and id_conta = @id
                """;
            Add(unlinkEntradas, "id", id);
            Add(unlinkEntradas, "userId", userId);
            await unlinkEntradas.ExecuteNonQueryAsync();
        }

        await using (var unlinkSaidas = connection.CreateCommand())
        {
            unlinkSaidas.Transaction = transaction;
            unlinkSaidas.CommandText = """
                update public.saidas
                set id_conta = null
                where id_usuario = @userId and id_conta = @id
                """;
            Add(unlinkSaidas, "id", id);
            Add(unlinkSaidas, "userId", userId);
            await unlinkSaidas.ExecuteNonQueryAsync();
        }

        await using (var deleteTransfers = connection.CreateCommand())
        {
            deleteTransfers.Transaction = transaction;
            deleteTransfers.CommandText = """
                delete from public.transferencias_contas
                where id_usuario = @userId
                  and (id_conta_origem = @id or id_conta_destino = @id)
                """;
            Add(deleteTransfers, "id", id);
            Add(deleteTransfers, "userId", userId);
            await deleteTransfers.ExecuteNonQueryAsync();
        }

        await using (var deleteAccount = connection.CreateCommand())
        {
            deleteAccount.Transaction = transaction;
            deleteAccount.CommandText = """
                delete from public.contas
                where id = @id and id_usuario = @userId
                """;
            Add(deleteAccount, "id", id);
            Add(deleteAccount, "userId", userId);
            await deleteAccount.ExecuteNonQueryAsync();
        }

        await transaction.CommitAsync();
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
                    null,
                    null,
                    valor,
                    data,
                    string.IsNullOrWhiteSpace(observacao) ? "Movimentação entre contas próprias. Não altera o saldo total." : observacao,
                    null));
            }
        }

        return entradas
            .Select(x => new CalendarioMovimentacaoDto(x.Id, "entrada", x.Descricao, x.Categoria, x.IdSubcategoria, x.SubcategoriaNome, x.Valor, x.DataMovimentacao, x.Observacao, x.IdConta))
            .Concat(saidas.Select(x => new CalendarioMovimentacaoDto(x.Id, "saida", x.Descricao, x.Categoria, x.IdSubcategoria, x.SubcategoriaNome, x.Valor, x.DataMovimentacao, x.Observacao, x.IdConta)))
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
            null,
            null,
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


    private static async Task EnsureOnboardingSchemaAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandTimeout = 90;
        command.CommandText = """
            create extension if not exists pgcrypto;

            create table if not exists public.user_onboarding (
                id uuid primary key default gen_random_uuid(),
                user_id uuid not null unique,
                current_step integer not null default 1,
                completed boolean not null default false,
                skipped boolean not null default false,
                completed_at timestamptz null,
                created_at timestamptz not null default timezone('utc', now()),
                updated_at timestamptz not null default timezone('utc', now())
            );

            alter table public.user_onboarding add column if not exists id uuid default gen_random_uuid();
            alter table public.user_onboarding add column if not exists user_id uuid;
            alter table public.user_onboarding add column if not exists current_step integer not null default 1;
            alter table public.user_onboarding add column if not exists completed boolean not null default false;
            alter table public.user_onboarding add column if not exists skipped boolean not null default false;
            alter table public.user_onboarding add column if not exists completed_at timestamptz null;
            alter table public.user_onboarding add column if not exists created_at timestamptz not null default timezone('utc', now());
            alter table public.user_onboarding add column if not exists updated_at timestamptz not null default timezone('utc', now());
            update public.user_onboarding set id = gen_random_uuid() where id is null;
            create unique index if not exists ux_user_onboarding_user_id on public.user_onboarding(user_id);

            create table if not exists public.user_onboarding_profile (
                id uuid primary key default gen_random_uuid(),
                id_usuario uuid not null unique,
                profile_type text not null default 'Pessoal',
                main_goal text not null default 'Controlar gastos',
                financial_moment text not null default 'Quero me organizar melhor',
                biggest_challenge text not null default 'Cartão de crédito',
                usage_frequency text not null default 'Algumas vezes por semana',
                created_at timestamptz not null default timezone('utc', now()),
                updated_at timestamptz not null default timezone('utc', now())
            );

            alter table public.user_onboarding_profile add column if not exists id uuid default gen_random_uuid();
            alter table public.user_onboarding_profile add column if not exists id_usuario uuid;
            alter table public.user_onboarding_profile add column if not exists profile_type text not null default 'Pessoal';
            alter table public.user_onboarding_profile add column if not exists main_goal text not null default 'Controlar gastos';
            alter table public.user_onboarding_profile add column if not exists financial_moment text not null default 'Quero me organizar melhor';
            alter table public.user_onboarding_profile add column if not exists biggest_challenge text not null default 'Cartão de crédito';
            alter table public.user_onboarding_profile add column if not exists usage_frequency text not null default 'Algumas vezes por semana';
            alter table public.user_onboarding_profile add column if not exists created_at timestamptz not null default timezone('utc', now());
            alter table public.user_onboarding_profile add column if not exists updated_at timestamptz not null default timezone('utc', now());
            update public.user_onboarding_profile set id = gen_random_uuid() where id is null;
            create unique index if not exists ux_user_onboarding_profile_id_usuario on public.user_onboarding_profile(id_usuario);
            create index if not exists ix_user_onboarding_profile_updated_at on public.user_onboarding_profile(updated_at desc);
            """;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task EnsureAdminSchemaAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandTimeout = 90;
        command.CommandText = """
            create extension if not exists pgcrypto;

            create table if not exists public.admin_access_logs (
                id uuid primary key default gen_random_uuid(),
                id_usuario uuid not null,
                email text not null,
                path text not null,
                method text not null,
                ip_address text null,
                user_agent text null,
                accessed_at timestamptz not null default timezone('utc', now())
            );

            alter table public.admin_access_logs add column if not exists id uuid default gen_random_uuid();
            alter table public.admin_access_logs add column if not exists id_usuario uuid;
            alter table public.admin_access_logs add column if not exists email text not null default '';
            alter table public.admin_access_logs add column if not exists path text not null default '';
            alter table public.admin_access_logs add column if not exists method text not null default 'GET';
            alter table public.admin_access_logs add column if not exists ip_address text null;
            alter table public.admin_access_logs add column if not exists user_agent text null;
            alter table public.admin_access_logs add column if not exists accessed_at timestamptz not null default timezone('utc', now());
            update public.admin_access_logs set id = gen_random_uuid() where id is null;
            create index if not exists ix_admin_access_logs_user on public.admin_access_logs(id_usuario, accessed_at desc);
            create index if not exists ix_admin_access_logs_email on public.admin_access_logs(lower(email), accessed_at desc);

            create table if not exists public.admin_user_actions (
                id uuid primary key default gen_random_uuid(),
                admin_user_id uuid not null,
                admin_email text not null,
                target_user_id uuid not null,
                action text not null,
                reason text null,
                metadata jsonb not null default '{}'::jsonb,
                created_at timestamptz not null default timezone('utc', now())
            );

            alter table public.admin_user_actions add column if not exists id uuid default gen_random_uuid();
            alter table public.admin_user_actions add column if not exists admin_user_id uuid;
            alter table public.admin_user_actions add column if not exists admin_email text not null default '';
            alter table public.admin_user_actions add column if not exists target_user_id uuid;
            alter table public.admin_user_actions add column if not exists action text not null default 'unknown';
            alter table public.admin_user_actions add column if not exists reason text null;
            alter table public.admin_user_actions add column if not exists metadata jsonb not null default '{}'::jsonb;
            alter table public.admin_user_actions add column if not exists created_at timestamptz not null default timezone('utc', now());
            update public.admin_user_actions set id = gen_random_uuid() where id is null;
            create index if not exists ix_admin_user_actions_target on public.admin_user_actions(target_user_id, created_at desc);
            create index if not exists ix_admin_user_actions_admin on public.admin_user_actions(admin_user_id, created_at desc);
            """;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task EnsurePerfilSchemaAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandTimeout = 90;
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

    private static async Task EnsurePreferenciasSchemaAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandTimeout = 90;
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
                helpers_ocultos text[] not null default '{}'::text[],
                helpers_vistos text[] not null default '{}'::text[],
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
            alter table public.preferencias_usuario add column if not exists helpers_ocultos text[] not null default '{}'::text[];
            alter table public.preferencias_usuario add column if not exists helpers_vistos text[] not null default '{}'::text[];
            alter table public.preferencias_usuario add column if not exists criado_em timestamptz not null default timezone('utc', now());
            alter table public.preferencias_usuario add column if not exists atualizado_em timestamptz not null default timezone('utc', now());

            update public.preferencias_usuario
               set tema = coalesce(nullif(trim(tema), ''), 'claro'),
                   cor_principal = coalesce(nullif(trim(cor_principal), ''), 'purple'),
                   moeda = coalesce(nullif(trim(moeda), ''), 'BRL'),
                   idioma = coalesce(nullif(trim(idioma), ''), 'pt-BR'),
                   modo_nucleo = coalesce(modo_nucleo, false),
                   helpers_ocultos = coalesce(helpers_ocultos, '{}'::text[]),
                   helpers_vistos = coalesce(helpers_vistos, '{}'::text[]),
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
                or helpers_ocultos is null
                or helpers_vistos is null
                or criado_em is null
                or atualizado_em is null;

            create unique index if not exists ux_preferencias_usuario_id_usuario
            on public.preferencias_usuario (id_usuario);
            """;
        await command.ExecuteNonQueryAsync();
    }


    public async Task<PerfilDto?> GetPerfilAsync(Guid userId)
    {
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
        await EnsurePreferenciasAsync(userId, null);

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select tema, cor_principal, moeda, idioma, modo_nucleo, filtro_ano, filtro_mes, filtro_inicio, filtro_fim, helpers_ocultos, helpers_vistos
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
                  (id_usuario, tema, cor_principal, moeda, idioma, modo_nucleo, filtro_ano, filtro_mes, filtro_inicio, filtro_fim, helpers_ocultos, helpers_vistos)
                values
                  (@userId, 'claro', 'purple', 'BRL', 'pt-BR', false, null, null, null, null, '{}'::text[], '{}'::text[])
                on conflict (id_usuario) do nothing
                """;
            Add(command, "userId", userId);
            await command.ExecuteNonQueryAsync();
            return;
        }

        command.CommandText = """
            insert into public.preferencias_usuario
              (id_usuario, tema, cor_principal, moeda, idioma, modo_nucleo, filtro_ano, filtro_mes, filtro_inicio, filtro_fim, helpers_ocultos, helpers_vistos)
            values
              (@userId, coalesce(@tema, 'claro'), coalesce(@cor, 'purple'), coalesce(@moeda, 'BRL'), coalesce(@idioma, 'pt-BR'), coalesce(@modo, false), @ano, @mes, @inicio, @fim, coalesce(@helpersOcultos, '{}'::text[]), coalesce(@helpersVistos, '{}'::text[]))
            on conflict (id_usuario)
            do update set tema = coalesce(@tema, preferencias_usuario.tema),
                          cor_principal = coalesce(@cor, preferencias_usuario.cor_principal),
                          moeda = coalesce(@moeda, preferencias_usuario.moeda),
                          idioma = coalesce(@idioma, preferencias_usuario.idioma),
                          modo_nucleo = coalesce(@modo, preferencias_usuario.modo_nucleo),
                          filtro_ano = coalesce(@ano, preferencias_usuario.filtro_ano),
                          filtro_mes = coalesce(@mes, preferencias_usuario.filtro_mes),
                          filtro_inicio = coalesce(@inicio, preferencias_usuario.filtro_inicio),
                          filtro_fim = coalesce(@fim, preferencias_usuario.filtro_fim),
                          helpers_ocultos = coalesce(@helpersOcultos, preferencias_usuario.helpers_ocultos),
                          helpers_vistos = coalesce(@helpersVistos, preferencias_usuario.helpers_vistos),
                          atualizado_em = timezone('utc', now())
            """;
        Add(command, "userId", userId);
        Add(command, "tema", NullIfWhiteSpace(request.Tema), NpgsqlDbType.Text);
        Add(command, "cor", NullIfWhiteSpace(request.CorPrincipal), NpgsqlDbType.Text);
        Add(command, "moeda", NullIfWhiteSpace(request.Moeda), NpgsqlDbType.Text);
        Add(command, "idioma", NullIfWhiteSpace(request.Idioma), NpgsqlDbType.Text);
        Add(command, "modo", request.ModoNucleo, NpgsqlDbType.Boolean);
        Add(command, "ano", NullIfWhiteSpace(request.FiltroAno), NpgsqlDbType.Text);
        Add(command, "mes", NullIfWhiteSpace(request.FiltroMes), NpgsqlDbType.Text);
        Add(command, "inicio", request.FiltroInicio, NpgsqlDbType.Date);
        Add(command, "fim", request.FiltroFim, NpgsqlDbType.Date);
        Add(command, "helpersOcultos", NormalizeHelperKeys(request.HelpersOcultos), NpgsqlDbType.Array | NpgsqlDbType.Text);
        Add(command, "helpersVistos", NormalizeHelperKeys(request.HelpersVistos), NpgsqlDbType.Array | NpgsqlDbType.Text);
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
            reader.IsDBNull(8) ? null : reader.GetFieldValue<DateOnly>(8),
            reader.IsDBNull(9) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(9),
            reader.IsDBNull(10) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(10));

    private static string[]? NormalizeHelperKeys(string[]? keys)
        => keys is null
            ? null
            : keys.Select(key => NullIfWhiteSpace(key))
                .Where(key => key is not null)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Select(key => key!)
                .ToArray();

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
    var normalizedValue = NormalizeDbValue(value);

    var parameter = dbType.HasValue
        ? command.Parameters.Add(name, dbType.Value)
        : command.Parameters.AddWithValue(name, normalizedValue ?? DBNull.Value);

    parameter.Value = normalizedValue ?? DBNull.Value;
}

private static object? NormalizeDbValue(object? value)
{
    if (value is null)
        return null;

    if (value is DateTimeOffset dateTimeOffset)
        return dateTimeOffset.ToUniversalTime();

    if (value is DateTime dateTime)
    {
        if (dateTime.Kind == DateTimeKind.Utc)
            return dateTime;

        return DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
    }

    return value;
}
}
