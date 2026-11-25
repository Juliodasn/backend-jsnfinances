using JsnFinances.Api.Domain;
using NpgsqlTypes;
using Npgsql;
using System.Text.Json;

namespace JsnFinances.Api.Data;

public sealed partial class JsnFinancesDb
{
    public async Task<IReadOnlyList<BoletoDto>> ListBoletosAsync(Guid userId, DateOnly? inicio = null, DateOnly? fim = null)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var tx = await connection.BeginTransactionAsync();
        await LockBoletosAsync(connection, tx, userId);
        var today = BoletoToday();
        var month = new DateOnly(today.Year, today.Month, 1);
        var monthEnd = month.AddMonths(1).AddDays(-1);
        var forecastEnd = month.AddMonths(4).AddDays(-1);
        if (inicio.HasValue != fim.HasValue || (inicio.HasValue && (fim!.Value < inicio.Value || inicio.Value.Year<1900 || fim.Value.Year>9900 || fim.Value.DayNumber-inicio.Value.DayNumber > 3660)))
            throw new ArgumentException("Período inválido para consulta de boletos.");
        await SyncFixedAsync(connection, tx, userId, monthEnd);
        await using var command = connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = """
            select b.id, b.descricao, b.beneficiario, b.categoria, b.id_subcategoria,
                   sc.nome as subcategoria_nome, b.tipo, b.valor_original, b.frequencia,
                   b.quantidade_parcelas, b.data_inicio, b.data_fim, b.id_conta_padrao,
                   c.nome as conta_padrao_nome, b.modo_pagamento, b.dias_lembrete,
                   b.codigo_barras, b.observacao, b.ativo
              from public.boletos b
              left join public.subcategorias sc on sc.id = b.id_subcategoria
              left join public.contas c on c.id = b.id_conta_padrao
             where b.id_usuario = @userId
             order by b.ativo desc, b.data_inicio desc, b.descricao
            """;
        Add(command, "userId", userId);
        var result = new List<BoletoDto>();
        await using var reader = await command.ExecuteReaderAsync();
        var headers = new List<(Guid Id,string Descricao,string? Beneficiario,string? Categoria,Guid? SubId,string? SubNome,string Tipo,decimal Valor,string Frequencia,int Qtd,DateOnly Inicio,DateOnly? Fim,Guid? ContaId,string? ContaNome,string Modo,int Dias,string? Codigo,string? Obs,bool Ativo)>();
        while (await reader.ReadAsync()) headers.Add((reader.GetGuid(0), reader.GetString(1), reader.IsDBNull(2)?null:reader.GetString(2), reader.IsDBNull(3)?null:reader.GetString(3), reader.IsDBNull(4)?null:reader.GetGuid(4), reader.IsDBNull(5)?null:reader.GetString(5), reader.GetString(6), reader.GetDecimal(7), reader.GetString(8), reader.GetInt32(9), reader.GetFieldValue<DateOnly>(10), reader.IsDBNull(11)?null:reader.GetFieldValue<DateOnly>(11), reader.IsDBNull(12)?null:reader.GetGuid(12), reader.IsDBNull(13)?null:reader.GetString(13), reader.GetString(14), reader.GetInt32(15), reader.IsDBNull(16)?null:reader.GetString(16), reader.IsDBNull(17)?null:reader.GetString(17), reader.GetBoolean(18)));
        await reader.CloseAsync();
        foreach (var h in headers)
        {
            var parcelas = await ListBoletoParcelasAsync(connection, tx, userId, h.Id, monthEnd, forecastEnd, inicio, fim);
            result.Add(new BoletoDto(h.Id,h.Descricao,h.Beneficiario,h.Categoria,h.SubId,h.SubNome,h.Tipo,h.Valor,h.Frequencia,h.Qtd,h.Inicio,h.Fim,h.ContaId,h.ContaNome,h.Modo,h.Dias,h.Codigo,h.Obs,h.Ativo,parcelas));
        }
        await tx.CommitAsync();
        return result;
    }

    private static async Task<IReadOnlyList<BoletoParcelaDto>> ListBoletoParcelasAsync(NpgsqlConnection connection, NpgsqlTransaction tx, Guid userId, Guid boletoId, DateOnly monthEnd, DateOnly forecastEnd, DateOnly? inicio, DateOnly? fim)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = """
          with previstas as materialized (
             select v.* from public.boletos b cross join lateral public.boleto_previsoes(@userId,@boletoId,
                greatest(@forecastEnd,coalesce(@fim,@forecastEnd),(greatest(b.data_inicio,@monthEnd)+interval '1 year')::date)) v
             where b.id=@boletoId and b.id_usuario=@userId and b.tipo='fixo' and b.ativo
               and v.vencimento>@monthEnd and not exists(select 1 from public.boleto_parcelas p where p.id_boleto=b.id and p.numero_parcela=v.numero)
          )
          select id,id_boleto,numero_parcela,total_parcelas,valor_previsto,data_vencimento,status,
                 data_pagamento,valor_pago,juros,multa,desconto,id_conta_pagamento,id_saida_gerada,
                 pagamento_automatico,observacao_pagamento,false as virtual,dados_ocorrencia::text
            from public.boleto_parcelas
           where id_usuario=@userId and id_boleto=@boletoId
          union all
          select md5(@boletoId::text || ':' || v.numero::text)::uuid,@boletoId,v.numero,1,v.valor,v.vencimento,v.status,
                 null::date,null::numeric,0,0,0,null::uuid,null::uuid,false,null::text,true,v.dados::text
            from previstas v
           where v.vencimento<=@forecastEnd or v.vencimento between @inicio and @fim
                 or v.numero=(select min(n.numero) from previstas n where n.status='pendente')
           order by numero_parcela
        """;
        Add(command,"userId",userId); Add(command,"boletoId",boletoId);
        Add(command,"monthEnd",monthEnd,NpgsqlDbType.Date); Add(command,"forecastEnd",forecastEnd,NpgsqlDbType.Date);
        Add(command,"inicio",inicio,NpgsqlDbType.Date); Add(command,"fim",fim,NpgsqlDbType.Date);
        var rows = new List<BoletoParcelaDto>();
        await using var reader = await command.ExecuteReaderAsync();
        while(await reader.ReadAsync()) rows.Add(new BoletoParcelaDto(reader.GetGuid(0),reader.GetGuid(1),reader.GetInt32(2),reader.GetInt32(3),reader.GetDecimal(4),reader.GetFieldValue<DateOnly>(5),reader.GetString(6),reader.IsDBNull(7)?null:reader.GetFieldValue<DateOnly>(7),reader.IsDBNull(8)?null:reader.GetDecimal(8),reader.GetDecimal(9),reader.GetDecimal(10),reader.GetDecimal(11),reader.IsDBNull(12)?null:reader.GetGuid(12),reader.IsDBNull(13)?null:reader.GetGuid(13),reader.GetBoolean(14),reader.IsDBNull(15)?null:reader.GetString(15),reader.GetBoolean(16),reader.IsDBNull(17)?null:JsonSerializer.Deserialize<JsonElement>(reader.GetString(17))));
        return rows;
    }

    public static DateOnly BoletoToday() => DateOnly.FromDateTime(TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("America/Sao_Paulo")));
    private static DateOnly BoletoMonthEnd() { var today=BoletoToday(); return new DateOnly(today.Year,today.Month,1).AddMonths(1).AddDays(-1); }

    private static async Task LockBoletosAsync(NpgsqlConnection c, NpgsqlTransaction tx, Guid userId)
    {
        await using var cmd=c.CreateCommand(); cmd.Transaction=tx;
        cmd.CommandText="select pg_advisory_xact_lock(hashtextextended(@userId::text, 87419))";
        Add(cmd,"userId",userId); await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SyncFixedAsync(NpgsqlConnection c, NpgsqlTransaction tx, Guid userId, DateOnly through)
    {
        await using var cmd=c.CreateCommand(); cmd.Transaction=tx;
        cmd.CommandText="""
            insert into public.boleto_parcelas(id_usuario,id_boleto,numero_parcela,total_parcelas,valor_previsto,data_vencimento,dados_ocorrencia,status)
            select b.id_usuario,b.id,v.numero,1,v.valor,v.vencimento,v.dados,v.status
            from public.boletos b cross join lateral public.boleto_previsoes(b.id_usuario,b.id,@through) v
            where b.id_usuario=@userId and b.tipo='fixo' and b.ativo
            on conflict(id_boleto,numero_parcela) do nothing
            """;
        Add(cmd,"userId",userId); Add(cmd,"through",through,NpgsqlDbType.Date); await cmd.ExecuteNonQueryAsync();
        cmd.CommandText="delete from public.boleto_excecoes e using public.boleto_parcelas p where e.id_usuario=@userId and p.id_usuario=e.id_usuario and p.id_boleto=e.id_boleto and p.numero_parcela=e.numero";
        await cmd.ExecuteNonQueryAsync();
    }

    private sealed record BoletoOccurrence(Guid? Id, DateOnly Due, string Status, decimal Value, string Dados);

    private static async Task<BoletoOccurrence> FindOccurrenceAsync(NpgsqlConnection c,NpgsqlTransaction tx,Guid userId,Guid id,int numero)
    {
        if(numero<1 || numero>600000) throw new ArgumentException("Número de ocorrência inválido.");
        await using var cmd=c.CreateCommand(); cmd.Transaction=tx;
        Add(cmd,"userId",userId); Add(cmd,"id",id); Add(cmd,"numero",numero);
        cmd.CommandText="""
            select p.id,p.data_vencimento,p.status,p.valor_previsto,coalesce(p.dados_ocorrencia,to_jsonb(b))::text
            from public.boleto_parcelas p join public.boletos b on b.id=p.id_boleto and b.id_usuario=p.id_usuario
            where p.id_usuario=@userId and p.id_boleto=@id and p.numero_parcela=@numero
            """;
        await using(var reader=await cmd.ExecuteReaderAsync())
            if(await reader.ReadAsync()) return new(reader.GetGuid(0),reader.GetFieldValue<DateOnly>(1),reader.GetString(2),reader.GetDecimal(3),reader.GetString(4));
        // Calcula a data em .NET antes de consultar a projeção para rejeitar índices fora do calendário suportado.
        cmd.CommandText="select data_inicio,frequencia from public.boletos where id=@id and id_usuario=@userId and tipo='fixo' and ativo";
        DateOnly due;
        await using(var reader=await cmd.ExecuteReaderAsync())
        {
            if(!await reader.ReadAsync()) throw new InvalidOperationException("Ocorrência não encontrada.");
            try { due=AdvanceDate(reader.GetFieldValue<DateOnly>(0),numero-1,reader.GetString(1)); }
            catch(ArgumentOutOfRangeException) { throw new ArgumentException("Vencimento fora do calendário suportado."); }
            if(due.Year>9900) throw new ArgumentException("Vencimento fora do calendário suportado.");
        }
        Add(cmd,"due",due,NpgsqlDbType.Date);
        cmd.CommandText="""
            select v.vencimento,v.status,v.valor,v.dados::text from public.boleto_previsoes(@userId,@id,
              greatest(@due,coalesce((select vencimento from public.boleto_excecoes where id_boleto=@id and id_usuario=@userId and numero=@numero),@due))) v
            where v.numero=@numero
            """;
        await using(var reader=await cmd.ExecuteReaderAsync())
            if(await reader.ReadAsync()) return new(null,reader.GetFieldValue<DateOnly>(0),reader.GetString(1),reader.GetDecimal(2),reader.GetString(3));
        throw new InvalidOperationException("Ocorrência não encontrada.");
    }

    // Materialização antecipada só é necessária para registrar um pagamento real.
    private static async Task<Guid> MaterializeAsync(NpgsqlConnection c,NpgsqlTransaction tx,Guid userId,Guid id,int numero)
    {
        var occurrence=await FindOccurrenceAsync(c,tx,userId,id,numero);
        if(occurrence.Id.HasValue) return occurrence.Id.Value;
        if(occurrence.Status!="pendente") throw new InvalidOperationException("Ocorrência cancelada.");
        await using var cmd=c.CreateCommand(); cmd.Transaction=tx;
        cmd.CommandText="""
            insert into public.boleto_parcelas(id_usuario,id_boleto,numero_parcela,total_parcelas,valor_previsto,data_vencimento,dados_ocorrencia,status)
            values(@userId,@id,@numero,1,@valor,@due,@dados,'pendente') returning id
            """;
        Add(cmd,"userId",userId); Add(cmd,"id",id); Add(cmd,"numero",numero); Add(cmd,"valor",occurrence.Value);
        Add(cmd,"due",occurrence.Due,NpgsqlDbType.Date); Add(cmd,"dados",occurrence.Dados,NpgsqlDbType.Jsonb);
        return (Guid)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task UpsertBoletoExceptionAsync(NpgsqlConnection c,NpgsqlTransaction tx,Guid userId,Guid id,int numero,DateOnly due,decimal value,string dados,string status)
    {
        await using var cmd=c.CreateCommand(); cmd.Transaction=tx;
        cmd.CommandText="""
            insert into public.boleto_excecoes(id_boleto,id_usuario,numero,vencimento,valor,dados,status)
            values(@id,@userId,@numero,@due,@valor,@dados,@status)
            on conflict(id_boleto,numero) do update set vencimento=excluded.vencimento,valor=excluded.valor,dados=excluded.dados,status=excluded.status
            where boleto_excecoes.id_usuario=excluded.id_usuario
            """;
        Add(cmd,"userId",userId); Add(cmd,"id",id); Add(cmd,"numero",numero); Add(cmd,"due",due,NpgsqlDbType.Date); Add(cmd,"valor",value); Add(cmd,"dados",dados,NpgsqlDbType.Jsonb); Add(cmd,"status",status);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task ValidateBoletoReferencesAsync(NpgsqlConnection c,NpgsqlTransaction tx,Guid userId,Guid? account,Guid? subcategory)
    {
        await using var cmd=c.CreateCommand(); cmd.Transaction=tx;
        cmd.CommandText="select (@conta is null or exists(select 1 from public.contas where id=@conta and id_usuario=@userId)) and (@sub is null or exists(select 1 from public.subcategorias where id=@sub and id_usuario=@userId))";
        Add(cmd,"userId",userId); Add(cmd,"conta",account,NpgsqlDbType.Uuid); Add(cmd,"sub",subcategory,NpgsqlDbType.Uuid);
        if(await cmd.ExecuteScalarAsync() is not true) throw new ArgumentException("Conta ou subcategoria inválida para este usuário.");
    }

    public async Task<Guid> InsertBoletoAsync(Guid userId, BoletoRequest request)
    {
        ValidateBoleto(request);
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var tx = await connection.BeginTransactionAsync();
        await LockBoletosAsync(connection, tx, userId);
        await ValidateBoletoReferencesAsync(connection,tx,userId,request.IdContaPadrao,request.IdSubcategoria);
        var id = Guid.NewGuid();
        await using (var command = connection.CreateCommand())
        {
            command.Transaction=tx;
            command.CommandText="""insert into public.boletos(id,id_usuario,descricao,beneficiario,categoria,id_subcategoria,tipo,valor_original,frequencia,quantidade_parcelas,data_inicio,data_fim,id_conta_padrao,modo_pagamento,dias_lembrete,codigo_barras,observacao,ativo) values(@id,@userId,@descricao,@beneficiario,@categoria,@sub,@tipo,@valor,@freq,@qtd,@inicio,@fim,@conta,@modo,@dias,@codigo,@obs,@ativo)""";
            Add(command,"id",id); Add(command,"userId",userId); Add(command,"descricao",request.Descricao.Trim()); Add(command,"beneficiario",NullIfWhiteSpace(request.Beneficiario),NpgsqlDbType.Text); Add(command,"categoria",NormalizeCategoria(request.Categoria)); Add(command,"sub",request.IdSubcategoria,NpgsqlDbType.Uuid); Add(command,"tipo",NormalizeBoletoTipo(request.Tipo)); Add(command,"valor",request.ValorOriginal); Add(command,"freq",NormalizeBoletoFrequencia(request.Frequencia)); Add(command,"qtd",ResolveQuantidade(request)); Add(command,"inicio",request.DataInicio,NpgsqlDbType.Date); Add(command,"fim",NormalizeBoletoTipo(request.Tipo)=="fixo" ? null : request.DataFim,NpgsqlDbType.Date); Add(command,"conta",request.IdContaPadrao,NpgsqlDbType.Uuid); Add(command,"modo",NormalizeBoletoModo(request.ModoPagamento)); Add(command,"dias",Math.Clamp(request.DiasLembrete,0,90)); Add(command,"codigo",NullIfWhiteSpace(request.CodigoBarras),NpgsqlDbType.Text); Add(command,"obs",NullIfWhiteSpace(request.Observacao),NpgsqlDbType.Text); Add(command,"ativo",request.Ativo);
            await command.ExecuteNonQueryAsync();
        }
        await using (var rule=connection.CreateCommand())
        {
            rule.Transaction=tx; rule.CommandText="insert into public.boleto_regras(id_boleto,id_usuario,numero_inicio,dados) select id,id_usuario,1,to_jsonb(b) from public.boletos b where id=@id and id_usuario=@userId";
            Add(rule,"id",id); Add(rule,"userId",userId); await rule.ExecuteNonQueryAsync();
        }
        if(NormalizeBoletoTipo(request.Tipo)=="fixo") await SyncFixedAsync(connection,tx,userId,BoletoMonthEnd());
        else await RebuildBoletoParcelasAsync(connection,tx,userId,id,request);
        await tx.CommitAsync();
        return id;
    }

    public async Task UpdateBoletoAsync(Guid userId, Guid id, BoletoRequest request)
    {
        ValidateBoleto(request);
        var scope=request.Escopo ?? "atual";
        if(scope is not ("atual" or "futuros" or "todos")) throw new ArgumentException("Escopo de edição inválido.");
        await using var connection=await _dataSource.OpenConnectionAsync(); await using var tx=await connection.BeginTransactionAsync();
        await LockBoletosAsync(connection,tx,userId);
        await SyncFixedAsync(connection,tx,userId,BoletoMonthEnd());
        string tipo;
        await using(var get=connection.CreateCommand())
        {
            get.Transaction=tx; get.CommandText="select tipo from public.boletos where id=@id and id_usuario=@userId";
            Add(get,"id",id); Add(get,"userId",userId);
            tipo=await get.ExecuteScalarAsync() as string ?? throw new InvalidOperationException("Boleto não encontrado.");
        }
        if(tipo!=NormalizeBoletoTipo(request.Tipo)) throw new ArgumentException("O tipo da série não pode ser alterado.");
        var numero=request.NumeroParcela ?? (tipo=="unico" ? 1 : throw new ArgumentException("Selecione uma ocorrência."));
        // Alterações de série mantêm o calendário original; a data individual só muda no escopo atual.
        if(tipo=="unico") scope="atual";
        var occurrence=await FindOccurrenceAsync(connection,tx,userId,id,numero);
        if(occurrence.Status=="cancelado") throw new InvalidOperationException("Ocorrência cancelada.");
        await ValidateBoletoReferencesAsync(connection,tx,userId,request.IdContaPadrao,request.IdSubcategoria);
        var dados=JsonSerializer.Serialize(new {
            descricao=request.Descricao.Trim(), beneficiario=NullIfWhiteSpace(request.Beneficiario), categoria=NormalizeCategoria(request.Categoria),
            id_subcategoria=request.IdSubcategoria, valor_original=request.ValorOriginal, id_conta_padrao=request.IdContaPadrao,
            modo_pagamento=NormalizeBoletoModo(request.ModoPagamento), dias_lembrete=Math.Clamp(request.DiasLembrete,0,90),
            codigo_barras=NullIfWhiteSpace(request.CodigoBarras), observacao=NullIfWhiteSpace(request.Observacao)
        });
        var start=scope=="todos"?1:numero;
        if(scope=="atual" && !occurrence.Id.HasValue)
        {
            await UpsertBoletoExceptionAsync(connection,tx,userId,id,numero,request.DataVencimento??occurrence.Due,request.ValorOriginal,dados,"pendente");
            await SyncFixedAsync(connection,tx,userId,BoletoMonthEnd());
            await tx.CommitAsync(); return;
        }
        if(scope!="atual" && tipo=="fixo")
        {
            await using var rule=connection.CreateCommand(); rule.Transaction=tx;
            rule.CommandText="delete from public.boleto_regras where id_boleto=@id and id_usuario=@userId and numero_inicio>=@start";
            Add(rule,"id",id); Add(rule,"userId",userId); Add(rule,"start",start); Add(rule,"dados",dados,NpgsqlDbType.Jsonb);
            await rule.ExecuteNonQueryAsync();
            rule.CommandText="insert into public.boleto_regras(id_boleto,id_usuario,numero_inicio,dados) values(@id,@userId,@start,@dados)";
            await rule.ExecuteNonQueryAsync();
            rule.CommandText="update public.boleto_excecoes set valor=(@dados->>'valor_original')::numeric,dados=@dados where id_boleto=@id and id_usuario=@userId and numero>=@start and status='pendente'";
            await rule.ExecuteNonQueryAsync();
        }
        await using(var edit=connection.CreateCommand())
        {
            edit.Transaction=tx;
            edit.CommandText="""
              update public.boleto_parcelas set valor_previsto=@valor,dados_ocorrencia=@dados,
                data_vencimento=case when @scope='atual' then coalesce(@due,data_vencimento) else data_vencimento end
              where id_boleto=@id and id_usuario=@userId and status<>'cancelado'
                and ((@scope='atual' and numero_parcela=@numero) or (@scope<>'atual' and numero_parcela>=@start))
              """;
            Add(edit,"id",id); Add(edit,"userId",userId); Add(edit,"valor",request.ValorOriginal); Add(edit,"dados",dados,NpgsqlDbType.Jsonb);
            Add(edit,"scope",scope); Add(edit,"numero",numero); Add(edit,"start",start); Add(edit,"due",request.DataVencimento,NpgsqlDbType.Date);
            if(await edit.ExecuteNonQueryAsync()==0 && (tipo!="fixo" || scope=="atual")) throw new InvalidOperationException("Nenhuma ocorrência disponível para edição.");
        }
        // Cabeçalho reflete o padrão futuro, sem modificar a âncora ou as datas históricas.
        if(scope!="atual" || tipo=="unico")
        {
            await using var header=connection.CreateCommand(); header.Transaction=tx;
            header.CommandText="""
              update public.boletos b set descricao=d.descricao,beneficiario=d.beneficiario,categoria=d.categoria,id_subcategoria=d.id_subcategoria,
                valor_original=case when b.tipo='parcelado' then (select coalesce(sum(p.valor_previsto),b.valor_original) from public.boleto_parcelas p where p.id_boleto=b.id and p.status<>'cancelado') else d.valor_original end,
                id_conta_padrao=d.id_conta_padrao,modo_pagamento=d.modo_pagamento,dias_lembrete=d.dias_lembrete,codigo_barras=d.codigo_barras,observacao=d.observacao,
                data_inicio=case when b.tipo='unico' then coalesce(@due,b.data_inicio) else b.data_inicio end
              from jsonb_to_record(@dados) as d(descricao text,beneficiario text,categoria text,id_subcategoria uuid,valor_original numeric,id_conta_padrao uuid,modo_pagamento text,dias_lembrete int,codigo_barras text,observacao text)
              where b.id=@id and b.id_usuario=@userId
              """;
            Add(header,"id",id); Add(header,"userId",userId); Add(header,"dados",dados,NpgsqlDbType.Jsonb); Add(header,"due",request.DataVencimento,NpgsqlDbType.Date);
            await header.ExecuteNonQueryAsync();
        }
        else if(tipo=="parcelado")
        {
            await using var header=connection.CreateCommand(); header.Transaction=tx;
            header.CommandText="update public.boletos b set valor_original=(select sum(valor_previsto) from public.boleto_parcelas where id_boleto=b.id and status<>'cancelado') where b.id=@id and b.id_usuario=@userId";
            Add(header,"id",id); Add(header,"userId",userId); await header.ExecuteNonQueryAsync();
        }
        await tx.CommitAsync();
    }

    public Task DeleteBoletoAsync(Guid userId,Guid id) => DeleteBoletoOccurrenceAsync(userId,id,new BoletoExclusaoRequest(1,"futuros"));

    public async Task DeleteBoletoOccurrenceAsync(Guid userId,Guid id,BoletoExclusaoRequest request)
    {
        if(request.Escopo is not ("atual" or "futuros")) throw new ArgumentException("Escopo de exclusão inválido.");
        await using var connection=await _dataSource.OpenConnectionAsync(); await using var tx=await connection.BeginTransactionAsync();
        await LockBoletosAsync(connection,tx,userId);
        await SyncFixedAsync(connection,tx,userId,BoletoMonthEnd());
        var occurrence=await FindOccurrenceAsync(connection,tx,userId,id,request.NumeroParcela);
        if(request.Escopo=="atual" && !occurrence.Id.HasValue)
        {
            if(occurrence.Status!="pendente") throw new InvalidOperationException("Ocorrência cancelada.");
            await UpsertBoletoExceptionAsync(connection,tx,userId,id,request.NumeroParcela,occurrence.Due,occurrence.Value,occurrence.Dados,"cancelado");
            await tx.CommitAsync(); return;
        }
        await using var cmd=connection.CreateCommand(); cmd.Transaction=tx;
        cmd.CommandText="""
          update public.boleto_parcelas set status='cancelado' where id_boleto=@id and id_usuario=@userId and status='pendente'
          and ((@scope='atual' and numero_parcela=@numero) or (@scope='futuros' and numero_parcela>=@numero))
          """;
        Add(cmd,"id",id); Add(cmd,"userId",userId); Add(cmd,"scope",request.Escopo); Add(cmd,"numero",request.NumeroParcela);
        var count=await cmd.ExecuteNonQueryAsync();
        if(request.Escopo=="atual" && count==0) throw new InvalidOperationException("Não é possível excluir um pagamento realizado.");
        if(request.Escopo=="futuros")
        {
            cmd.CommandText="""
              update public.boletos set
                data_fim=case when tipo='fixo' and @numero>1 then least(coalesce(data_fim,'infinity'::date),public.boleto_data(data_inicio,@numero-1,frequencia)) else data_fim end,
                ativo=case when tipo='fixo' and @numero>1 then ativo when tipo<>'fixo' and exists(select 1 from public.boleto_parcelas p where p.id_boleto=boletos.id and p.status='pendente') then ativo else false end
              where id=@id and id_usuario=@userId
              """;
            await cmd.ExecuteNonQueryAsync();
        }
        await tx.CommitAsync();
    }

    public async Task PayBoletoParcelaAsync(Guid userId, Guid parcelaId, BoletoPagamentoRequest request, bool automatico=false, Guid? boletoRef=null, int? numero=null)
    {
        if(request.ValorPago<=0 || request.DataPagamento.Year<1900 || request.DataPagamento.Year>9900) throw new ArgumentException("Informe um valor e data de pagamento válidos.");
        await using var connection=await _dataSource.OpenConnectionAsync(); await using var tx=await connection.BeginTransactionAsync();
        await LockBoletosAsync(connection,tx,userId);
        if(boletoRef.HasValue && numero.HasValue) parcelaId=await MaterializeAsync(connection,tx,userId,boletoRef.Value,numero.Value);
        Guid boletoId; string descricao; string? categoria; Guid? sub; Guid? contaPadrao; decimal previsto; string modo; DateOnly vencimento;
        await using(var get=connection.CreateCommand()) { get.Transaction=tx; get.CommandText="""select p.id_boleto,d.dados->>'descricao',d.dados->>'categoria',(d.dados->>'id_subcategoria')::uuid,(d.dados->>'id_conta_padrao')::uuid,p.valor_previsto,d.dados->>'modo_pagamento',p.data_vencimento from public.boleto_parcelas p join public.boletos b on b.id=p.id_boleto cross join lateral (select coalesce(p.dados_ocorrencia,to_jsonb(b)) dados) d where p.id=@id and p.id_usuario=@userId and b.id_usuario=@userId and p.status='pendente' for update of p"""; Add(get,"id",parcelaId); Add(get,"userId",userId); await using var r=await get.ExecuteReaderAsync(); if(!await r.ReadAsync()) throw new InvalidOperationException("Parcela pendente não encontrada."); boletoId=r.GetGuid(0); descricao=r.GetString(1); categoria=r.IsDBNull(2)?null:r.GetString(2); sub=r.IsDBNull(3)?null:r.GetGuid(3); contaPadrao=r.IsDBNull(4)?null:r.GetGuid(4); previsto=r.GetDecimal(5); modo=r.GetString(6); vencimento=r.GetFieldValue<DateOnly>(7); }
        if(automatico)
        {
            if(modo!="automatico" || !contaPadrao.HasValue || vencimento>request.DataPagamento) throw new InvalidOperationException("Parcela pendente não encontrada.");
            request=request with { ValorPago=previsto, IdContaPagamento=contaPadrao };
        }
        var saidaId=Guid.NewGuid(); var conta=request.IdContaPagamento??contaPadrao;
        // Uma conta explicitamente escolhida deve pertencer ao usuário. Referências históricas removidas viram nulas.
        if(request.IdContaPagamento.HasValue && !automatico) await ValidateBoletoReferencesAsync(connection,tx,userId,request.IdContaPagamento,null);
        await using(var refs=connection.CreateCommand())
        {
            refs.Transaction=tx; refs.CommandText="select (select id from public.contas where id=@conta and id_usuario=@userId),(select id from public.subcategorias where id=@sub and id_usuario=@userId)";
            Add(refs,"conta",conta,NpgsqlDbType.Uuid); Add(refs,"sub",sub,NpgsqlDbType.Uuid); Add(refs,"userId",userId);
            await using var reader=await refs.ExecuteReaderAsync(); await reader.ReadAsync();
            conta=reader.IsDBNull(0)?null:reader.GetGuid(0); sub=reader.IsDBNull(1)?null:reader.GetGuid(1);
        }
        if(automatico && !conta.HasValue) throw new InvalidOperationException("Parcela pendente não encontrada.");
        await using(var insert=connection.CreateCommand()) { insert.Transaction=tx; insert.CommandText="""insert into public.saidas(id,id_usuario,descricao,categoria,id_subcategoria,valor,data_movimentacao,observacao,id_conta) values(@id,@userId,@descricao,@categoria,@sub,@valor,@data,@obs,@conta)"""; Add(insert,"id",saidaId); Add(insert,"userId",userId); Add(insert,"descricao",descricao); Add(insert,"categoria",categoria,NpgsqlDbType.Text); Add(insert,"sub",sub,NpgsqlDbType.Uuid); Add(insert,"valor",request.ValorPago); Add(insert,"data",request.DataPagamento,NpgsqlDbType.Date); Add(insert,"obs",$"Pagamento de boleto. {request.Observacao}".Trim()); Add(insert,"conta",conta,NpgsqlDbType.Uuid); await insert.ExecuteNonQueryAsync(); }
        await using(var update=connection.CreateCommand()) { update.Transaction=tx; update.CommandText="""update public.boleto_parcelas set status='pago',data_pagamento=@data,valor_pago=@valor,juros=@juros,multa=@multa,desconto=@desconto,id_conta_pagamento=@conta,id_saida_gerada=@saida,pagamento_automatico=@automatico,observacao_pagamento=@obs where id=@id and id_usuario=@userId"""; Add(update,"id",parcelaId); Add(update,"userId",userId); Add(update,"data",request.DataPagamento,NpgsqlDbType.Date); Add(update,"valor",request.ValorPago); Add(update,"juros",Math.Max(0,request.Juros)); Add(update,"multa",Math.Max(0,request.Multa)); Add(update,"desconto",Math.Max(0,request.Desconto)); Add(update,"conta",conta,NpgsqlDbType.Uuid); Add(update,"saida",saidaId); Add(update,"automatico",automatico); Add(update,"obs",NullIfWhiteSpace(request.Observacao),NpgsqlDbType.Text); await update.ExecuteNonQueryAsync(); }
        await tx.CommitAsync();
    }

    public async Task<int> ProcessAutomaticBoletosAsync(Guid userId, DateOnly today)
    {
        var boletos=await ListBoletosAsync(userId); var count=0;
        foreach(var b in boletos.Where(x=>x.Ativo))
          foreach(var p in b.Parcelas.Where(x=>!x.Virtual && x.Status=="pendente" && x.DataVencimento<=today))
          {
              var mode=p.Dados is JsonElement d && d.TryGetProperty("modo_pagamento",out var m) ? m.GetString() : b.ModoPagamento;
              var conta=p.Dados is JsonElement c && c.TryGetProperty("id_conta_padrao",out var v) ? (v.ValueKind==JsonValueKind.String ? v.GetGuid() : (Guid?)null) : b.IdContaPadrao;
              if(mode!="automatico" || !conta.HasValue) continue;
              try { await PayBoletoParcelaAsync(userId,p.Id,new BoletoPagamentoRequest(today,p.ValorPrevisto,0,0,0,conta,"Pagamento automático no vencimento."),true); count++; }
              catch(InvalidOperationException ex) when(ex.Message=="Parcela pendente não encontrada.") { /* Outra requisição já baixou/cancelou. */ }
          }
        return count;
    }

    private static async Task RebuildBoletoParcelasAsync(Npgsql.NpgsqlConnection connection,Npgsql.NpgsqlTransaction tx,Guid userId,Guid boletoId,BoletoRequest request)
    {
        var qtd=ResolveQuantidade(request); var tipo=NormalizeBoletoTipo(request.Tipo);
        var centavosBase=decimal.Floor(request.ValorOriginal*100/qtd);
        var resto=request.ValorOriginal*100-centavosBase*qtd;
        for(var i=1;i<=qtd;i++)
        {
            var due=AdvanceDate(request.DataInicio,i-1,NormalizeBoletoFrequencia(request.Frequencia)); if(request.DataFim.HasValue && due>request.DataFim.Value) break;
            await using var command=connection.CreateCommand(); command.Transaction=tx; command.CommandText="""insert into public.boleto_parcelas(id_usuario,id_boleto,numero_parcela,total_parcelas,valor_previsto,data_vencimento,dados_ocorrencia) select @userId,@boleto,@numero,@total,@valor,@data,to_jsonb(b) from public.boletos b where b.id=@boleto and b.id_usuario=@userId on conflict(id_boleto,numero_parcela) do nothing"""; Add(command,"userId",userId); Add(command,"boleto",boletoId); Add(command,"numero",i); Add(command,"total",qtd); Add(command,"valor",tipo=="parcelado" ? (centavosBase+(i<=resto?1:0))/100 : request.ValorOriginal); Add(command,"data",due,NpgsqlDbType.Date); await command.ExecuteNonQueryAsync();
        }
    }
    private static DateOnly AdvanceDate(DateOnly date,int step,string freq)=>freq switch {"semanal"=>date.AddDays(step*7),"anual"=>date.AddYears(step),_=>date.AddMonths(step)};
    private static int ResolveQuantidade(BoletoRequest r)=>NormalizeBoletoTipo(r.Tipo) switch {"unico"=>1,"fixo"=>1,_=>Math.Clamp(r.QuantidadeParcelas,2,120)};
    private static string NormalizeBoletoTipo(string? v)=>v?.Trim().ToLowerInvariant() switch {"fixo"=>"fixo","parcelado"=>"parcelado",_=>"unico"};
    private static string NormalizeBoletoFrequencia(string? v)=>v?.Trim().ToLowerInvariant() switch {"semanal"=>"semanal","anual"=>"anual",_=>"mensal"};
    private static string NormalizeBoletoModo(string? v)=>v?.Trim().ToLowerInvariant() switch {"automatico"=>"automatico","confirmacao"=>"confirmacao",_=>"manual"};
    private static void ValidateBoleto(BoletoRequest r){if(string.IsNullOrWhiteSpace(r.Descricao))throw new ArgumentException("Informe a descrição."); if(r.ValorOriginal<=0 || r.ValorOriginal!=Math.Round(r.ValorOriginal,2))throw new ArgumentException("Informe um valor positivo com até duas casas decimais."); if(r.DataInicio.Year<1900 || r.DataInicio.Year>9900) throw new ArgumentException("Data inicial inválida."); if(r.DataVencimento is DateOnly due && (due.Year<1900 || due.Year>9900)) throw new ArgumentException("Vencimento inválido."); if(r.NumeroParcela is null && NormalizeBoletoTipo(r.Tipo)=="parcelado" && (r.QuantidadeParcelas<2 || r.QuantidadeParcelas>120 || r.ValorOriginal < r.QuantidadeParcelas*0.01m)) throw new ArgumentException("Valor total ou quantidade de parcelas inválida.");}
}
