# JsnFinances.Api

Backend ASP.NET Core para o JSN Finances, conectado ao Supabase PostgreSQL.

## Arquitetura definitiva

- Supabase Auth fica no frontend somente para login, cadastro e recuperação de senha.
- O frontend envia `Authorization: Bearer <access_token>` para a API .NET.
- O backend valida o JWT do Supabase usando `Supabase:JwtSecret`.
- O backend extrai o usuário pela claim `sub` e usa esse UUID em todas as consultas.
- O frontend não acessa tabelas do Supabase diretamente.

## O que fica no backend

- CRUD de entradas e saídas.
- CRUD de categorias com normalização de tipo.
- Renomear categoria atualizando entradas/saídas automaticamente.
- CRUD de metas.
- Regra de guardar valor na meta sem ultrapassar o valor objetivo.
- Parcelamento de despesas.
- Orçamento mensal por categoria.
- Contas/carteiras e transferências.
- Perfil e preferências do usuário.
- Filtro global persistido.
- Resumo de relatórios.
- Insights simples por regra de negócio.
- Paginação real de tabelas grandes.

## Como configurar

1. No Supabase, copie a connection string PostgreSQL.
2. Cole em `appsettings.Development.json` no campo `ConnectionStrings:SupabasePostgres`.
3. No Supabase, copie o JWT Secret em `Project Settings > API > JWT Settings`.
4. Cole em `appsettings.Development.json` no campo `Supabase:JwtSecret`.
5. Execute no Supabase o script:
   `front/src/database/supabase-arquitetura-definitiva.sql`
6. Rode a API:

```bash
dotnet restore
dotnet run --project CODEX/back/JsnFinances/JsnFinances/JsnFinances.API.csproj
```

## Segurança

A API não usa mais id de usuário enviado manualmente pelo frontend.
O usuário é identificado exclusivamente pelo JWT assinado do Supabase.

## Portal Admin Seguro

Foi adicionado um portal interno em `/admin`, protegido no backend por `/api/admin/*`.

Configuração:

```json
"Admin": {
  "AllowedEmails": [
    "juleo745@gmail.com"
  ],
  "RequireVerifiedEmail": true
}
```

Script obrigatório no Supabase:

```txt
CODEX/front/src/database/supabase-admin-portal.sql
```

Os endpoints admin retornam `403 Forbidden` para qualquer conta fora da lista permitida, mesmo que a pessoa tente acessar a rota pelo DevTools ou chamar a API manualmente.
