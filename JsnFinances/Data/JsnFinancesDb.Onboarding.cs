using JsnFinances.Api.Domain;
using Npgsql;

namespace JsnFinances.Api.Data;

public sealed partial class JsnFinancesDb
{
    public async Task<OnboardingProfileDto?> GetOnboardingProfileAsync(Guid userId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select id_usuario, profile_type, main_goal, financial_moment, biggest_challenge, usage_frequency, created_at, updated_at
            from public.user_onboarding_profile
            where id_usuario = @userId
            limit 1
            """;
        Add(command, "userId", userId);

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;

        return new OnboardingProfileDto(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.GetFieldValue<DateTimeOffset>(6),
            reader.GetFieldValue<DateTimeOffset>(7));
    }

    public async Task<OnboardingProfileDto> UpsertOnboardingProfileAsync(Guid userId, OnboardingProfileRequest request)
    {
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            insert into public.user_onboarding_profile
                (id_usuario, profile_type, main_goal, financial_moment, biggest_challenge, usage_frequency, updated_at)
            values
                (@userId, @profileType, @mainGoal, @financialMoment, @biggestChallenge, @usageFrequency, now())
            on conflict (id_usuario) do update set
                profile_type = excluded.profile_type,
                main_goal = excluded.main_goal,
                financial_moment = excluded.financial_moment,
                biggest_challenge = excluded.biggest_challenge,
                usage_frequency = excluded.usage_frequency,
                updated_at = now()
            returning id_usuario, profile_type, main_goal, financial_moment, biggest_challenge, usage_frequency, created_at, updated_at
            """;
        Add(command, "userId", userId);
        Add(command, "profileType", NormalizeOnboardingAnswer(request.ProfileType, "Pessoal"));
        Add(command, "mainGoal", NormalizeOnboardingAnswer(request.MainGoal, "Controlar gastos"));
        Add(command, "financialMoment", NormalizeOnboardingAnswer(request.FinancialMoment, "Quero me organizar melhor"));
        Add(command, "biggestChallenge", NormalizeOnboardingAnswer(request.BiggestChallenge, "Cartão de crédito"));
        Add(command, "usageFrequency", NormalizeOnboardingAnswer(request.UsageFrequency, "Algumas vezes por semana"));

        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            throw new InvalidOperationException("Não foi possível salvar o perfil do onboarding.");
        }

        return new OnboardingProfileDto(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.GetFieldValue<DateTimeOffset>(6),
            reader.GetFieldValue<DateTimeOffset>(7));
    }

    private static string NormalizeOnboardingAnswer(string? value, string fallback)
    {
        var normalized = (value ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(normalized) ? fallback : normalized.Length > 80 ? normalized[..80] : normalized;
    }
}
