with __dbt__cte__user_session_channel as (
SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_DRAGON.raw.user_session_channel
WHERE sessionId IS NOT NULL
),  __dbt__cte__session_timestamp as (
SELECT
sessionId,
ts
FROM USER_DB_DRAGON.raw.session_timestamp
WHERE sessionId IS NOT NULL
) SELECT u.userId, u.sessionId, u.channel, st.ts
FROM __dbt__cte__user_session_channel u
JOIN __dbt__cte__session_timestamp st ON u.sessionId = st.sessionId

