SELECT
sessionId,
ts
FROM USER_DB_DRAGON.raw.session_timestamp
WHERE sessionId IS NOT NULL