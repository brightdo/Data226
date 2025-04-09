
  
    

        create or replace transient table USER_DB_DRAGON.analytics.user_session_channel
         as
        (SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_DRAGON.raw.user_session_channel
WHERE sessionId IS NOT NULL
        );
      
  