
  
    

        create or replace transient table USER_DB_DRAGON.analytics.session_timestamp
         as
        (SELECT
sessionId,
ts
FROM USER_DB_DRAGON.raw.session_timestamp
WHERE sessionId IS NOT NULL
        );
      
  