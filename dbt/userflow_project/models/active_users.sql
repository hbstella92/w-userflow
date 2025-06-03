{{ config(materialized="table") }}

SELECT user_id, COUNT(*) AS event_count
FROM public.user_events
GROUP BY user_id