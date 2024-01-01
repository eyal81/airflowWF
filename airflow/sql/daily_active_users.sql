select count(user_id) as daily_active_users
FROM
(
select user_id ,last_time, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY last_time DESC) as RN
	FROM
	(
		SELECT user_id,(jsonb_array_elements(messages)->>'id')::timestamp as last_time
		FROM public.profiles_manage_turbo
	)SUBY
)SUBY2
WHERE RN = 1
AND date(last_time)> current_date - interval '10' day;