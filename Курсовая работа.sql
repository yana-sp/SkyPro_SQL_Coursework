with first_payments as --  даты первой оплаты для каждого студента
    (select user_id
        , date_trunc('day', min(transaction_datetime)) as first_payment_date
    from skyeng_db.payments
    where id_transaction is not null
        and operation_name in ('Покупка уроков', 'Начисление корпоративному клиенту')
        and status_name = 'success'
    group by 1 
    ),
all_dates as --   даты всех уроков 
    (select distinct date_trunc('day', class_start_datetime) as dt 
    from skyeng_db.classes 
    where class_start_datetime between '2016-01-01' and '2016-12-31'
order by 1 
    ),
all_dates_by_user as --  все даты уроков для каждого студента после оплаты 
    (select fp.user_id as user_id
        , ad.dt as dt 
    from all_dates as ad
        left join first_payments as fp
            on ad.dt >= fp.first_payment_date
    ),
payments_by_dates as --  сколько уроков начислено в конкретный день 
    (select user_id
        , date_trunc('day', transaction_datetime) as payment_date
        , sum(classes) as transaction_balance_change
    from skyeng_db.payments
    where id_transaction is not null
        and operation_name in ('Покупка уроков', 'Начисление корпоративному клиенту')
        and status_name = 'success'
        and transaction_datetime between '2016-01-01' and '2016-12-31'
    group by 1, 2 
    order by user_id 
    ),
payments_by_dates_cumsum as --  баланс, сформированный транзакциями (кумулятивный)
    (select adbu.user_id as user_id
        , adbu.dt as dt 
        , pbd.transaction_balance_change as transaction_balance_change
        , sum(case when pbd.transaction_balance_change is not null then pbd.transaction_balance_change else 0 end) over(partition by adbu.user_id order by adbu.dt) as transaction_balance_change_cs 
    from all_dates_by_user as adbu 
        left join payments_by_dates as pbd 
            on adbu.user_id = pbd.user_id
                and adbu.dt = pbd.payment_date
    order by 1, 2 
    ), 
classes_by_dates as  --  сколько уроков списано в конкретный день 
    (select user_id
        , date_trunc('day', class_start_datetime) as class_date
        , count(id_class) * (-1) as classes
    from skyeng_db.classes 
    where class_type != 'trial'
        and class_status in ('success', 'failed_by_student')
        and class_start_datetime between '2016-01-01' and '2016-12-31'
    group by 1, 2 
    order by 1, 2 
    ), 
classes_by_dates_dates_cumsum as --  баланс пройденных уроков (кумулятивный)
    (select adbu.user_id as user_id
        , adbu.dt as dt
        , classes
        , sum(case when cbd.classes is not null then cbd.classes else 0 end) over (partition by adbu.user_id order by adbu.dt) as classes_cs
    from all_dates_by_user as adbu
        left join classes_by_dates as cbd 
            on adbu.user_id = cbd.user_id
            and adbu.dt = cbd.class_date
    order by 1, 2 
    ),
balances as --  вычисленные балансы для каждого студента 
    (select *,
        transaction_balance_change_cs + classes_cs as balance
    from payments_by_dates_cumsum
        join classes_by_dates_dates_cumsum
            using(user_id, dt)
    )
    
-- Задание 1 
-- select *
-- from balances
-- order by 1, 2 
-- limit 1000
    

--Задание 2
select
    dt,
    sum(transaction_balance_change) as transaction_balance_change,
    sum(classes) as classes, 
    sum(transaction_balance_change_cs) as transaction_balance_change_cs,
    sum(classes_cs) as classes_cs,
    sum(balance) as balance
from balances
group by 1 
order by 1     
        
    



-- Вопросы к дата-инженерам и владельцам таблицы payments перед началом формирования запроса:
-- Это вопросы по фильтрации данных. То есть какие данные нам учитывать при вормировании запроса. Например, какие виды транзакций (operation_name) нам использовать в расчете. 
-- Также уточнить, что означают другие status_name кроме "success" и "failed", и нужно ли нам учитывать еще операции со status_name кроме "success".


-- ВЫВОДЫ:

-- Сумма оставшихся у ученика на балансе урков (кривая balance) , близка к 0, но к концу года стремится к увеличению оплаченных, но не списанных уроков. То есть к концу года списаны не все уроки, купленные в течение этого года.
-- Это может свидетельсвовать о том, что студенты пополняют пакет уроков, когда у них на балансе еще есть несколько несписанных. 
-- Однако стоит проверить гипотезу о том, что не все купленные уроки проходятся, и студенты бросают занятия до того, как израсходуются все уроки на балансе. 

-- Также из полученных данных можно сделать вывод о том, что в течение года количество проводимых в день уроков в среднем росло. 
-- Особенно выражен рост во втором полугодии, что может свидетельствовать об увеличении спроса на занятия у студентов с осени. Причиной тому может быть активная маркетинговая компания, или повышение интереса к обучению с началом нового учебного года/возвращения из летних отпусков.
-- Максимальное количество проведенных уроков зафиксировано в ноябре (14 ноября - 110 уроков).
-- При этом наблюдается ярко выраженная недельная периодичность проведения уроков. Так минимальное количество уроков приходится на воскресенье. А максимальное - на понедельник-вторник и четверг-пятницу.
-- Особенно четко эта периодичность прослеживается во втором полугодии.








