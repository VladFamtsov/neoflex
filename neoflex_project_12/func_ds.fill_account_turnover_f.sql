create or replace function ds.fill_account_turnover_f (
	i_OnDate date
	) returns void as $$
declare 
	start_log timestamptz;
	finish_log timestamptz;
begin 
	start_log := now();
--Удаляем данные, если таковые имеются за дату отчёта.
	delete from dm.DM_ACCOUNT_TURNOVER_F
	 where on_date = i_OnDate;
	insert into dm.DM_ACCOUNT_TURNOVER_F
	  with acc_day_amount as ( --Расчёт оборотов по дебету/кредиту счёта за день.
		   select oper_date,
		   		  'cr' as cr_db, 
		   		  credit_account_rk as acc_rk,
		   		  sum(credit_amount) as day_total
		   	 from ft_posting_f fpf
			where oper_date = i_OnDate
			group by oper_date,
				  credit_account_rk
			union all
		   SELECT oper_date,
				  'db' as cr_db,
				  debet_account_rk,
				  sum(debet_amount)
			 from ft_posting_f fpf
			where oper_date = i_OnDate
			group by oper_date,
				  debet_account_rk 
				  ),
		 data_tbl as (  --Соединение данных по счетам иоборотам построчно.
		   select ada.oper_date as on_date, 
		   	      ada.acc_rk as account_rk, 
		   	      ada_cr.day_total as credit_amount,
		   	      ada_db.day_total as debet_amount
			 from acc_day_amount ada
			 left join acc_day_amount ada_cr 
			   on ada_cr.acc_rk = ada.acc_rk
			  and ada_cr.cr_db = 'cr'
		     left join acc_day_amount ada_db 
		       on ada_db.acc_rk = ada.acc_rk
			  and ada_db.cr_db = 'db'
			  )
	select distinct
		   dtbl.on_date,
		   dtbl.account_rk,
		   dtbl.credit_amount,
		   case --Если курса нет, то умножаем на 1 (по ТЗ).
			    when merd.reduced_cource is null 
				then cast(dtbl.credit_amount * 1 as numeric(23,8))
				else cast(dtbl.credit_amount * merd.reduced_cource as numeric(23,8)) 
		    end as credit_amount_rub,
		   dtbl.debet_amount,
		   case  --Если курса нет, то умножаем на 1 (по ТЗ).
			    when merd.reduced_cource is null 
			    then cast(dtbl.debet_amount * 1 as numeric(23,8)) 
			    else cast(dtbl.debet_amount * merd.reduced_cource as numeric(23,8)) 
		    end as debet_amount_rub
	  from data_tbl dtbl
	  left join ds.md_account_d mad --Соединяем таблицу со счетам для привязки валюты счёта.
	    on mad.account_rk = dtbl.account_rk
	   and i_OnDate between mad.data_actual_date and mad.data_actual_end_date
	  left join ds.md_exchange_rate_d merd  --Соединяем табицу с курсами для конвертации в рубль.
	    on merd.currency_rk = mad.currency_rk
	   and i_OnDate between merd.data_actual_date and merd.data_actual_end_date
	 order by dtbl.account_rk;
		finish_log := now();
	insert into logs.load (  --Логгирование.
		   id,
		   schema_name,
		   table_name,
		   stage_name,
		   start_stage,
		   end_stage,
		   description
		   )
	values (
		   nextval('logs.load_id_seq'),
		   'dm',
		   'dm_account_turnover_f',
		   'load data for',
		   start_log,
		   finish_log,
		   concat('date of report - ', i_OnDate)
		   );
end; $$ language plpgsql


--Скрипты для проверки
truncate dm.dm_account_turnover_f;

select ds.fill_account_turnover_f('2018-01-31');

select distinct on_date 
  from dm.dm_account_turnover_f datf 
 order by on_date desc;


