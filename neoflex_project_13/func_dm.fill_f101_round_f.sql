create or replace function dm.fill_f101_round_f(i_OnDate date) returns void as $$ 
declare 
	start_log timestamptz;
	finish_log timestamptz;
	rep_start_date date := i_OnDate - interval '1 month';
	rep_end_date date := i_OnDate - interval '1 day';
begin 
	start_log := now();
--Удаление данных, при перезаписи за дату отчёта.
delete from dm.dm_f101_round_f  
 where from_date = rep_start_date
   and to_date = rep_end_date;
insert into dm.dm_f101_round_f(
--Не понятно зачем столбцы "R_%", поэтому данные в них отсутвуют.
			from_date, to_date,  
			chapter, ledger_account, characteristic, balance_in_rub,
			r_balance_in_rub, balance_in_val, r_balance_in_val, balance_in_total,
			r_balance_in_total, turn_deb_rub, r_turn_deb_rub, turn_deb_val,
			r_turn_deb_val, turn_deb_total, r_turn_deb_total, turn_cre_rub,
			r_turn_cre_rub, turn_cre_val, r_turn_cre_val, turn_cre_total,
			r_turn_cre_total, balance_out_rub, r_balance_out_rub, balance_out_val,
			r_balance_out_val, balance_out_total, r_balance_out_total
			)			
	   with rest_in_rub as (  --СТЕ входящих остатков в рублях.
			select mla.ledger_account,
				   sum(dabf.balance_out_rub) as balance_in_rub
			  from ds.MD_LEDGER_ACCOUNT_S mla
--Таблица со счетами для проверки активности счета в период отчёта
			  left join ds.md_account_d mad 					
			    on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
--Только рублевые счета
	           and mad.currency_code in (643, 810) 	
--Проверка на "открытость" счёта в период отчёта	           
	           and mad.data_actual_date <= rep_end_date	
--Проверка на "открытость" счёта в период отчёта 
			   and mad.data_actual_end_date >= rep_start_date 
--Таблица с остаток на счёте для входящего остатка
			  join dm.dm_account_balance_f dabf 			
			    on dabf.account_rk = mad.account_rk
			   and dabf.on_date = i_OnDate - interval '1 month' - interval '1 day'
--Группировка по маске 2го порядка, как требует форма
			 group by mla.ledger_account						
			 ), 
			 rest_in_val as (  --СТЕ входящих остатков в валюте
			select mla.ledger_account,
				   sum(dabf.balance_out_rub) as balance_in_val
			  from ds.MD_LEDGER_ACCOUNT_S mla						
			  left join ds.md_account_d mad 
			    on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
			   and mad.currency_code not in (643, 810)			--Аналогично предыдущей CTE, но в валюте
			   and mad.data_actual_date <= rep_end_date
			   and mad.data_actual_end_date >= rep_start_date  
			  join dm.dm_account_balance_f dabf 
  			    on dabf.account_rk = mad.account_rk
		       and dabf.on_date = i_OnDate - interval '1 month' - interval '1 day'
		     group by mla.ledger_account
		     ),	
			rest_in_total as (  --СТЕ входящих остатков итого		
		    select rep_start_date as from_date,
			  	   rep_end_date as to_date,
				   mla.chapter,
				   mla.ledger_account,
				   mla.characteristic,
				   sum(dabf.balance_out_rub) as balance_in_total
			  from ds.MD_LEDGER_ACCOUNT_S mla
			  left join ds.md_account_d mad 
			    on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
			   and mad.data_actual_date <= rep_end_date				--Аналогично предыдущей CTE, но все
			   and mad.data_actual_end_date >= rep_start_date  
			  join dm.dm_account_balance_f dabf 
  			    on dabf.account_rk = mad.account_rk
			   and dabf.on_date = i_OnDate - interval '1 month' - interval '1 day'
		     group by 1,2,3,4,5 --Для укорочения кода использовал нумерацию столбцов
		     ), turns_rub as (  -- СТЕ оборотов в рублях
		    select mla.ledger_account,
			  	   sum(datf.debet_amount_rub) as turn_deb_rub,
				   sum(datf.credit_amount_rub) as turn_cre_rub
			  from ds.MD_LEDGER_ACCOUNT_S mla
			  join ds.md_account_d mad 
			    on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
			   and mad.currency_code in (643, 810)
			   and mad.data_actual_date <= rep_end_date
			   and mad.data_actual_end_date >= rep_start_date 
--Аналогично предыдущей CTE, кроме таблицы оборотов.
			  join dm.dm_account_turnover_f datf 							
			    on datf.account_rk = mad.account_rk 				
--Проверка на попадаение оборота в период отчёта
			   and datf.on_date between rep_start_date and rep_end_date
		     group by mla.ledger_account
			), turns_val as (  -- СТЕ оборотов в валюте
--Аналогично предыдущей CTE, но в валюте.
		    select mla.ledger_account,
			   	   sum(datf.debet_amount_rub) as turn_deb_val,
				   sum(datf.credit_amount_rub) as turn_cre_val	
			  from ds.MD_LEDGER_ACCOUNT_S mla
			  join ds.md_account_d mad 
			    on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
			   and mad.currency_code not in (643, 810)
			   and mad.data_actual_date <= rep_end_date
			   and mad.data_actual_end_date >= rep_start_date 
			  join dm.dm_account_turnover_f datf 
			    on datf.account_rk = mad.account_rk 
			   and datf.on_date between rep_start_date and rep_end_date 
		     group by mla.ledger_account
			), turns_total as (  -- СТЕ оборотов итого
--Аналогично предыдущей CTE, но все.			
		    select mla.ledger_account,
			       sum(datf.debet_amount_rub) as turn_deb_total,
				   sum(datf.credit_amount_rub) as turn_cre_total
			  from ds.MD_LEDGER_ACCOUNT_S mla
			  join ds.md_account_d mad 
			    on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
		       and mad.data_actual_date <= rep_end_date
			   and mad.data_actual_end_date >= rep_start_date 
			  join dm.dm_account_turnover_f datf 
			    on datf.account_rk = mad.account_rk 
			   and datf.on_date between rep_start_date and rep_end_date 
		     group by mla.ledger_account
		     ), rest_out_rub as (  -- СТЕ исходящих остатков в рублях
/*Cледующие три CTE аналогичны первым (входящим остатком), 
только остаток берут за последний день отчётного периода.*/
			select mla.ledger_account,
				   sum(dabf.balance_out_rub) as balance_out_rub
			  from ds.MD_LEDGER_ACCOUNT_S mla
			  left join ds.md_account_d mad 
			    on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
			    and mad.currency_code in (643, 810)
			    and mad.data_actual_date <= rep_end_date
			    and mad.data_actual_end_date >= rep_start_date 
			   join dm.dm_account_balance_f dabf 
			     on dabf.account_rk = mad.account_rk
			    and dabf.on_date = rep_end_date
			  group by mla.ledger_account
			), rest_out_val as (  -- СТЕ исходящих остатков в валюте
		     select mla.ledger_account,
				    sum(dabf.balance_out_rub) as balance_out_val
			   from ds.MD_LEDGER_ACCOUNT_S mla
			   left join ds.md_account_d mad 
  			     on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
			    and mad.currency_code not in (643, 810)
			    and mad.data_actual_date <= rep_end_date
			    and mad.data_actual_end_date >= rep_start_date 
			   join dm.dm_account_balance_f dabf
			     on dabf.account_rk = mad.account_rk
			    and dabf.on_date = rep_end_date
			  group by mla.ledger_account
			  ), rest_out_total as (  -- СТЕ исходящих остатков итого
		     select mla.ledger_account,
				    sum(dabf.balance_out_rub) as balance_out_total
			   from ds.MD_LEDGER_ACCOUNT_S mla
			   left join ds.md_account_d mad 
			     on cast(substring(mad.account_number, 1, 5) as int) = mla.ledger_account
			    and mad.data_actual_date <= rep_end_date
			    and mad.data_actual_end_date >= rep_start_date 
			   join dm.dm_account_balance_f dabf 
			     on dabf.account_rk = mad.account_rk
			    and dabf.on_date = rep_end_date
			  group by mla.ledger_account
			  )
		     select rit.from_date, rit.to_date,
				    rit.chapter, rit.ledger_account, rit.characteristic,
				    rir.balance_in_rub, null, riv.balance_in_val, null, rit.balance_in_total, null, 
				    tr.turn_deb_rub, null, tv.turn_deb_val, null, tt.turn_deb_total, null, 
				    tr.turn_cre_rub, null, tv.turn_cre_val, null, tt.turn_cre_total, null, 
				    ror.balance_out_rub,  null, rov.balance_out_val,  null, rot.balance_out_total, null
			   from rest_in_total rit 
		  	   left join rest_in_rub rir using (ledger_account)
		  	   left join rest_in_val riv using (ledger_account)
			   left join turns_rub tr using (ledger_account)
			   left join turns_val tv using (ledger_account)
			   left join turns_total tt using (ledger_account)
			   left join rest_out_rub ror using (ledger_account)
			   left join rest_out_val rov using (ledger_account)
			   left join rest_out_total rot using (ledger_account);
	finish_log := now();
--Логгирование
	insert into logs.load (
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
		   'dm_f101_round_f',
		   'load data',
		   start_log,
		   finish_log,
		   concat('date of report - ', i_OnDate)
		   );
end; $$ language plpgsql


--Скрипты для проверки
select * from dm.dm_f101_round_f dfrf;

select * from logs.load;

truncate dm.dm_f101_round_f;

select dm.fill_f101_round_f('2018-02-01');

select * from dm.dm_f101_round_f_v2;

truncate dm.dm_f101_round_f_v2;





