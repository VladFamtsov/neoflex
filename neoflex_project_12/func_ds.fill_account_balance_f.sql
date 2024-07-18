select *
  from DM.DM_ACCOUNT_BALANCE_F 

  drop table dm.dm_account_balance_f 

create table DM.DM_ACCOUNT_BALANCE_F as
select *
  from ds.ft_balance_f fbf 

select *
  from dm.DM_ACCOUNT_BALANCE_F

 alter table dm.dm_account_balance_f 
   add column balance_out_rub float8;

update dm_account_balance_f dabf
   set balance_out_rub = balance_out * 1;

update dm_account_balance_f dabf
   set balance_out_rub = merd.reduced_cource * dabf.balance_out
  from ds.md_exchange_rate_d merd
 where dabf.currency_rk = merd.currency_rk
   and '2017-12-31' between merd.data_actual_date and merd.data_actual_end_date;

select * 
  from dm.dm_account_turnover_f 

select * 
  from dm.dm_account_balance_f dabf 

 create or replace function ds.fill_account_balance_f(
 	i_OnDate date
 	) returns void as $$
declare rep_date date := i_OnDate;
		start_log timestamptz;
		finish_log timestamptz;
  begin 
	 	start_log := now();
		delete from dm.dm_account_balance_f dabf
		 where dabf.on_date = rep_date;
		insert into dm.dm_account_balance_f 
		select
			   mad.account_rk,
			   mad.currency_rk,
			   case 
				    when mad.char_type = 'А'
				    then coalesce(dabf.balance_out,0) 
				         + coalesce(datf.debet_amount,0) 
				         - coalesce (datf.credit_amount, 0) 
				    when mad.char_type = 'П'
				    then coalesce(dabf.balance_out,0) 
				    	 - coalesce(datf.debet_amount,0) 
				         + coalesce (datf.credit_amount, 0)
			    end as bal_out,
			   rep_date,
		       case 
			        when mad.char_type = 'А'
			        then coalesce(dabf.balance_out_rub,0) 
			        	 + coalesce(datf.debet_amount_rub,0) 
			        	 - coalesce (datf.credit_amount_rub, 0) 
			        when mad.char_type = 'П'
			        then coalesce(dabf.balance_out_rub,0) 
			             - coalesce(datf.debet_amount_rub,0) 
			             + coalesce (datf.credit_amount_rub, 0)
			    end as bal_out_rub
		   from ds.md_account_d mad
		   left join dm_account_balance_f dabf 
		     on dabf.account_rk = mad.account_rk
			and dabf.on_date = rep_date - 1
	       left join dm_account_turnover_f datf 
	         on datf.account_rk = mad.account_rk
	        and datf.on_date = rep_date
		  where rep_date between mad.data_actual_date 
		             and mad.data_actual_end_date;
	 finish_log := now();
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
		   'dm_account_balance_f',
		   'load data',
		   start_log,
		   finish_log,
		   concat('date of report - ', i_OnDate)
		   );
end; $$ language plpgsql
	
select *
  from dm_account_balance_f

do $$
declare 
	i int;
	dt date = to_date('2018-01-01','yyyy-mm-dd');
begin
	for i in 0..30 loop
		perform ds.fill_account_balance_f(dt + i);
	end loop;
end; $$ language plpgsql

select * from logs."load" 

select ds.fill_account_balance_f('2018-01-11')



