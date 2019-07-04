 


--- Wdong  5月新客
select t1.borrower_id
		,t1.loan_no
		,t1.loan_amount
		,t1.actual_amount
		,t1.service_fee
		,t3.repay_date_id
		,t3.repayed_date_id
		,t3.repay_status
		,t3.repay_type
		,t3.repay_total_amt
		,t3.repayed_total_amt
		,t3.repayed_intst
		,t3.repayed_service_fee
		,t3.repayed_penalty
		,t3.overdue_cap
		,t3.overdue_amt
		,t3.overdue_days
		,t3.overdue_back_amt
		,t3.overdue_back_cap
		,t3.overdue_back_days
		,t3.curr_overdue_amt
		,t3.curr_overdue_cap
		,t3.curr_overdue_days
		,t3.curr_penalty
from dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
left join dw_dpt.dpt_vietnam_wdong_repay_dtl_dfp t3 on t1.borrower_id =t3.borrower_id and t1.loan_no=t3.loan_no
where t1.loan_status = 1
and substr(t1.loan_time,1,10) < '2019-06-01' and substr(t1.loan_time,1,10) >'2019-04-30'
and t1.borrower_id not in (
	select t2.borrower_id 
	from dw_dpt.dpt_vietnam_wdong_loan_order_info_df t2
	where t2.loan_status = 1 and  substr(t2.loan_time,1,10) < '2019-05-01')




---- Wdong 新客

select t1.borrower_id
		,t1.loan_no
		,t1.loan_amount
		,t1.actual_amount
		,t1.service_fee
		,t1.loan_type
		,t3.repay_date_id
		,t3.repayed_date_id
		,t3.repay_status
		,t3.repay_type
		,t3.repay_total_amt
		,t3.repayed_total_amt
		,t3.repayed_intst
		,t3.repayed_service_fee
		,t3.repayed_penalty
		,t3.overdue_cap
		,t3.overdue_amt
		,t3.overdue_days
		,t3.overdue_back_amt
		,t3.overdue_back_cap
		,t3.overdue_back_days
		,t3.curr_overdue_amt
		,t3.curr_overdue_cap
		,t3.curr_overdue_days
		,t3.curr_penalty
from  dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
left join dw_dpt.dpt_vietnam_wdong_repay_dtl_dfp t3 on t1.borrower_id =t3.borrower_id and t1.loan_no=t3.loan_no

where t1.loan_status = 1 and t1.loan_type =1
and substr(t1.loan_time,1,10) < '2019-06-01' and substr(t1.loan_time,1,10) >'2019-04-30'
and t1.loan_type =1
and t3.loan_type=1









---- Wdong 新客:借款一次的客户
select  r.*
from
(
	select t1.borrower_id
			,t1.loan_no
			,t1.loan_amount
			,t1.actual_amount
			,t1.service_fee
			,t1.loan_type
	from  dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
	where t1.loan_status = 1 and t1.loan_type =1  -- 5月份的首借
	and substr(t1.loan_time,1,10) < '2019-06-01' and substr(t1.loan_time,1,10) >'2019-04-30'
)r

inner join 
(
	select borrower_id
			,count(borrower_id)as loan_times  --- 5月份首借用户的借款次数
	from  dw_dpt.dpt_vietnam_wdong_loan_order_info_df
	where loan_status = 1 and loan_type =1
	and substr(loan_time,1,10) < '2019-06-01' and substr(loan_time,1,10) >'2019-04-30'
	group by 1
)s
on r.borrower_id = s.borrower_id
where s.loan_times = 1


----Wdong新客坏账 
select sum(s.loan_amount)
from
(select t1.borrower_id
		,t1.loan_no
		,t1.loan_amount
		,t1.actual_amount
		,t1.service_fee
		,t1.loan_type
		,t3.repay_date_id
		,t3.repayed_date_id
		,t3.repay_status
		,t3.repay_type
		,t3.repay_total_amt
		,t3.repayed_total_amt
		,t3.repayed_intst
		,t3.repayed_service_fee
		,t3.repayed_penalty
		,t3.overdue_cap
		,t3.overdue_amt
		,t3.overdue_days
		,t3.overdue_back_amt
		,t3.overdue_back_cap
		,t3.overdue_back_days
		,t3.curr_overdue_amt
		,t3.curr_overdue_cap
		,t3.curr_overdue_days
		,t3.curr_penalty
from  dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
left join dw_dpt.dpt_vietnam_wdong_repay_dtl_dfp t3 on t1.borrower_id =t3.borrower_id and t1.loan_no=t3.loan_no
where t1.loan_status = 1
	 and t1.loan_type =1
	 and t3.dt=20190619
	 and substr(t1.loan_time,1,10) < '2019-06-01' and substr(t1.loan_time,1,10) >'2019-04-30'
)s


--- Wdong5月统一新客老客，根据最早日期筛选是否是新客
select n.borrower_id
		,n.loaned_times
		,row_number() over (partition by n.borrower_id order by t3.loan_time)as loan_order_time
		,substr(t3.loan_time,1,10) as every_loan_date
		,t3.loan_no
		,t3.loan_time
		,t3.loan_amount
		,t3.actual_amount
		,t3.service_fee
		,t3.loan_type
		,t4.repay_date_id
		,t4.repayed_date_id
		,t4.repay_status
		,t4.repay_type
		,t4.repay_total_amt
		,t4.repayed_total_amt
		,t4.repayed_intst
		,t4.repayed_service_fee
		,t4.repayed_penalty
		,t4.overdue_cap
		,t4.overdue_amt
		,t4.overdue_days
		,t4.overdue_back_amt
		,t4.overdue_back_cap
		,t4.overdue_back_days
		,t4.curr_overdue_amt
		,t4.curr_overdue_cap
		,t4.curr_overdue_days
		,t4.curr_penalty	
from
(
	select r.borrower_id
			,count(1)as loaned_times
	from
	(
		select s.*
		from 
		(
			select t1.borrower_id
					,min(loan_time)as loan_time
			from  dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
			group by 1
		)s
		-- 筛选出最早借款时间在5月份之间的客户，即为5月份的新客
		where substr(s.loan_time,1,10) >'2019-04-30' 
		and substr(s.loan_time,1,10) < '2019-06-01'
	)r  -- 5月份的新客
	left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t2 on r.borrower_id =t2.borrower_id
	where t2.loan_status = 1 -- 根据5月份的新客id，关联出5月份放款成功的次数
		and substr(t2.loan_time,1,10) >'2019-04-30'
		and substr(t2.loan_time,1,10) < '2019-06-01'
	group by r.borrower_id
)n
-- 注意新客: 新客在5月复借的次数，loaned_times必须大于1，复借1次等于loaned_times-1
left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t3 on n.borrower_id =t3.borrower_id
left join dw_dpt.dpt_vietnam_wdong_repay_dtl_dfp t4 on t3.borrower_id =t4.borrower_id 
												and t3.loan_no=t4.loan_no

where t3.loan_status = 1 -- 关联出5月份放款成功的次数的订单信息
	and substr(t3.loan_time,1,10) >'2019-04-30'
	and substr(t3.loan_time,1,10) < '2019-06-01'
	and t4.dt=20190620
-- where n.loaned_times = 2






--- Wdong5月统一新客老客，根据最早日期筛选是否是新客
select n.borrower_id,
		n.loaned_times
		,substr(t3.loan_time,1,10) as every_loan_date
		,row_number() over (partition by n.borrower_id order by t3.loan_time)as loan_order_time
		,t3.loan_no
		,t3.loan_time
		,t3.loan_amount
		,t3.actual_amount
		,t3.service_fee
		,t3.loan_type
		,t4.repay_date_id
		,t4.repayed_date_id
		,t4.repay_status
		,t4.repay_type
		,t4.repay_total_amt
		,t4.repayed_total_amt
		,t4.repayed_intst
		,t4.repayed_service_fee
		,t4.repayed_penalty
		,t4.overdue_cap
		,t4.overdue_amt
		,t4.overdue_days
		,t4.overdue_back_amt
		,t4.overdue_back_cap
		,t4.overdue_back_days
		,t4.curr_overdue_amt
		,t4.curr_overdue_cap
		,t4.curr_overdue_days
		,t4.curr_penalty
from
(
	select r.borrower_id,
			count(1)as loaned_times
	from
	(
		select s.*
		from 
		(
			select t1.borrower_id,
					min(loan_time)as loan_time
			from  dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
			group by 1
		)s
		-- 筛选出最早借款时间在5月份之前的客户，即为5月份的老客
		where substr(s.loan_time,1,10) <'2019-05-01' 
	)r  -- 5月份的新客
	left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t2 on r.borrower_id =t2.borrower_id
	where t2.loan_status = 1 -- 5月份的老客关联出放款成功的次数
		and substr(t2.loan_time,1,10) >'2019-04-30'
		and substr(t2.loan_time,1,10) < '2019-06-01'
	group by r.borrower_id
)n
-- 注意老客: 老客在5月复借的次数，就是loaned_times的具体值
left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t3 on n.borrower_id =t3.borrower_id
left join dw_dpt.dpt_vietnam_wdong_repay_dtl_dfp t4 on t3.borrower_id =t4.borrower_id 
												and t3.loan_no=t4.loan_no
where t3.loan_status = 1 
	and substr(t3.loan_time,1,10) >'2019-04-30'
	and substr(t3.loan_time,1,10) < '2019-06-01'
	and t4.dt=20190620





---- Wdong老客在5月份复借情况


select s.* 
		,t4.repay_date_id
		,t4.repayed_date_id
		,t4.repay_status
		,t4.repay_type
		,t4.repay_total_amt
		,t4.repayed_total_amt
		,t4.repayed_intst
		,t4.repayed_service_fee
		,t4.repayed_penalty
		,t4.overdue_cap
		,t4.overdue_amt
		,t4.overdue_days
		,t4.overdue_back_amt
		,t4.overdue_back_cap
		,t4.overdue_back_days
		,t4.curr_overdue_amt
		,t4.curr_overdue_cap
		,t4.curr_overdue_days
		,t4.curr_penalty
from
(
    select m.*
    	   ,t2.loan_no
    	   ,t2.loan_time
    	   ,row_number() over (partition by t2.borrower_id order by t2.loan_time)as loan_order_time
    from
    (
    	select r.*
    	from(
    		    select  t1.borrower_id
    					,min(t1.loan_time)as old_loan_time
    			from dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
    			where t1.loan_status = 1 
    			group by t1.borrower_id
    	)r
    	where r.old_loan_time < '2019-05-01'
    )m
    left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t2 on m.borrower_id =t2.borrower_id
    where t2.loan_status =1 
    and substr(t2.loan_time,1,10) < '2019-06-01'
)s
left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t3 on s.borrower_id =t3.borrower_id and s.loan_no=t3.loan_no
left join dw_dpt.dpt_vietnam_wdong_repay_dtl_dfp t4 on s.borrower_id =t4.borrower_id 
												and s.loan_no=t4.loan_no
where substr(s.loan_time,1,10) > '2019-04-30'








left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t2 on m.borrower_id =t2.borrower_id and m.loan_no = t2.loan_no
where t2.loan_status= 1 
and substr(t2.loan_time,1,10)  >'2019-04-30'
and substr(t2.loan_time,1,10) < '2019-06-01'





---- Wdong老客在5月份复借情况


select m.*
from(
	select  t1.borrower_id
			,t1.loan_no
			,t1.loan_time
			,row_number() over (partition by t1.borrower_id order by t1.loan_time)as loan_order_time
	from dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
			-- 在5月份之前的所有放款成功订单时间排序
	where t1.loan_status = 1 and substr(t1.loan_time,1,10) < '2019-06-01'  
)m

left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t2 on m.borrower_id =t2.borrower_id and m.loan_no = t2.loan_no
where t2.loan_status= 1 
and substr(t2.loan_time,1,10)  >'2019-04-30'
and substr(t2.loan_time,1,10) < '2019-06-01'




----Wdong 新客在5月份首借新客(没问题)

select m.*
		,t2.loan_time
		,row_number() over (partition by t2.borrower_id order by t2.loan_time)as loan_order_time
from(
	select r.*
	from(
		select  t1.borrower_id
				,min(t1.loan_time)as old_loan_time
		from dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
		where t1.loan_status = 1 
		group by t1.borrower_id

	)r
	where substr(r.old_loan_time,1,10) >'2019-04-30'
)m
left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t2 on m.borrower_id =t2.borrower_id
where t2.loan_status =1 




-- 老客综合最后结果

select f.*
-- sum(f.loan_amount) as con_total_amount
--  sum(f.loan_amount)/count(1) as avg_loan_amount,
--   sum(f.actual_amount)/count(1) as avg_actual_amount,
-- sum(f.repayed_service_fee)as repayed_service_fee ,
-- sum(f.repayed_penalty)as repayed_penalty
from
(
    select s.* 
    		,t4.repay_date_id
    		,t4.repayed_date_id
    		,t4.repay_status
    		,t4.repay_type
    		,t4.repay_total_amt
    		,t4.repayed_total_amt
    		,t4.repayed_intst
    		,t4.repayed_service_fee
    		,t4.repayed_penalty
    		,t4.overdue_cap
    		,t4.overdue_amt
    		,t4.overdue_days
    		,t4.overdue_back_amt
    		,t4.overdue_back_cap
    		,t4.overdue_back_days
    		,t4.curr_overdue_amt
    		,t4.curr_overdue_cap
    		,t4.curr_overdue_days
    		,t4.curr_penalty
    from
    (
        select m.*
        	   ,t2.loan_no
        	   ,t2.loan_time
        	   ,row_number() over (partition by t2.borrower_id order by t2.loan_time)as loan_order_time
    	    	,t2.loan_amount
    	    	,t2.actual_amount
    	     	,t2.service_fee
    	    	,t2.loan_type
        from
        (
        	select r.*
        	from(
        		    select  t1.borrower_id
        					,min(t1.loan_time)as old_loan_time
        			from dw_dpt.dpt_vietnam_wdong_loan_order_info_df t1
        			where t1.loan_status = 1 
        			group by t1.borrower_id
        	)r
        	where r.old_loan_time < '2019-05-01'
        )m
        left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df t2 on m.borrower_id =t2.borrower_id
        where t2.loan_status =1 
        and substr(t2.loan_time,1,10) < '2019-06-01'
    )s
    left join dw_dpt.dpt_vietnam_wdong_repay_dtl_dfp t4 on s.borrower_id =t4.borrower_id and s.loan_no=t4.loan_no
    
    where substr(s.loan_time,1,10) > '2019-04-30'
    and t4.dt=20190621
)