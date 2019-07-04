CREATE TABLE if not exists rpt.vietnam_wdong_month_batch_info
(
	first_year_month string comment '最早借款年月批次'
	,loan_history int comment '借款次序：0表示新增借款，1表示复借第一次,2表示第二次复借,依此类推'
	,loan_num int comment '放款笔数'
	,next_apply_num int comment '下一次借款申请人数'
	,next_loan_num int comment '下一次放款人数'
	,reloan_apply_rate decimal(6,2) comment '复借申请率'
	,reloan_pass_rate decimal(6,2) comment '复借通过率'
	,total_repay_amount decimal(24,2) comment '总应还合同金额'
	,avg_loan_amount decimal(24,2) comment '平均合同金额'
	,avg_actual_amount decimal(24,2) comment '平均实际放款金额'
	,total_repay_service_fee decimal(24,2) comment '总应还服务费'
	,total_repay_intst decimal(24,2) comment '总应还利息'
	,total_repayed_penalty decimal(24,2) comment '总已还罚息'
	,total_income decimal(24,2) comment '总计收=(总应还服务费+总应还利息+总已还罚息)/1.1'
	,in_urge decimal(24,2) comment '入催金额'
	,in_urge_rate decimal(6,2) comment '入催率'
	,out_urge decimal(24,2) comment '出催金额'
	,out_urge_rate decimal(6,2) comment '出催率'
	,urge_days_1_7 decimal(24,2) comment '逾期T1-7天总合同金额'
	,urge_days_1_7_rate decimal(6,2) comment '逾期T1-7在逾率'
	,urge_days_8_15 decimal(24,2) comment '逾期T8-15天总合同金额'
	,urge_days_8_15_rate decimal(6,2) comment '逾期T8-15在逾率'
	,urge_days_16_30 decimal(24,2) comment '逾期T16-30天总合同金额'
	,urge_days_16_30_rate decimal(6,2) comment '逾期T16-30在逾率'
	,bad_debt decimal(24,2) comment '总坏账金额：逾期30天以上的总合同金额'
	,bad_debt_rate decimal(6,2) comment '坏账率'
)
comment '越南现金贷，星合Wdong系统，每月放款批次更新表'
PARTITIONED BY (dt int)
;


insert overwrite table rpt.vietnam_wdong_month_batch_info PARTITION(dt='${dt}')
select  f.first_year_month
        ,f.loan_history
        ,f.loan_num
        ,f.next_apply_num
        ,f.next_loan_num
        ,(case when f.loan_num !=0 then (1.00*f.next_apply_num/f.loan_num) else 0 end)as reloan_apply_rate
        ,(case when f.next_apply_num !=0 then (1.00*f.next_loan_num/f.next_apply_num) else 0 end)as reloan_pass_rate
        ,f.total_repay_amount
        ,f.avg_loan_amount
        ,f.avg_actual_amount
        ,f.total_repay_service_fee
        ,f.total_repay_intst
        ,f.total_repayed_penalty
        ,((f.total_repay_service_fee+f.total_repay_intst+f.total_repayed_penalty)/1.1)as total_income
        ,f.in_urge
        ,(1.00*f.in_urge/f.total_repay_amount)as in_urge_rate
        ,f.out_urge
        ,(case when f.in_urge !=0 then (1.00*f.out_urge/f.in_urge) else 0 end)as out_urge_rate
        ,f.urge_days_1_7
        ,(1.00*f.urge_days_1_7/f.total_repay_amount)as urge_days_1_7_rate
        ,f.urge_days_8_15
        ,(1.00*f.urge_days_8_15/f.total_repay_amount)as urge_days_8_15_rate
        ,f.urge_days_16_30
        ,(1.00*f.urge_days_16_30/f.total_repay_amount)as urge_days_16_30_rate
        ,f.bad_debt
        ,(1.00*f.bad_debt/f.total_repay_amount)as bad_debt_rate
from
(
    select r1.first_year_month
    		,r1.loan_history
    		,r1.loan_num
    		,(case when r1.total_repay_amount !=0 then r1.total_repay_amount else 0 end)as total_repay_amount
    		,r2.next_apply_num
    		,(case when r1.next_loan_num is null then 0 else r1.next_loan_num end)as next_loan_num
    		,r1.avg_loan_amount
    		,r1.avg_actual_amount
    		,r1.total_repay_service_fee
    		,r1.total_repay_intst
    		,r1.total_repayed_penalty
    		,(case when r1.in_urge is null then 0 else r1.in_urge end)as in_urge
    		,(case when r1.out_urge is null then 0 else r1.out_urge end)as out_urge
    		,(case when r1.urge_days_1_7 is null then 0 else r1.urge_days_1_7 end)as urge_days_1_7
    		,(case when r1.urge_days_8_15 is null then 0 else r1.urge_days_8_15 end)as urge_days_8_15
    		,(case when r1.urge_days_16_30 is null then 0 else r1.urge_days_16_30 end)as urge_days_16_30
    		,(case when r1.bad_debt is null then 0 else r1.bad_debt end) as bad_debt
    from
    (
    	select n.first_year_month
    			,(n.loan_history-1)as loan_history
    			,n.loan_num
    			,n.total_repay_amount
    			,lead(n.loan_num) OVER(PARTITION BY n.first_year_month ORDER BY n.loan_num desc) as next_loan_num
    			,n.avg_loan_amount
    			,n.avg_actual_amount
    			,n.total_repay_service_fee
    			,n.total_repay_intst
    			,n.total_repayed_penalty
    			,n.in_urge
    			,n.out_urge
    			,n.urge_days_1_7
    			,n.urge_days_8_15
    			,n.urge_days_16_30
    			,n.bad_debt
    		from
    	(
    		select first_year_month
    				,loan_history
    				,count(t2.borrower_id)as loan_num
    				,sum(t2.loan_amount)as total_repay_amount
    				,sum(t2.loan_amount)/count(t2.borrower_id) as avg_loan_amount
    				,sum(t2.actual_amount)/count(t2.borrower_id) as avg_actual_amount
    				,sum(case when t2.curr_overdue_days>0 then t2.loan_amount end) as in_urge -- 入催
    				,sum(case when t2.overdue_days>0 and t2.curr_overdue_days = 0 then t2.loan_amount end) as out_urge
    				,sum(case when (t2.repay_status =0) and t2.curr_overdue_days >=1 and t2.curr_overdue_days <=7 then t2.loan_amount end) as urge_days_1_7 
    				,sum(case when (t2.repay_status =0) and t2.curr_overdue_days >=8 and t2.curr_overdue_days <=15 then t2.loan_amount end) as urge_days_8_15
    				,sum(case when (t2.repay_status =0) and t2.curr_overdue_days >=16 and t2.curr_overdue_days <=30 then t2.loan_amount end) as urge_days_16_30
    				,sum(case when (t2.repay_status =0) and t2.curr_overdue_days >30 then t2.loan_amount end) as bad_debt
    				,sum(t2.repay_service_fee)as total_repay_service_fee
    				,sum(t2.repay_intst)as total_repay_intst
    				,sum(t2.repayed_penalty)as total_repayed_penalty
    		from
    		(
    			select a.*
    				   ,b.loan_no
    				   ,b.loan_time
    				   ,b.apply_time
    				   ,row_number() over(partition by a.borrower_id,a.first_year_month order by b.loan_time)as loan_history
    				   ,lead(b.apply_time) OVER(PARTITION BY a.borrower_id ORDER BY b.apply_time) as next_apply_time  --apply_time的下一个记录的时间
    			from
    				(select borrower_id
    						,substr(min(loan_time),1,7)as first_year_month
    					from  dw_dpt.dpt_vietnam_wdong_loan_order_info_df
    					where loan_status =1
    					group by 1   ---历史所有放款成功表和该用户对应首借的最早年份月份
    				)a
    			left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df b  ---找出以上客户的所有成功放款订单信息
    			on a.borrower_id = b.borrower_id
    			where b.loan_status = 1 
    			and b.loan_time is not null -- 过滤掉成功放款，但放款时间为空值的情况
    		)t1
    		left join dw_dpt.dpt_vietnam_wdong_repay_dtl_dfp t2 on t1.borrower_id =t2.borrower_id and t1.loan_no=t2.loan_no
    		where t2.dt='${dt}' 
    		group by 1,2
    		order by 1,2
    	)n
    	order by 1,2
    )r1

    left join 
    (
        select m.first_year_month
        		,(m.loan_history -1)as loan_history
        		,(case when m.next_apply_num is null then 0 else m.next_apply_num end)as next_apply_num
        from
        (
        	select first_year_month
        	    	,loan_history
        	    	,count(case when t1.next_apply_time>t1.loan_time then 1 end) as next_apply_num
        	from
        	(	select a.*
        			   ,b.loan_no
        			   ,b.loan_time
        			   ,b.apply_time
        			   ,row_number() over(partition by a.borrower_id,a.first_year_month order by b.loan_time)as loan_history
        			   ,lead(b.apply_time) OVER(PARTITION BY a.borrower_id ORDER BY b.apply_time) as next_apply_time  --apply_time的下一个记录的时间
        		from
        			(select borrower_id
        					,substr(min(loan_time),1,7)as first_year_month
        				from  dw_dpt.dpt_vietnam_wdong_loan_order_info_df
        				where loan_status =1
        				group by 1   ---历史所有放款成功表和该用户对应首借的最早年份月份
        			)a
        		left join dw_dpt.dpt_vietnam_wdong_loan_order_info_df b  ---找出以上客户的所有放款信息，包括放款失败信息
        		on a.borrower_id = b.borrower_id
        	)t1
        	where t1.loan_time is not null
        	group by 1,2
        	order by 1,2
        )m
    )r2
    on r1.first_year_month= r2.first_year_month and r1.loan_history = r2.loan_history
    order by 1,2
)f