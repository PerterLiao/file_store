from django.db import connection
from scheduler.utils import get_last_day
from scheduler.models import Trade_Cal

def run_period_income_old():
    # 季度收益历史
    delete_sql = 'delete from period_income'
    sql_old="""
        insert into period_income(ts_code,period,income,income_l,income_u)
        select f.ts_code,f.end_date as period,
        f.n_income_attr_p/f.total_share  as income,
        case substring(f.end_date,5,4) when '0331'then net_profit_min*10000 /f.total_share
        when '0930' then net_profit_min*10000 /f.total_share
        when '0630' then (net_profit_min*10000 - income_1) /f.total_share
        when '1231' then (net_profit_min*10000 - income_1 - income_2 - income_3)/f.total_share
        else null end as income_l,
        case substring(f.end_date,5,4) when '0331'then net_profit_max*10000 /f.total_share
        when '0930' then (net_profit_max*10000 / f.total_share)
        when '0630' then ((net_profit_max*10000 - income_1)/f.total_share)
        when '1231' then (((net_profit_max*10000) - (income_1 + income_2 + income_3))/f.total_share)
        else null end as income_u
        from
        (select a.ts_code,income_1,income_2,income_3,a.end_date,net_profit_min,net_profit_max
        from forecast a left join
        (select ts_code,end_year,sum(income_1) income_1,sum(income_2) income_2,sum(income_3) income_3
        from
        (select ts_code,end_date,
        substring(end_date,1,4) as end_year,
        case substring(end_date,5,4) when '0331' then n_income_attr_p else 0 end as income_1,
        case substring(end_date,5,4) when '0630' then n_income_attr_p else 0 end as income_2,
        case substring(end_date,5,4) when '0930' then n_income_attr_p else 0 end as income_3
        from income where report_type=2 group by ts_code,end_date ) c group by ts_code,end_year ) b on a.ts_code=b.ts_code and substring(a.end_date,1,4)=b.end_year) aa
        right join
        (select ts_code,n_income_attr_p,end_date,total_share
        from
        (select d.ts_code,n_income_attr_p,end_date,total_share
        from (select ts_code,
        n_income_attr_p,end_date
        from income
        where report_type =2
        group by ts_code,end_date
        ) d
        join
        (select ts_code,min(trade_date) min_trade,max(trade_date) max_trade,total_share*10000 as total_share
        from dailybasic
        group by ts_code,total_share
        ) as e on e.ts_code = d.ts_code and d.end_date >= e.min_trade and d.end_date <= e.max_trade
        union all
        select g.ts_code,n_income_attr_p,end_date,total_share
        from ((select ts_code,n_income_attr_p,end_date
        from income
        where report_type =2
        group by ts_code,end_date
        ) g
        join
        (select ts_code,min(trade_date) min_trade,total_share*10000 as total_share
        from dailybasic
        group by ts_code
        ) as h on h.ts_code = g.ts_code and g.end_date < h.min_trade ))t)f
        on f.ts_code = aa.ts_code and f.end_date = aa.end_date
        group by ts_code,period
        """
    cursor = connection.cursor()
    cursor.execute(delete_sql)
    cursor.execute(sql_old)
    cursor.close()

def run_period_income_new():
    # 只有预报时
    date = get_last_day()
    new_forecast_sql ='''
        insert into period_income(ts_code,period,income,income_l,income_u)
        select a.ts_code,
        a.end_date period,
        b.income income,
        case substring(a.end_date,5,4) when '0331' then a.net_profit_min/f.total_share  
        when '0630' then (a.net_profit_min -c.income_1)/f.total_share 
        when '0930' then a.net_profit_min /f.total_share 
        when '1231' then (a.net_profit_min -(c.income_1+d.income_2+e.income_3))/f.total_share 
        else null end income_l,
        case substring(a.end_date,5,4) when '0331' then a.net_profit_max/f.total_share  
        when '0630' then (a.net_profit_max -c.income_1)/f.total_share 
        when '0930' then a.net_profit_max /f.total_share 
        when '1231' then (a.net_profit_max -(c.income_1+d.income_2+e.income_3))/f.total_share 
        else null end income_u
        from (select ts_code,end_date,net_profit_min*10000 net_profit_min,net_profit_max * 10000 net_profit_max
        from forecast 
        where ann_date = {} )a 
        left join 
        (select ts_code,max(end_date) end_date,n_income_attr_p income 
        from income 
        where report_type=2 
        group by ts_code) b on a.ts_code = b.ts_code
        left join 
        (select ts_code,max(end_date) end_date,n_income_attr_p income_1 
        from income 
        where report_type =2 and substring(end_date,5,4) = '0331'
        group by ts_code ) c on a.ts_code = c.ts_code
        left join 
        (select ts_code,max(end_date) end_date,n_income_attr_p income_2 
        from income 
        where report_type =2 and substring(end_date,5,4) = '0630'
        group by ts_code ) d on a.ts_code = d.ts_code
        left join
        (select ts_code,max(end_date) end_date,n_income_attr_p income_3 
        from income 
        where report_type =2 and substring(end_date,5,4) = '0930'
        group by ts_code ) e on a.ts_code = e.ts_code
        left join
        (select ts_code,max(trade_date) trade_date,total_share * 10000 as total_share
        from dailybasic
        group by ts_code ) f on a.ts_code = f.ts_code
        '''.format(date)
    
    # 有快报无财报（利润表）的sql
    new_express_sql = '''
        replace into period_income(ts_code,period,income,income_l,income_u)
        select a.ts_code,
        a.end_date period,
        case substring(a.end_date,5,4) when '0331' then a.n_income/f.total_share  
        when '0630' then (a.n_income -c.income_1)/f.total_share 
        when '0930' then a.n_income /f.total_share 
        when '1231' then (a.n_income -(c.income_1+d.income_2+e.income_3))/f.total_share 
        else null end income,
        income_l,
        income_u
        from 
        (select ts_code,end_date,n_income
        from express
        where ann_date = {}) a
        left join
        (select ts_code,max(period) period,income_l,income_u 
        from period_income 
        group by ts_code) b on a.ts_code = b.ts_code
        left join 
        (select ts_code,max(end_date) end_date,n_income_attr_p income_1 
        from income 
        where report_type =2 and substring(end_date,5,4) = '0331'
        group by ts_code ) c on a.ts_code = c.ts_code
        left join 
        (select ts_code,max(end_date) end_date,n_income_attr_p income_2 
        from income 
        where report_type =2 and substring(end_date,5,4) = '0630'
        group by ts_code ) d on a.ts_code = d.ts_code
        left join
        (select ts_code,max(end_date) end_date,n_income_attr_p income_3 
        from income 
        where report_type =2 and substring(end_date,5,4) = '0930'
        group by ts_code ) e on a.ts_code = e.ts_code
        left join
        (select ts_code,max(trade_date) trade_date,total_share * 10000 as total_share
        from dailybasic
        group by ts_code ) f on a.ts_code = f.ts_code
        '''.format(date)

    # 利润表发布
    new_income_sql = '''
        replace into period_income(ts_code,period,income,income_l,income_u)
        select a.ts_code,
        a.end_date period,
        a.n_income_attr_p income,
        income_l,
        income_u
        from 
        (select ts_code,
        n_income_attr_p,
        end_date
        from income
        where report_type = 2 and ann_date = {}) a
        left join 
        (select ts_code,max(period),income_l,income_u
        from period_income
        group by ts_code ) b
        on a.ts_code = b.ts_code
        '''.format(date)
    cursor = connection.cursor()
    cursor.execute(new_forecast_sql)
    cursor.execute(new_express_sql)
    cursor.execute(new_income_sql)
    cursor.close()

def run_report_stat(date=None):
    if date is None:
        date = get_last_day()

    pd_day1 = list(Trade_Cal.objects.values('cal_date').filter(is_open=1,cal_date__lt=date)[:10])[9]['cal_date']
    pd_day2 = list(Trade_Cal.objects.values('cal_date').filter(is_open=1,cal_date__lt=date)[:20])[19]['cal_date']
    pd_day3 = list(Trade_Cal.objects.values('cal_date').filter(is_open=1,cal_date__lt=date)[:30])[29]['cal_date']
    pd_day4 = list(Trade_Cal.objects.values('cal_date').filter(is_open=1,cal_date__lt=date)[:40])[39]['cal_date']
    pd_day5 = list(Trade_Cal.objects.values('cal_date').filter(is_open=1,cal_date__lt=date)[:50])[49]['cal_date']
    pd_day6 = list(Trade_Cal.objects.values('cal_date').filter(is_open=1,cal_date__lt=date)[:60])[59]['cal_date']
    pd_day7 = list(Trade_Cal.objects.values('cal_date').filter(is_open=1,cal_date__lt=date)[:70])[69]['cal_date']
    pd_day8 = list(Trade_Cal.objects.values('cal_date').filter(is_open=1,cal_date__lt=date)[:80])[79]['cal_date']
    report_sql='''
        replace into report_stat(report_date,ts_code,name,sum_pre_quarter,eps_l,eps_u,curr_pe,curr_pe_l,curr_pe_u,price,val_m,val_l,val_u,pd,pd_l,pd_u,ci_market_value,pe_fix,co_pe,total_share,float_share,grossprofit_margin,cust_parm_1,co,capital_rese_ps,dps,undist_profit_ps,ocfps,pledge_ratio,goodwill,market_segments_wind,market_segments_cust1,market_segments_cust2,market,area,pd_10,pd_20,pd_30,pd_40,pd_50,pd_60,pd_70,pd_80,holder_number,trade_ann_date,trade_num)
        select DATE report_date,AF.ts_code,name, 
        (am + ap + aq +ar) as sum_pre_quarter, 
        (am + ao + aq + ar) as eps_l , 
        (am+an+aq+ar) as eps_u , 
        (price/(am + ap + aq +ar)) as curr_pe, 
        (price/(am + ao + aq + ar)) as curr_pe_l, 
        (price/(am+an+aq+ar))  as curr_pe_u, 
        price,
        (30* pow(0.9,log(1.5,price * float_share/pow(10,8)/9)) * co*(am+ap+aq+ar))  as val_m, 
        (30* pow(0.9,log(1.5,price * float_share/pow(10,8)/9)) * co*(am + ao + aq + ar)) as val_l, 
        (30* pow(0.9,log(1.5,price * float_share/pow(10,8)/9)) * co*(am+an+aq+ar))  as val_u, 
        ((30* pow(0.9,log(1.5,(price * float_share/pow(10,8)/9))) * co*(am+ap+aq+ar) - price)/price ) as pd, 
        ((30* pow(0.9,log(1.5,(price * float_share/pow(10,8)/9))) * co*(am + ao + aq + ar) - price)/price ) as pd_l, 
        ((30* pow(0.9,log(1.5,(price * float_share/pow(10,8)/9))) * co*(am + an + aq + ar) - price)/price ) as pd_u,
        (price*float_share/pow(10,8)) as ci_market_value , 
        (30* pow(0.9,log(1.5,(price * float_share/pow(10,8)/9)))) as pe_fix , 
        (30* pow(0.9,log(1.5,(price * float_share/pow(10,8)/9))))*co as co_pe , 
        total_share,
        float_share,
        grossprofit_margin,
        cust_parm_1,
        co,
        capital_rese_ps,
        bps,
        undist_profit_ps,
        ocfps,
        pledge_ratio,
        goodwill,
        market_segments_wind,
        market_segments_cust1,
        market_segments_cust2,
        market,
        area,
        (price-price_10)/price_10 as pd_10,
        (price-price_20)/price_20 as pd_20,
        (price-price_30)/price_30 as pd_30,
        (price-price_40)/price_40 as pd_40,
        (price-price_50)/price_50 as pd_50,
        (price-price_60)/price_60 as pd_60,
        (price-price_70)/price_70 as pd_70,
        (price-price_80)/price_80 as pd_80,
        holder_num,
        trade_ann_date,
        trade_num
        from  
        (select ts_code,name,area from basic) AF left join  
        (select ts_code,close as price  
        ,max(trade_date) as trade_date from daily where trade_date <= DATE group by ts_code) as G on AF.ts_code = G.ts_code 
         left join  
        (select ts_code,
        total_share * 10000 as total_share,
        float_share * 10000 as float_share ,max(trade_date) as trade_date
        from dailybasic where trade_date <= DATE group by ts_code) as Q_R on AF.ts_code = Q_R.ts_code 
        left  join 
        (select 
        ts_code,
        grossprofit_margin/100 as grossprofit_margin,
        (grossprofit_margin/100-0.3) as cust_parm_1,
        (1+ (grossprofit_margin/100-0.3) )as co,
        capital_rese_ps,
        bps,
        undist_profit_ps,
        ocfps ,
        max(ann_date) as ann_date 
        from finaindicator where ann_date <= DATE group by ts_code
        ) as S_Y on AF.ts_code = S_Y.ts_code 
        left  join 
        (select ts_code,pledge_ratio ,max(end_date) as end_date
                from pledge_stat where end_date <= DATE group by ts_code)
        as Z on AF.ts_code = Z.ts_code 
        left  join 
        (select ts_code, goodwill 
        ,max(ann_date) as ann_date from balancesheet where ann_date <= DATE group by ts_code) as AA on AF.ts_code = AA.ts_code 
        left  join 
        (select ts_code,market_segments_wind,market_segments_cust1,market_segments_cust2,market
        from market_segment) mkt on mkt.ts_code = AF.ts_code
        left join 
        (select m.ts_code,period_am,am,an,ao,period_ap,ap,period_aq,aq,period_ar,ar
        from 
        (select ts_code,period as period_am,income as am
        from period_income a 
        where period<= DATE and (select count(*) from period_income b where b.period<= DATE and  a.ts_code=b.ts_code and a.period < b.period ) = 0) m
         left join
        (select ts_code,period as period_ap ,income as ap
        from period_income a 
        where  period <= DATE and (select count(*) from period_income b where b.period<=DATE and  a.ts_code=b.ts_code and a.period < b.period ) = 1) p
        on m.ts_code = p.ts_code
        left join 
        (select ts_code,period as period_aq ,income as aq
        from period_income a 
        where period <= DATE and (select count(*) from period_income b where b.period <= DATE and a.ts_code=b.ts_code and a.period < b.period ) = 2) q
        on m.ts_code = q.ts_code
        left join 
        (select ts_code,period as period_ar,income as ar 
        from period_income a 
        where period <= DATE and (select count(*) from period_income b where b.period <= DATE and  a.ts_code=b.ts_code and a.period < b.period ) = 3) r
        on m.ts_code = r.ts_code
        left join
        (select ts_code,income_u as an
        from period_income a 
        where period <=DATE and (select count(*) from period_income b where b.period <= DATE and a.ts_code=b.ts_code and a.period < b.period ) = 1) n
        on m.ts_code = n.ts_code
        left join
        (select ts_code,income_l as ao
        from period_income a 
        where period<=DATE and (select count(*) from period_income b where b.period <= DATE and a.ts_code=b.ts_code and a.period < b.period ) = 1) o
        on m.ts_code = o.ts_code
        ) as am_ar
        on AF.ts_code = am_ar.ts_code
        left join
        (select ts_code,close as price_10
        from daily a where trade_date = PD_DAY1 )  p_10
        on p_10.ts_code = AF.ts_code
        left join
        (select ts_code,close as price_20
        from daily a where trade_date = PD_DAY2)  p_20
        on p_20.ts_code = AF.ts_code
        left join 
        (select ts_code,close as price_30
        from daily a where trade_date = PD_DAY3) p_30
        on p_30.ts_code = AF.ts_code
        left join
        (select ts_code,close as price_40
        from daily a where trade_date = PD_DAY4)  p_40
        on p_40.ts_code = AF.ts_code
        left join
        (select ts_code,close as price_50
        from daily a where trade_date = PD_DAY5)  p_50
        on p_50.ts_code = AF.ts_code
        left join
        (select ts_code,close as price_60
        from daily a where trade_date = PD_DAY6)  p_60
        on p_60.ts_code = AF.ts_code
        left join
        (select ts_code,close as price_70
        from daily a where trade_date =PD_DAY7)  p_70
        on p_70.ts_code = AF.ts_code
        left join
        (select ts_code,close as price_80
        from daily a where trade_date =PD_DAY8)  p_80
        on p_80.ts_code = AF.ts_code
        left join
        (select ts_code,max(ann_date),holder_num
        from stk_holdernumber
        where ann_date <= DATE 
        group by ts_code) stk_num
        on stk_num.ts_code = AF.ts_code
        left join 
        (select ts_code,max(ann_date) as trade_ann_date,
        case in_de when 'IN' then change_vol 
                when 'DE' then 0-change_vol
                else 0 end as trade_num
        from stk_holdertrade
        where ann_date <= DATE 
        group by ts_code) stk_trade
        on stk_trade.ts_code = AF.ts_code
        '''
    report_sql = report_sql.replace('DATE',date)
    report_sql = report_sql.replace('PD_DAY1',pd_day1)
    report_sql = report_sql.replace('PD_DAY2',pd_day2)
    report_sql = report_sql.replace('PD_DAY3',pd_day3)
    report_sql = report_sql.replace('PD_DAY4',pd_day4)
    report_sql = report_sql.replace('PD_DAY5',pd_day5)
    report_sql = report_sql.replace('PD_DAY6',pd_day6)
    report_sql = report_sql.replace('PD_DAY7',pd_day7)
    report_sql = report_sql.replace('PD_DAY8',pd_day8)
    cursor = connection.cursor()
    if date is None:
        run_period_income_new()
    cursor.execute(report_sql)
    cursor.close()

def run_report_stat_old(start_day=None):
    if start_day is None:
        start_day='20200801'
    now_day = get_last_day()
    days=list(Trade_Cal.objects.values('cal_date').filter(cal_date__lte=now_day,cal_date__gte=start_day))
    for day in days:
        run_report_stat(date=day['cal_date'])
        print(day['cal_date']+'_report OK!')
