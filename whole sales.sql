#SELECT sh.id as 股票代码, sh.stock_name as 股票名称, sh.h_name as 营业部 , sh.b_vol as 买入量, sh.b_price as 买入均价, sh.b_date as 最近买入日期, sh.s_vol as 卖出量, sh.s_price as 卖出均价, sh.s_date as 最近卖出日期, dd002.close as 最近收盘价  FROM stock.ws_share_holder as sh 
#join code_002_201901 as dd002  on dd002.id = sh.id where dd002.date = '20190201'  order by sh.id desc, sh.b_date desc


#left join code_00_201901 as dd00 on dd00.id = sh.id
#left join code_30_201901 as dd30 on dd30.id = sh.id
#left join code_60_201901 as dd60 on dd60.id = sh.id  order by b_vol_tf desc;


#SELECT dd.id as ID, la.name as 名称, sum(dd.vol)/100 as 累计成交量, la.flow_shares as 流动股本, sum(dd.vol)/(flow_shares*100) as 占比, sum(dd.vol)/700 as 日均成交量 FROM stock.code_00_201901 as dd left join list_a as la on dd.id = la.id where str_to_date(dd.date,'%Y%m%d') > (current_date()-7)  group by dd.id order by  占比 desc limit 30;