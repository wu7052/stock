select fr.id, fr.name, fr.totaloperatereve, fr.ystz, fr.parentnetprofit, fr.sjltz, fr.roe, fr.bps, dd.close, fr.xsmll 
from dd_qfq_002_201901 as dd , fin_report as fr 
where dd.id = fr.id and dd.close <= fr.bps  and fr.type='2019Q2' and dd.date='20191011' and fr.ystz > 0 and fr.sjltz > 0
union all
select fr.id, fr.name, fr.totaloperatereve, fr.ystz, fr.parentnetprofit, fr.sjltz, fr.roe, fr.bps, dd.close, fr.xsmll 
from dd_qfq_00_201901 as dd , fin_report as fr 
where dd.id = fr.id and dd.close <= fr.bps  and fr.type='2019Q2' and dd.date='20191011' and fr.ystz > 0 and fr.sjltz > 0
union all
select fr.id, fr.name, fr.totaloperatereve, fr.ystz, fr.parentnetprofit, fr.sjltz, fr.roe, fr.bps, dd.close, fr.xsmll 
from dd_qfq_30_201901 as dd , fin_report as fr 
where dd.id = fr.id and dd.close <= fr.bps  and fr.type='2019Q2' and dd.date='20191011' and fr.ystz > 0 and fr.sjltz > 0
union all
select fr.id, fr.name, fr.totaloperatereve, fr.ystz, fr.parentnetprofit, fr.sjltz, fr.roe, fr.bps, dd.close, fr.xsmll 
from dd_qfq_60_201901 as dd , fin_report as fr 
where dd.id = fr.id and dd.close <= fr.bps  and fr.type='2019Q2' and dd.date='20191011' and fr.ystz > 0 and fr.sjltz > 0
