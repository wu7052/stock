
大宗数据 结果统计
SELECT id as `证券代码` , name as `证券名称`, sum(ass_weight) as `回购积分排名`
FROM fruit where ass_type like 'disc%' group by id order by sum(ass_weight) desc;

回购数据 结果统计