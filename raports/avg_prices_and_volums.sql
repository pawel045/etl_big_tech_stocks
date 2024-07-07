-- get avarage prices/volumes by date
select 
  parse_date('%Y-%m-%d', stock_date) as stock_date
  ,avg(open) as open
  ,avg(close) as close
  ,avg(high) as high
  ,avg(low) as low
  ,avg(volume) as volume
from `stocks-428319.tech_stocks.tech_big5`
where company <> 'TEST' or company <> 'BIG5'
group by 1
order by 1;
