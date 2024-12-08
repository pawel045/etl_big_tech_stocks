-- create table for raport
DROP TABLE IF EXISTS `stocks-428319.tech_stocks.tech_big5_raport_2024`;

CREATE TABLE `stocks-428319.tech_stocks.tech_big5_raport_2024` AS 
    SELECT * 
    FROM (
        SELECT 
            stock_date
            ,company
            ,close
        FROM `stocks-428319.tech_stocks.tech_big5`
        WHERE LEFT(stock_date, 4) = '2024'
    )
    PIVOT (
        MAX(close) FOR company IN ('GOOG' as google, 'AMZN' as amazon, 'AAPL' as apple, 'META' as meta, 'MSFT' as microsoft)
    );
