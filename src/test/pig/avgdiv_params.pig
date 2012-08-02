divs   = load '$input' using PigStorage('\t') as (exchange, symbol, date, dividends);
grpd   = group divs all;                                                
avgdiv = foreach grpd generate AVG(divs.dividends);                              
store avgdiv into 'average_dividend';