// Package policy provides facilities for creating policy from SQL-like language.
//   eBNF grammar is provided in `grammar.ebnf` for illustration.
//
// Current limitations:
// 1. Grouping filter expressions in parenthesis is not supported right now.
//    Requiring this will make query too verbose, making it optional makes
//    our grammar not LL(1). This can be supported in future.
// 2. Filters must be defined before they are used.
// 	This requirement may be relaxed in future.
//
// Example query:
// REP 1 in SPB
// REP 2 in Americas
// CBF 4
// SELECT 1 Node IN City FROM SPBSSD AS SPB
// SELECT 2 Node IN SAME City FROM Americas AS Americas
// FILTER SSD EQ true AS IsSSD
// FILTER @IsSSD AND Country eq "RU" AND City eq "St.Petersburg" AS SPBSSD
// FILTER 'Continent' == 'North America' OR Continent == 'South America' AS Americas
package policy
