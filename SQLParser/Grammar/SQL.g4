grammar SQL;

options
{
    language=CSharp;
}

//Everything inside the @header section will be placed at the start of your Parser class
@header {
	using System;
}
 
@parser::members
{
    protected const int EOF = Eof;
}
 
@lexer::members
{
    protected const int EOF = Eof;
    protected const int HIDDEN = Hidden;
}


/*
 * Parser Rules
 */

//parse : ( sql_stmt_list | error )* EOF;
parse : (sql_stmt_list | error);

error : UNEXPECTED_CHAR 
   { 
     throw new Exception("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text); 
   }
 ;

 


sql_stmt_list : ';'* sql_stmt ( ';'+ sql_stmt )* ';'* ;

sql_stmt : ( select_stmt) ;

 
select_stmt : select_or_values ( compound_operator select_or_values )*  ( K_ORDER K_BY ordering_term ( ',' ordering_term )* )?
 ;

select_or_values
 : K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
   ( K_FROM ( table_or_subquery ( ',' table_or_subquery )* | join_clause ) )?
   ( K_WHERE expr )?
   ( K_PREFERENCE expr )?
   ( K_GROUP K_BY expr ( ',' expr )* ( K_HAVING expr )? )?
 | K_VALUES '(' expr ( ',' expr )* ')' ( ',' '(' expr ( ',' expr )* ')' )*
 ;

type_name : name+ ( '(' signed_number ')' | '(' signed_number ',' signed_number ')' )?
 ;


/*
    SQL understands the following binary operators, in order from highest to
    lowest precedence:

    ||
    *    /    %
    +    -
    <<   >>   &    |
    <    <=   >    >=
    =    ==   !=   <>   IS   IS NOT   IN   LIKE   MATCH   REGEXP
    AND
    OR
*/

expr
 : literal_value																						#opLiteral
 | column_term																							#opDatabaseName
 | unary_operator expr																					#opUnary
 | expr '||' expr																						#opOr
 | expr ( '*' | '/' | '%' ) expr																		#opPoint
 | expr ( '+' | '-' ) expr																				#opLine
 | expr ( '<<' | '>>' | '&' | '|' ) expr																#opDoubleOrder
 | expr ( '<' | '<=' | '>' | '>=' ) expr																#opOrder
 | expr ( '=' | '==' | '!=' | '<>' | K_IS | K_IS K_NOT | K_IN | K_LIKE | K_MATCH ) expr					#opequal
 | expr K_AND expr																	#exprand
 | expr K_OR expr																	#expror
 | function_name '(' ( K_DISTINCT? expr ( ',' expr )* | '*' )? ')'					#function
 | '(' expr ')'																		#exprInBracket
 | K_CAST '(' expr K_AS type_name ')'												#cast
 | expr K_COLLATE collation_name													#collate
 | expr K_NOT? ( K_LIKE | K_MATCH ) expr ( K_ESCAPE expr )?		#not
 | expr ( K_ISNULL | K_NOTNULL | K_NOT K_NULL )										#isNull
 | expr K_IS K_NOT? expr															#isNot
 | expr K_NOT? K_BETWEEN expr K_AND expr											#notBetween
 | expr K_NOT? K_IN ( '(' ( select_stmt												
                          | expr ( ',' expr )*
                          )? 
                      ')'															
                    | ( database_name '.' )? table_name )							#notIn
 | ( ( K_NOT )? K_EXISTS )? '(' select_stmt ')'										#notExists
 | K_CASE expr? ( K_WHEN expr K_THEN expr )+ ( K_ELSE expr )? K_END					#case
 //Don't use expression. The word after Low or High must be a column name!!
 | op=(K_LOW | K_HIGH) column_term ('{' expr '}')?									#preferenceLOWHIGH
 | column_term op=(K_AROUND | K_FAVOUR | K_DISFAVOUR) expr							#preferenceAROUND
 | '(' expr ',' expr ')'															#geocoordinate
 //| K_WEIGHTED expr													#prefWeighted
 ;


column_term : ( ( database_name '.' )? table_name '.' )? column_name;
ordering_term : expr ( K_COLLATE collation_name )? ( K_ASC | K_DESC )? ;

result_column : '*' | table_name '.' '*'  | expr ( K_AS? column_alias )?;

table_or_subquery
 : ( database_name '.' )? table_name ( K_AS? table_alias )?
 | '(' ( table_or_subquery ( ',' table_or_subquery )*
       | join_clause )
   ')' ( K_AS? table_alias )?
 | '(' select_stmt ')' ( K_AS? table_alias )?
 ;

join_clause : table_or_subquery ( join_operator table_or_subquery join_constraint )* ;

join_operator: ',' | K_NATURAL? ( K_LEFT K_OUTER? | K_INNER | K_CROSS )? K_JOIN;

join_constraint: ( K_ON expr  | K_USING '(' column_name ( ',' column_name )* ')' )?;

select_core
 : K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
   ( K_FROM ( table_or_subquery ( ',' table_or_subquery )* | join_clause ) )?
   ( K_WHERE expr )?
   ( K_PREFERENCE expr )?
   ( K_GROUP K_BY expr ( ',' expr )* ( K_HAVING expr )? )?
 | K_VALUES '(' expr ( ',' expr )* ')' ( ',' '(' expr ( ',' expr )* ')' )*
 ;

compound_operator: K_UNION | K_UNION K_ALL | K_INTERSECT | K_EXCEPT;

signed_number : ( '+' | '-' )? NUMERIC_LITERAL;

literal_value
 : NUMERIC_LITERAL
 | STRING_LITERAL
 | K_NULL
 | K_CURRENT_TIME
 | K_CURRENT_DATE
 | K_CURRENT_TIMESTAMP
 ;

unary_operator
 : '-'
 | '+'
 | '~'
 | K_NOT
 ;

error_message: STRING_LITERAL;


column_alias: IDENTIFIER | STRING_LITERAL;

keyword
 : K_ALL
 | K_AND
 | K_AS
 | K_ASC
 | K_BETWEEN
 | K_BY
 | K_CASCADE
 | K_CASE
 | K_CAST
 | K_CHECK
 | K_COLLATE
 | K_CROSS
 | K_CURRENT_DATE
 | K_CURRENT_TIME
 | K_CURRENT_TIMESTAMP
 | K_DESC
 | K_DISTINCT
 | K_ELSE
 | K_END
 | K_ESCAPE
 | K_EXCEPT
 | K_EXISTS
 | K_FROM
 | K_FULL
 | K_GROUP
 | K_HAVING
 | K_IN
 | K_INNER
 | K_INTERSECT
 | K_IS
 | K_ISNULL
 | K_JOIN
 | K_LEFT
 | K_LIKE
 | K_MATCH
 | K_NATURAL
 | K_NO
 | K_NOT
 | K_NOTNULL
 | K_NULL
 | K_OF
 | K_ON
 | K_OR
 | K_ORDER
 | K_OUTER
 | K_RIGHT
 | K_SELECT
 | K_TABLE
 | K_THEN
 | K_UNION
 | K_USING
 | K_VALUES
 | K_WHEN
 | K_WHERE
 //Preference
 | K_AROUND
 | K_DISFAVOUR
 | K_FAVOUR
 | K_HIGH
 | K_LOW
 | K_OTHERS
 | K_PREFERENCE
 | K_WEIGHTED
 ;


name : any_name;

function_name : any_name;

database_name : any_name;

table_name : any_name;


new_table_name : any_name;

column_name : any_name;

collation_name : any_name;

table_alias : any_name;

any_name : IDENTIFIER 
 | keyword
 | STRING_LITERAL
 | '(' any_name ')'
 ;

SCOL : ';';
DOT : '.';
OPEN_PAR : '(';
CLOSE_PAR : ')';
COMMA : ',';
ASSIGN : '=';
STAR : '*';
PLUS : '+';
MINUS : '-';
TILDE : '~';
PIPE2 : '||';
DIV : '/';
MOD : '%';
LT2 : '<<';
GT2 : '>>';
AMP : '&';
PIPE : '|';
LT : '<';
LT_EQ : '<=';
GT : '>';
GT_EQ : '>=';
EQ : '==';
NOT_EQ1 : '!=';
NOT_EQ2 : '<>';

// SQL Keywords
K_ALL : A L L;
K_AND : A N D;
K_AS : A S;
K_ASC : A S C;
K_BETWEEN : B E T W E E N;
K_BY : B Y;
K_CASCADE : C A S C A D E;
K_CASE : C A S E;
K_CAST : C A S T;
K_CHECK : C H E C K;
K_COLLATE : C O L L A T E;
K_CROSS : C R O S S;
K_CURRENT_DATE : C U R R E N T '_' D A T E;
K_CURRENT_TIME : C U R R E N T '_' T I M E;
K_CURRENT_TIMESTAMP : C U R R E N T '_' T I M E S T A M P;
K_DESC : D E S C;
K_DISTINCT : D I S T I N C T;
K_ELSE : E L S E;
K_END : E N D;
K_ESCAPE : E S C A P E;
K_EXCEPT : E X C E P T;
K_EXISTS : E X I S T S;
K_FROM : F R O M;
K_FULL : F U L L;
K_GROUP : G R O U P;
K_HAVING : H A V I N G;
K_IN : I N;
K_INNER : I N N E R;
K_INTERSECT : I N T E R S E C T;
K_IS : I S;
K_ISNULL : I S N U L L;
K_JOIN : J O I N;
K_LEFT : L E F T;
K_LIKE : L I K E;
K_MATCH : M A T C H;
K_NATURAL : N A T U R A L;
K_NO : N O;
K_NOT : N O T;
K_NOTNULL : N O T N U L L;
K_NULL : N U L L;
K_OF : O F;
K_ON : O N;
K_OR : O R;
K_ORDER : O R D E R;
K_OUTER : O U T E R;
K_RIGHT : R I G H T;
K_SELECT : S E L E C T;
K_TABLE : T A B L E;
K_THEN : T H E N;
K_UNION : U N I O N;
K_USING : U S I N G;
K_VALUES : V A L U E S;
K_WHEN : W H E N;
K_WHERE : W H E R E;
//Preference Keywords
K_AROUND : A R O U N D;
K_DISFAVOUR : D I S F A V O U R;
K_FAVOUR : F A V O U R;
K_HIGH : H I G H;
K_LOW : L O W;
K_OTHERS : O T H E R S;
K_PREFERENCE : P R E F E R E N C E;
K_WEIGHTED : W E I G H T E D;



 /*
 * Lexer Rules
 */

IDENTIFIER
 : '"' (~'"' | '""')* '"'
 | '`' (~'`' | '``')* '`'
 | '[' ~']'* ']'
 | [a-zA-Z_] [a-zA-Z_0-9]* // TODO check: needs more chars in set
 ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;


STRING_LITERAL
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

MULTILINE_COMMENT
 : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR : . ;

 
fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
