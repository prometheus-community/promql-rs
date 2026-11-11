%x BRACES SERIES HIST BUCKETS

%%

[ \t\n\r]+ ;
[a-zA-Z_][a-zA-Z_0-9]*:[a-zA-Z_0-9:]* <SERIES>"METRIC_IDENTIFIER"
[a-zA-Z_][a-zA-Z_0-9:]* <SERIES>"IDENT"
\{ <BRACES>"LBRACE"

<BRACES>[ \t\n\r]+ ;
<BRACES>[a-zA-Z_][a-zA-Z_0-9]* "IDENT_LABEL"
<BRACES>("([^"\\\n]|\\.)*"|'([^'\\\n]|\\.)*'|`[^`]*`) "STRING"
<BRACES>= "EQ"
<BRACES>, "COMMA"
<BRACES>\} <SERIES>"RBRACE"

<SERIES>[ \t]+ "SPACE"
<SERIES>\{ <BRACES>"LBRACE_AFTER_NAME"
<SERIES>x "TIMES"
<SERIES>_ "BLANK"
<SERIES>stale "STALE"
<SERIES>([0-9]+\.[0-9]+([eE][+\-]?[0-9]+)?|[0-9]+([eE][+\-]?[0-9]+)?|\.[0-9]+([eE][+\-]?[0-9]+)?|[Nn][Aa][Nn]|[Ii][Nn][Ff]) "NUMBER"
<SERIES>\+ "ADD"
<SERIES>- "SUB"
<SERIES>\{\{ <HIST>"OPEN_HIST"

<HIST>[ \t]+ "HIST_SPACE"
<HIST>: "COLON"
<HIST>- "HIST_SUB"
<HIST>(?i:schema) "SCHEMA_DESC"
<HIST>(?i:sum) "SUM_DESC"
<HIST>(?i:count) "COUNT_DESC"
<HIST>(?i:z_bucket) "ZERO_BUCKET_DESC"
<HIST>(?i:z_bucket_w) "ZERO_BUCKET_WIDTH_DESC"
<HIST>(?i:custom_values) "CUSTOM_VALUES_DESC"
<HIST>(?i:buckets) "BUCKETS_DESC"
<HIST>(?i:offset) "OFFSET_DESC"
<HIST>(?i:n_buckets) "NEGATIVE_BUCKETS_DESC"
<HIST>(?i:n_offset) "NEGATIVE_OFFSET_DESC"
<HIST>(?i:counter_reset_hint) "COUNTER_RESET_HINT_DESC"
<HIST>(?i:unknown) "UNKNOWN_COUNTER_RESET"
<HIST>(?i:reset) "COUNTER_RESET"
<HIST>(?i:not_reset) "NOT_COUNTER_RESET"
<HIST>(?i:gauge) "GAUGE_TYPE"
<HIST>([0-9]+\.[0-9]+([eE][+\-]?[0-9]+)?|[0-9]+([eE][+\-]?[0-9]+)?|\.[0-9]+([eE][+\-]?[0-9]+)?|[Nn][Aa][Nn]|[Ii][Nn][Ff]) "HIST_NUMBER"
<HIST>\[ <BUCKETS>"LBRACKET"
<HIST>\}\} <SERIES>"CLOSE_HIST"

<BUCKETS>[ \t]+ "BUCKETS_SPACE"
<BUCKETS>- "BUCKETS_SUB"
<BUCKETS>([0-9]+\.[0-9]+([eE][+\-]?[0-9]+)?|[0-9]+([eE][+\-]?[0-9]+)?|\.[0-9]+([eE][+\-]?[0-9]+)?|[Nn][Aa][Nn]|[Ii][Nn][Ff]) "BUCKETS_NUMBER"
<BUCKETS>\] <HIST>"RBRACKET"
