# HBaseCoprocessor

A HBase region observer coprocessor to do secondary index and text index in elastic

ruby is for HBase shell

how to use in hbase shell:

add_global_index 'tablename','indexname','columnfamily:columnname:length|columnfamily:columnname:length|...'

delete_global_index 'tablename','indexname'

rebuild_global_index 'tablename','indexname'

add_fulltext_index 'tablename','indexname','columnfamily:columnname|columnfamily:columnname|...'

delete_fulltext_index 'tablename','indexname'

rebuild_fulltext_index 'tablename','indexname'
