#! /bin/bash

# make the database table
sqlplus marketbasket/welcome1 @transactions.sql

#run the OLH job
hadoop jar ${OLH_HOME}/jlib/oraloader.jar oracle.hadoop.loader.OraLoader -conf /mnt/shared/market_basket_example/olh/marketbasket_job.xml
