#!/usr/bin/env bash

: ${2?}

i_db=$1
i_dt=$2

l_db_backup=${i_db}_backup

declare -r c_backup_dir=/mnt/backup/`hostname -s`/daily/$i_dt/$i_db
declare -r c_clickhouse_dir=/var/opt/clickhouse

mv 2>/dev/null $c_backup_dir $c_backup_dir.bak.`date +%s`

        mkdir -p $c_backup_dir \
&&
        echo "drop database if exists $l_db_backup; create database $l_db_backup;" \
    | 
        clickhouse-client -mn \
&& 
        echo select name from tables where database = \'$i_db\' \
    | 
        clickhouse-client -mndsystem \
    | 
        xargs -rn1 echo show create table \
    | 
        sed -e's/$/;/' \
    |
        clickhouse-client -mnd$i_db \
    | 
        sed -Ee's/CREATE TABLE '$i_db'\./CREATE TABLE /;s/ReplicatedMergeTree\(\\'"'"'[^'"'"']+\\'"'"',\s*\\'"'"'[^'"'"']+\\'"'"',\s*/MergeTree(/;/\)/s/$/;/' \
    |
        clickhouse-client -mnd$l_db_backup \
&&
        echo select name from tables where database = \'$i_db\' \
    |
        clickhouse-client -mndsystem \
    |
        xargs -rn1 echo show create table \
    | 
        sed -e's/$/;/' \
    |
        clickhouse-client -mnd$i_db \
    |
        sed -Ee's/CREATE TABLE pladform\.([^[:space:]]+)\s.*ReplicatedMergeTree\('"\\\\'[^']+\\\\'"',\s*'"\\\\'[^']+\\\\'"',\s*([^[:space:]]+),.*$/\1\n\1\n\2/' \
    |
        xargs -r printf "insert into $l_db_backup.%s select "'*'" from $i_db.%s where %s = '$i_dt';\n" \
    |
        clickhouse-client -mn \
&&
        echo select table, partition from parts where database = \'$l_db_backup\' \
    |
        clickhouse-client -mndsystem \
    |
        xargs -r printf "alter table $l_db_backup.%s detach partition %s;\n" \
    |
        clickhouse-client -mn \
&&
    ( \
        cd $c_clickhouse_dir \
    ;
        cp -R --parents data/$l_db_backup/*/detached/* $c_backup_dir \
    ;
        cp -R --parents metadata/$i_db $c_backup_dir  \
    ;
        cp -R --parents metadata/$i_db.sql $c_backup_dir  \
    ;
        cp -R --parents metadata/$l_db_backup $c_backup_dir  \
    ;
        cp -R --parents metadata/$l_db_backup.sql $c_backup_dir  \
    ;
        mv $c_backup_dir/data/$l_db_backup $c_backup_dir/data/$i_db
    ) \
&&
        echo "drop database if exists $l_db_backup" \
    | 
        clickhouse-client -mn \
&&
    echo 'Success' \
||
    echo 'Error'
