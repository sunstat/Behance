#!/usr/bin/env bash


echo now deleting directory $1/$2
hdfs dfs -rm -r $1/$2

echo now creating directory $1/$2
hdfs dfs -mkdir $1/$2

