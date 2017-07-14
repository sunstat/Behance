#!/usr/bin/env bash


echo now deleting directory $1/$2
hadoop fs -rm -r $1/$2

echo now creating directory $1/$2
hadoop fs -mkdir $1/$2

