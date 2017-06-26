#!/usr/bin/env bash


echo now deleting directory $1/$2
rm -rf $1/$2

echo now creating directory $1/$2
mkdir $1/$2