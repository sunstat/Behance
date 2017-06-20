#!/usr/bin/env bash
echo now replace ProjectView 
sed -i 's/ProjectView/V/g' ../action_data_trim.csv
echo now replace Appreciate
sed -i 's/Appreciate/A/g' ../action_data_trim.csv
echo now replace FollowUser
sed -i 's/FollowUser/F/g' ../action_data_trim.csv
echo now replace ProjectComment
sed -i 's/ProjectComment/C/g' ../action_data_trim.csv

 
