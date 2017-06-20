touch action_data.csv
for file in *.csv00*; do
	cat $file >> action_data.csv
done
