sum=0
for file in *.csv0*; do
	a=$(wc -l $file | awk '{print $1}') 
	echo " ====== "
	echo $a
	sum=$((sum+a))
	echo $sum
done
