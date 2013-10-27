echo "{  \"updated\" : " >> $1
echo " , \"deleted\" : " >> $3
cat $1 $2 $3 $4 >> $5
echo "}" >> $5
rm $1 $2 $3 $4