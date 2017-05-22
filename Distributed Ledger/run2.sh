
let N=$1-2

for i in `seq 0 $N`;
        do
        	let j=i+1
        	echo $i.log $j.log
            diff $i.log $j.log
            # diff -U 0 $i.log $j.log | grep ^@ | wc -l
            # diff -U 0 $i.io $j.io | grep ^@ | wc -l
        done 
