myCP=".:../lib/*:../dist/*"
myOPTS="-Dprop=$1"

java -cp ${myCP} $myOPTS LoadData $2 $3 $4 $5
