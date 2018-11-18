myCP=".:../lib/*:../dist/*"
myOPTS="-Dprop=$1"

java -cp $myCP $myOPTS jTPCC
