myCP=".:../lib/*:../dist/*"
myOPTS="-Dprop=$1 -DcommandFile=$2"

java -cp $myCP $myOPTS ExecJDBC
