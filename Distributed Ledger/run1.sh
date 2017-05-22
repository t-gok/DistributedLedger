echo "a" > a.log;echo "a" > a.io;echo "a" > a.glq
rm *.log;rm *.io;rm *.glq
go build
./dledger5


