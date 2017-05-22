import sys, random


NT = int(sys.argv[2])
N = int(sys.argv[1])

nt=0
while nt<NT:
	a = random.randint(1,3)
	a = min(a,NT-nt)
	n = range(N)
	random.shuffle(n)
	print a
	for i in range(a):
		print str(nt)+','+str(n[0])+','+str(n[1])+','+str(n[2])
		n = n[3:]
		nt+=1


	