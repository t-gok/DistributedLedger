package main

import (
	"fmt"
	"time"
	"math/rand"
	"container/heap"
	"io/ioutil"
	"strings"
	"strconv"
	"os"
)

// 0: InitTrans 1: VoteRequest 2: VoteCommit 3: VoteAbbort 4: GlobalCommit 5: GlobalAbort 6: GetTXNStatus 7: SendTXNStatus


type msg struct {
    tid int
    src int
    typ int
    n1 int
    n2 int
    status int //0 - aborted 1 - commited
}


// 0: InitTrans 1: VoteRequest 2: VoteCommit 3: VoteAbbort 4: GlobalCommit 5: GlobalAbort 6:Print Queue 7: GetQStatus 8:SendQStatus
type gmsg struct {
    tid int
    src int
    typ int
    clock int
    cid int //co-ordinator id for the transaction
    GQ []txn
    GLQ []txn
}

type hbmsg struct{
	src int
}

type hback struct{
	src int
}

type txn struct{
	tid int
	clock int
	id int
}	


type PriorityQueue []txn

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	if(pq[i].clock < pq[j].clock){
		return true
	}else if (pq[i].clock == pq[j].clock && pq[i].id < pq[j].id){
		return true
	}else{
		return false
	}
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	//n := len(pq)
	item := x.(txn)
	//fmt.Println(item)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

var ch []chan msg
var gch []chan gmsg
var hch []chan hbmsg
var hach []chan hback
var st []int 
var gst []int
var N int
var isAlive []bool
var totalfailure int
var totalabort int
var done chan bool 
var view [][]bool

// var locks []sync.Mutex{}

func InitTrans(tid int, id int, n1 int, n2 int){
	m1 := msg{tid: tid, src: id, typ: 1,n1:n1, n2:id}
	ch[n1] <- m1
	m2 := msg{tid: tid, src: id, typ: 1,n1:id, n2:n2}
	ch[n2] <- m2
}


func sendGlobalCommit(tid int, id int, n1 int, n2 int){
	m1 := msg{tid: tid, src: id, typ: 4}
	ch[n1] <- m1
	m2 := msg{tid: tid, src: id, typ: 4}
	ch[n2] <- m2
}

func sendGlobalAbort(tid int, id int, n1 int, n2 int){
	m1 := msg{tid: tid, src: id, typ: 5}
	ch[n1] <- m1
	m2 := msg{tid: tid, src: id, typ: 5}
	ch[n2] <- m2
}

func SendQStatus(src int, GQ []txn, GLQ []txn){
	var m gmsg
	m.typ = 8
	for i:=0;i<len(GQ);i++{
		var t txn
		t.tid = GQ[i].tid
		t.id = GQ[i].id
		t.clock = GQ[i].clock
		m.GQ = append(m.GQ,t)
	}
	for i:=0;i<len(GLQ);i++{
		var t txn
		t.tid = GLQ[i].tid
		t.id = GLQ[i].id
		t.clock = GLQ[i].clock
		m.GLQ = append(m.GLQ,t)
	}
	gch[src] <- m	
}


func HB(id int){
	for{
		m := <- hch[id]
		if isAlive[id]{
			hach[m.src] <- hback{src:id}
		}
	}	
}


func updateView(id int) {

	for i:=0; i<N; i++{
		view[id][i] = false
		if i!=id{
			hch[i] <- hbmsg{src:id}
		}
	}
	vc := 0
	for{
		select{
			case m := <- hach[id]:
				view[id][m.src] = true
				vc = vc + 1	
			case <-time.After(5*time.Millisecond):
				if vc < N-1{
					fmt.Println("Update View ",id," ",vc)
				}
				goto hbout
		}
	}
	hbout:
}

func GSync(id int, c chan gmsg){

	GLQ:= make(PriorityQueue,0)
	var GQ[] txn
	var IO[] txn

	vc := 0
	tid:= -1
	acks:= 0
	cid:= -1

	for{
		failed := false
		for ;!isAlive[id];{
			time.Sleep(10*time.Millisecond)
			failed = true
		}

		//reccovery mode //
		//randomly choose a node and get the information//
		if failed{
			failed = false
			found:= false
			k := 1
			for ;!found; k = (k + 1){
				m1 := gmsg{typ:7,src:id}
				gch[(id+k)%N] <-m1

				for{
					select{
					case m:=<-gch[id]:
						if m.typ == 8{
							// update GQ//
							GQ = m.GQ
							GLQ = m.GLQ
							found = true
							goto Rend
						}else if m.typ == 0{
							add := true
							for i:=0; i<len(GQ); i++{
								if m.tid == GQ[i].tid{
									add = false
								}
							}
							if add{
								var t txn
								t.tid = m.tid
								t.clock = m.clock
								t.id = m.cid
								heap.Push(&GLQ,t)
							}	
						}
					case <-time.After(10*time.Millisecond):
						goto outFor
					}
				}
				outFor:
			}
			Rend:
			// remove duplicates
			gst[id] = 100
		}

		if(gst[id] == 100){
			select{
			case m:= <-gch[id]:
				if(m.typ == 0){
					// new transction
					// put the new transaction in the GLQ // insertion sort
					var t txn
					t.tid = m.tid
					t.clock = m.clock
					t.id = m.cid //co-ordinator id
					heap.Push(&GLQ,t)
					IO = append(IO,t)
				}else if(m.typ == 1){
					tid = m.tid
					cid = m.src
					gst[id] = 4
				}else if m.typ==6{
					f, err := os.Create(strconv.Itoa(id)+".log")
					check(err)
					for i:=0; i<len(GQ); i++{
						f.WriteString(strconv.Itoa(GQ[i].tid))
						f.WriteString(" ")
						f.WriteString(strconv.Itoa(GQ[i].clock))
						f.WriteString(" ")						
						f.WriteString(strconv.Itoa(GQ[i].id))
						f.WriteString("\n")
					}
					f.Close()

					f, err = os.Create(strconv.Itoa(id)+".io")
					check(err)
					for i:=0; i<len(IO); i++{
						f.WriteString(strconv.Itoa(IO[i].tid))
						f.WriteString(" ")
						f.WriteString(strconv.Itoa(IO[i].clock))
						f.WriteString(" ")						
						f.WriteString(strconv.Itoa(IO[i].id))
						f.WriteString("\n")
					}
					f.Close()

					f, err = os.Create(strconv.Itoa(id)+".glq")
					check(err)
					for i:=0; i<len(GLQ); i++{
						f.WriteString(strconv.Itoa(GLQ[i].tid))
						f.WriteString(" ")
						f.WriteString(strconv.Itoa(GLQ[i].clock))
						f.WriteString(" ")						
						f.WriteString(strconv.Itoa(GLQ[i].id))
						f.WriteString("\n")
					}
					f.Close()
					done <- true


				}else if(m.typ == 7){
					SendQStatus(m.src,GQ,GLQ)
				}
			case <-time.After(10*time.Millisecond):
				//check if you are at the top
				if(GLQ.Len()!=0){
					if(GLQ[0].id == id){
						// become the initiator // send vote request to alive in your view
						tid = GLQ[0].tid
						acks = 0
						cid = id //myself
						vc = 0

						updateView(id)

						for i := 0; i < N; i++ {
							// if id!=i{
							if view[id][i] && id!=i{
							// if isAlive[i] && id!=i{
								var m1 gmsg
								m1.src = id
								m1.typ = 1
								m1.tid = GLQ[0].tid
								gch[i] <- m1						
								vc = vc+1
							}
						}

						gst[id] = 1
					}
				} 
			}
			//check of 
		}else if(gst[id] == 1){
			select{
			case m:=<-gch[id]:
				if(m.typ == 0){
					var t txn
					t.tid = m.tid
					t.clock = m.clock
					t.id = m.cid
					heap.Push(&GLQ,t)
					IO = append(IO,t)
				}else if(m.typ == 2 && m.tid == tid){
					//vote commit
					acks = acks + 1
					// if(acks == N-1){ 
					if(acks == vc){ // get only from alive nodes according to my view
						gst[id] = 3
					}
				}else if(m.typ == 3 && m.tid == tid){
					gst[id] = 4
				}else if(m.typ == 7){
					SendQStatus(m.src,GQ,GLQ)
				}
			case <-time.After(10*time.Millisecond):
				gst[id] = 100
			}
		}else if(gst[id] == 2){
			//send GAbort to everyone
			for i := 0; i < N; i++ {
				var m1 gmsg
				m1.src = id
				m1.typ = 5
				m1.tid = tid
				if(i != id){
				gch[i] <- m1						
				}
			}
			gst[id] = 100

		}else if(gst[id] == 3){
			// send GCommit to everybody
			for i := 0; i < N; i++ {
				var m1 gmsg
				m1.src = id
				m1.typ = 4
				m1.tid = tid
				if(i != id){
				gch[i] <- m1						
				}
			}
			//find item in GLQ
			index := 0
			for i := 0; i < GLQ.Len(); i++ {
				if(GLQ[i].tid == tid){
					index = i
					break
				}
			}

			var t txn
			t.tid = tid
			t.clock = GLQ[index].clock
			t.id = GLQ[index].id

			GQ = append(GQ,t)
			heap.Remove(&GLQ,index)
			tid = -1
			cid = -1
			acks = 0

			gst[id] = 100
		}else if(gst[id] == 4){
			// send vote commit or vote abort to cid
			if(GLQ.Len() !=0){
				if(GLQ[0].tid == tid){
					// send vote commit
					var m1 gmsg
					m1.typ = 2
					m1.src = id
					m1.tid = tid
					gch[cid] <- m1
					gst[id] = 5
				}else{
					//send vote abort
					var m1 gmsg
					m1.typ = 3
					m1.src = id
					m1.tid = tid
					gch[cid] <- m1
					gst[id] = 6
				}
			}else{
				var m1 gmsg
				m1.typ = 3
				m1.src = id
				m1.tid = tid
				gch[cid] <- m1
				gst[id] = 6
			}


		}else if(gst[id] == 5){
			// waiting for vote commit
			select{
			case m:=<-gch[id]:
				if m.typ == 0{
					var t txn
					t.tid = m.tid
					t.clock = m.clock
					t.id = m.cid
					heap.Push(&GLQ,t)
					IO = append(IO,t)

				}else if m.typ==4 && m.tid==tid {
					gst[id] = 7
				}else if m.typ==5 && m.tid == tid{
					gst[id] = 6
				}else if(m.typ == 7){
					SendQStatus(m.src,GQ,GLQ)
				}
			case <-time.After(10*time.Millisecond):
				gst[id] = 100
			}
		}else if(gst[id] == 6){
			gst[id] = 100
			tid = -1
			acks = 0
			cid = -1
		}else if(gst[id] == 7){
			index:= 0
			for i := 0; i < GLQ.Len(); i++ {
				if(GLQ[i].tid == tid){
					index = i
					break
				}
			}

			var t txn
			t.tid = tid
			t.clock = GLQ[index].clock
			t.id = GLQ[index].id
			GQ = append(GQ,t)
			heap.Remove(&GLQ,index)

			gst[id] = 100
			tid = -1
			acks = 0
			cid = -1
		}
	}
}

func clearChannel(c chan msg){
	n:= len(c)
	for i := 0; i < n; i++ {
		<-c
	}
}

func clearChannel2(c chan hbmsg){
	n:= len(c)
	for i := 0; i < n; i++ {
		<-c
	}
}


func SendTXNStatus(tid int, LQ []int, src int){
	found := 0
	for i:=0;i<len(LQ);i++{
		if(LQ[i] == tid){
			found = 1
			break
		}
	}
	m:= msg{typ:7, status:found}
	ch[src] <- m
}				

// FSM 1 //
func node(id int, c chan msg) {
	var LQ[] int
	clock:= 0 
	tid := -1
	acks := 0
	n1 := -1
	n2 := -1
	src:= -1

	for {
		clock = clock + 1
		pfail:= rand.Float64()
		// if(pfail < 0){
		if(pfail < 0.001){
			totalfailure = totalfailure + 1
			fmt.Println("Failed node ",id)
			isAlive[id] = false
			time.Sleep(1000 * time.Millisecond)
			clearChannel(c)
			clearChannel2(hch[id])
			isAlive[id] = true

			//Initiate recovery//
			if st[id] == 1 || st[id] == 2 || st[id] == 3 || st[id] == 4{
				st[id] = 100

			}else if st[id] == 5{
				// Now you do not know what happened with the transaction //
				m:= msg{typ:6,tid:tid,src:id}
				ch[n1] <- m
				// we have to wait for type 7 message
				found:= false
				for{
					select{
					case m:=<-c:
						if m.typ == 7{
							if m.status == 1{
								st[id] = 7
							}else{
								st[id] = 6
							}
							found = true
							break
						}
					case <-time.After(10*time.Millisecond):
						break
					}
				}

				if !found{
					ch[n2] <- m
					// we have to wait for type 7 message
					for{
						select{
						case m:=<-c:
							if m.typ == 7{
								if m.status == 1{
									st[id] = 7
								}else{
									st[id] = 6
								}
								found = true
								break
							}
						case <-time.After(10*time.Millisecond):
							break
						}
					}	
				}
				if !found{
					os.Exit(1)
				}
			}
		// recovery complete //
		}

		if st[id]==100{
			m := <-c
			if m.typ == 0{
				tid = m.tid
				n1 = m.n1
				n2 = m.n2
				acks = 0

				st[id] = 0
			}else if m.typ==1{
				tid = m.tid
				n1 = m.n1
				n2 = m.n2
				src = m.src
				st[id] = 4
			}else if(m.typ == 6){
				SendTXNStatus(m.tid,LQ,m.src)
			} 
		}else if st[id]==0{
			// INIT STATE //
			InitTrans(tid,id,n1,n2)
			st[id] = 1
		}else if st[id]==1{
			// WAIT STATE //
			select{
			case m:=<-ch[id]:
				if m.typ == 2 && m.tid==tid{
					// VC //
					acks = acks + 1
					if acks==2{
						st[id] = 3
					}
				}else if m.typ==3 && m.tid==tid{
					//VA//
					st[id] = 2
				}else if(m.typ == 6){
					SendTXNStatus(m.tid,LQ,m.src)
				}	
			case <-time.After(10*time.Millisecond):
				st[id] = 100
			}
		}else if st[id]==2{
			// GA //
			sendGlobalAbort(tid,id,n1,n2)
			totalabort = totalabort+1
			st[id] = 100
		}else if st[id]==3{
			//GC//
			sendGlobalCommit(tid,id,n1,n2)

			// also send txn to evrybody including yourself //
			for i := 0; i < N; i++ {
				var m1 gmsg
				m1.typ = 0
				m1.tid = tid
				m1.src = id
				m1.cid = id
				m1.clock = clock

				gch[i] <- m1
			}
			st[id] = 100
			LQ = append(LQ,tid)
		}else if st[id]==4{
			p:= rand.Float64()
			if p<0.2{  // Abort
				m1 := msg{tid: tid, src:id, typ:3}
				ch[src] <- m1
				st[id] = 6
			} else{  // VC
				m1 := msg{tid: tid, src:id, typ:2}
				ch[src] <- m1
				st[id] = 5
			}
		}else if st[id]==5{
			select{
			case m:=<-ch[id]:
				if m.typ==4 && m.tid==tid {
					st[id] = 7
				}else if m.typ==5 && m.tid == tid{
					st[id] = 6
				}else if(m.typ == 6){
					SendTXNStatus(m.tid,LQ,m.src)
				}
			case <-time.After(10*time.Millisecond):
				st[id] = 100
			}
		}else if st[id]==6{
			st[id] = 100
		}else if st[id]==7{
			LQ = append(LQ,tid)
			st[id] = 100
		}
	}
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {

	// N = 200
	// var input string
	fmt.Print("Enter Number of Nodes: ")
	fmt.Scanln(&N)

	done = make(chan bool, 200)

	totalfailure = 0
	for i:=0; i<N; i++{
		c := make(chan msg, 100)
		ch = append(ch,c)
		st = append(st,100)
		go node(i,c)

		gc:= make(chan gmsg,100)
		gch = append(gch,gc)
		gst = append(gst,100)
		go GSync(i,gc)

		isAlive = append(isAlive,true)

		hb := make(chan hbmsg,2*N)
		hch = append(hch,hb)
		hbk := make(chan hback,2*N)
		hach = append(hach,hbk)
		v := []bool{}	
		for j:=0; j<N; j++{
			v = append(v,true)
		}
		view = append(view,v)
		go HB(i)
	}

	dat, err := ioutil.ReadFile("input.txt")
    check(err)
    arr := strings.Fields(string(dat))
 
    for i:=0; i<len(arr); i++{
		if strings.Contains(arr[i], ","){
			w := strings.FieldsFunc(arr[i], func(r rune) bool {
				switch r {
				case ',':
					return true
				}
				return false
			})
			tid, err := strconv.ParseInt(w[0], 10, 32)
			check(err)
			src, err := strconv.ParseInt(w[1], 10, 32)
			check(err)
			n1, err := strconv.ParseInt(w[2], 10, 32)
			check(err)
			n2, err := strconv.ParseInt(w[3], 10, 32)
			check(err)
			fmt.Println("TID: ",tid)
			m := msg{tid: int(tid), src: int(src), typ: 0, n1:int(n1), n2:int(n2)}
			ch[src] <- m
		} else {
			time.Sleep(100 * time.Millisecond)
		} 	
    }

	time.Sleep(5000 * time.Millisecond)

	fmt.Println("st",st)
	fmt.Println("gst",gst)
	fmt.Println("Write Logs")
	for i:=0; i<N; i++{
		gch[i] <- gmsg{typ: 6}
	}
	fmt.Println("Total Node Failure ",totalfailure)
	fmt.Println("Total Node Abort ",totalabort)

	for i:=0; i<N; i++{
		<- done
	}


}
