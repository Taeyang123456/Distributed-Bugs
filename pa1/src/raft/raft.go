package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "math/rand"
import "time"
//import "fmt"

// import "bytes"
// import "encoding/gob"

type serverState int32

const (
	LEADER serverState = 0
	CANDIDATE serverState = 1
	FOLLOWER serverState = 2
)

type ElectionTime int64

const (
	TimeSlotMax = 400
	TimeSlotMin = 250
	HeartBeatTime = 100
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// LogEntry hods information about each log entry
type LogEntry struct {
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	//
	// persistent state on all servers
	CurrentTerm int
	VotedFor int
	Logs []LogEntry
	
	//
	// volatile state on servers
	CommitIndex int
	LastApplied int
	
	//
	// volatile state on leader
	NextIndex []int
	MatchIndex []int

	ApplyCh chan ApplyMsg

	//
	// timer for counting timeout
	Timer *time.Timer
	ServerState serverState
	VotesCount int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.CurrentTerm
	isleader = (rf.ServerState == LEADER)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// define AppendEntries struct
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	//follow dont know why
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//
	// case 1 : args.Term < rf.CurrentTerm
	// reject 
	if args.Term < rf.CurrentTerm {
		//fmt.Printf("%d in case 1\n", rf.me)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	//
	// is up-to-date determine
	isUpToDate := false

	if len(rf.Logs) == 0 {
		isUpToDate = true
	} else if rf.Logs[len(rf.Logs) - 1].Term < args.LastLogTerm {
		isUpToDate = true
	} else if rf.Logs[len(rf.Logs) - 1].Term == args.LastLogTerm {
		if args.LastLogIndex <  len(rf.Logs) {
			isUpToDate = false
		} else {
			isUpToDate = true
		}
	} else {
		isUpToDate = true
	}

	//
	// case 2 : args.Term == rf.CurrentTerm
	// 
	if args.Term == rf.CurrentTerm {
		//fmt.Printf("%d in case 2\n", rf.me)
		
		if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
		} else {
			if isUpToDate {
				rf.VotedFor = args.CandidateId
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = true
				rf.initTimer()
			} else {
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
			}
		}
		//fmt.Printf("%d with VotedFor = %d Term = %d  case 2\n", rf.me, rf.VotedFor, rf.CurrentTerm)
		return
	}

	//
	// case 3 : args.Term > rf.CurrentTerm
	//
	if args.Term > rf.CurrentTerm {
		//fmt.Printf("%d in case 3\n", rf.me)
		rf.ServerState = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1

		if isUpToDate {
			rf.VotedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.initTimer()
		} else {
			reply.Term = args.Term
			reply.VoteGranted = false
		}
		//fmt.Printf("%d with VotedFor = %d Term = %d  case 3\n", rf.me, rf.VotedFor, rf.CurrentTerm)
		
		return
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if rf.ServerState == LEADER {
		var newLog LogEntry
		newLog.Command = command
		newLog.Term = rf.CurrentTerm
		rf.Logs = append(rf.Logs, newLog)

		index = len(rf.Logs)
		term = rf.CurrentTerm
		isLeader = true
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {


	if args.Term < rf.CurrentTerm {
		//
		// args.Term < rf.CurrentTerm
		// appendEntries out of date 
		//fmt.Printf("assert\n")
		reply.Term = rf.CurrentTerm
		reply.Success = false
		//rf.initTimer()
		return
	} else {
		if rf.ServerState == LEADER {
			//fmt.Printf("%d is a LEADER but turn to a FOLLOWER\n", rf.me)
		}
		rf.ServerState = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1

		//fmt.Printf("%d recieve AppendEntries from %d with Term = %d\n", rf.me, args.LeaderId, rf.CurrentTerm)

		reply.Term = args.Term
		reply.Success = true

		rf.initTimer()
		return
	}

}


func (rf *Raft) sendAppendEntries() {

	for peerIndex := 0 ; peerIndex < len(rf.peers); peerIndex++ {
		if peerIndex != rf.me {

			var args AppendEntriesArgs
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			// 
			// appendEntries payload
			//
			/*if rf.NextIndex[i] > 0 {
				args.PrevLogIndex = rf.NextIndex[i] - 1
				args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
			} else {
				args.
			}*/
			// ATTENTION: need to be modify in ex2
			go func(peerIndex int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.peers[peerIndex].Call("Raft.AppendEntries", args, &reply)
			}(peerIndex, args)
			//fmt.Printf("%d sendAppendEntries to %d with Term = %d\n", rf.me, peerIndex, args.Term)
		}
	}
}

func (rf *Raft) initLeader() {
	rf.ServerState = LEADER
	for peerIndex := 0; peerIndex < len(rf.peers); peerIndex++ {
		if peerIndex != rf.me {
			rf.NextIndex[peerIndex] = len(rf.Logs)
			rf.MatchIndex[peerIndex] = 0;
		}
	}
	//fmt.Printf("LEADER %d broadcast an initial heartbeat\n", rf.me)
	rf.sendAppendEntries()
	//fmt.Printf("hhhhhhhhhhhhhh\n")
	rf.initTimer();
}

func (rf* Raft) requestVoteHandler(reply RequestVoteReply) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.CurrentTerm {
		// 
		// case 1 :
		// reply.Term < rf.CurrentTerm
		// reply expired
		//
		return
	} else if reply.Term == rf.CurrentTerm && reply.VoteGranted && rf.ServerState == CANDIDATE{
		//
		// case 2 :
		// reply.Term == rf.CurrentTerm
		// reply vote for rf
		// 
		rf.VotesCount += 1;
		if rf.VotesCount >= (len(rf.peers) / 2 + 1) {
			rf.ServerState = LEADER
			//fmt.Printf("%d get %d votes and become a LEADER\n", rf.me, rf.VotesCount)
			rf.initLeader()
		}
	} else if reply.Term > rf.CurrentTerm {
		//
		// case 3 :
		// reply.Term > rf.CurrentTerm
		// rf is out of date, turn to FOLLOWER
		//
		//fmt.Printf("%d turn to a FOLLOWER\n", rf.me)
		rf.CurrentTerm = reply.Term
		rf.ServerState = FOLLOWER
		rf.VotedFor = -1
		rf.VotesCount = 0
		rf.initTimer()
	}
}


func (rf *Raft) timeOutHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.ServerState == LEADER {
		//fmt.Printf("LEADER %d broadcast a heartbeat with Term = %d\n", rf.me, rf.CurrentTerm)
		rf.sendAppendEntries()
		rf.initTimer()
	} else {
		//
		// Election Timeout
		// begin an election and transitions to CANDIDATE
		rf.ServerState = CANDIDATE
		rf.CurrentTerm += 1
		rf.VotedFor = rf.me
		rf.VotesCount = 1

		//fmt.Printf("%d timeout and become CANDIDATE with Term = %d\n", rf.me,  rf.CurrentTerm)

		var args RequestVoteArgs
		args.Term = rf.CurrentTerm
		args.CandidateId = rf.me
		if len(rf.Logs) > 0 {
			args.LastLogIndex = len(rf.Logs) - 1
			args.LastLogTerm = rf.Logs[len(rf.Logs) - 1].Term
		} else {
			args.LastLogIndex = -1
			// ...BUGS
			args.LastLogTerm = -1
		}
		for peerIndex := 0; peerIndex < len(rf.peers) ; peerIndex++ {
			if(peerIndex != rf.me) {
				go func(peerIndex int, args RequestVoteArgs) {
					var reply RequestVoteReply
					execState := rf.sendRequestVote(peerIndex, args, &reply)
					//execState := rf.peers[peerIndex].Call("Raft.RequestVote", args, &reply)
					if execState {
						rf.requestVoteHandler(reply)
					}
				}(peerIndex, args)
			}
		}
		rf.initTimer()
	}
}


func (rf *Raft) initTimer() {

	//
	// rf is a LEADER
	//
	if rf.ServerState == LEADER {
		//fmt.Printf("%d is a %d with Hearbeat Milli [%v]\n", rf.me, rf.ServerState, time.Duration(HeartBeatTime) * time.Millisecond)
		if rf.Timer == nil {
			rf.Timer = time.NewTimer(time.Duration(HeartBeatTime) * time.Millisecond)
			go func() {
				for {
					<-rf.Timer.C
					rf.timeOutHandler()
				}
			}()
		}
		rf.Timer.Reset(time.Duration(HeartBeatTime) * time.Millisecond)
		return
	}
	
	//
	// rf is FOLLOWER or CANDIDATE
	//
	rand.Seed(time.Now().UnixNano())
	waitTime := rand.Int63n(TimeSlotMax - TimeSlotMin) + TimeSlotMin
	//fmt.Printf("%d is a %d Term = %d with Milli [%v]\n", rf.me, rf.ServerState, rf.CurrentTerm, time.Duration(waitTime) * time.Millisecond)
	if rf.Timer == nil {		
		rf.Timer = time.NewTimer(time.Duration(waitTime) * time.Millisecond)
		go func() {
			for {
				<-rf.Timer.C
				rf.timeOutHandler()
			}
		}()
	}
	rf.Timer.Reset(time.Duration(waitTime) * time.Millisecond)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.CurrentTerm = 0
	rf.VotedFor = -1

	rf.Logs = make([]LogEntry, 0)
	rf.CommitIndex = -1
	rf.LastApplied = -1


	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.ApplyCh = applyCh

	rf.VotesCount = 0
	rf.ServerState = FOLLOWER


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.initTimer()

	return rf
}
