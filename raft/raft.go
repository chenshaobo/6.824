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
import (
	"github.com/chenshaobo/6.824/labrpc"
	"time"
	"math/rand"
	"encoding/gob"
	"bytes"
)

// import "bytes"
// import "encoding/gob"


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

const (
	FOLLOWER = iota
	CANDIDATES
	LEADER
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu               sync.Mutex
	peers            []*labrpc.ClientEnd
	persister        *Persister
	me               int // index into peers[]

                         // Your data here.
                         // Look at the paper's Figure 2 for a description of what
                         // state a Raft server must maintain.
	currentTerm      int //currentTermlatest term server has seen (initialized to 0on first boot, increases monotonically

	lastCommitIndex  int //
	lastAppliedIndex int

	logs             []Log

	nextIndex        []int
	matchIndex       []int

	applyCh          chan ApplyMsg
	voteCh           chan bool
	heartbeatCh      chan bool
	timeoutTimer     *time.Timer

	voteFor          int
	votes            int

	role             int
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.role == LEADER
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.logs)
}



func (rf *Raft) readSnapshot(data []byte) {

	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.lastCommitIndex = LastIncludedIndex
	rf.lastAppliedIndex = LastIncludedIndex

	rf.logs = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.logs)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	go func() {
		rf.applyCh <- msg
	}()
}

//RequestAppendEntries

type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PreLogIndex       int
	PreLogTerm        int
	Entries           []Log
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term      int
	NextIndex int
	Success   bool
}

func (rf *Raft) AppendEntriesRPC(args AppendEntriesArgs, reply *AppendEntriesReply) {

	//defer func(){
		if len(args.Entries) > 0 {
			DPrintf("[APPENDENTRIES] %v  receive from %v ,rf term:%v rf index :%v,%v,reply:%v,rf.logs:%v\n", rf.me,args.LeaderID, rf.currentTerm,rf.getLastLogIndex(),args,reply,rf.logs)
		}
	//}()

	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.currentTerm > args.Term {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.voteFor = -1
		rf.votes = 0
		rf.mu.Unlock()
	}
	rf.heartbeatCh <- true

	if args.PreLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}

	baseIndex := rf.logs[0].Index

	if args.PreLogIndex >= baseIndex {
		term := rf.logs[args.PreLogIndex - baseIndex].Term
		if term != args.PreLogTerm {
			for i := args.PreLogIndex - 1; i >= baseIndex; i-- {
				if rf.logs[i - baseIndex].Term != term {
					reply.NextIndex = i + 1
					break
				}
			}
			rf.logs = rf.logs[:reply.NextIndex - baseIndex]
			return
		} else {

			rf.logs = rf.logs[:args.PreLogIndex - baseIndex + 1]
			rf.logs = append(rf.logs, args.Entries...)
			reply.Success = true
			reply.NextIndex = rf.getLastLogIndex()

			if args.LeaderCommitIndex > rf.lastCommitIndex {
				last := rf.getLastLogIndex()
				if args.LeaderCommitIndex > last {
					rf.lastCommitIndex = last
				} else {
					rf.lastCommitIndex = args.LeaderCommitIndex
				}
				rf.commit()
			}
		}
	}

}

func (rf *Raft) sendAppendEntriesRPC(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := false
	if rf.role == LEADER {
		ok = rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)

	}
	if ok {
		if len(args.Entries) > 0 {
			DPrintf("[sendAppend] %v send %v to %v ,return %v\n", rf.me, args, server, *reply)
		}
		if rf.role != LEADER || args.Term != rf.currentTerm {
			return ok
		}
		//rf.mu.Lock()
		if reply.Term > args.Term {
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.voteFor = -1
			rf.votes = 0
			rf.heartbeatCh <-true
			return ok
		}



		if reply.Success {
			if len(args.Entries)>0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.updateCommit()
			}
		}else {
			rf.nextIndex[server] = reply.NextIndex
		}
		//rf.mu.Unlock()
	}
	return ok
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term    int
	VoteFor int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.


	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteFor = rf.voteFor
		return
	}
	DPrintf("[RequestVote] %v,%v receive vote :%v,args:%v,myterm:%v,lastindex:%v,last term:%v\n",
		time.Now().UnixNano(), rf.me, args.CandidateID, args, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.voteFor == -1) {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votes = 0
		rf.voteFor = -1
		//DPrintf("RF.VOTEFOR:%v\n",rf.voteFor)
		if (rf.getLastLogTerm() < args.LastLogTerm) ||
			(rf.getLastLogIndex() <= args.LastLogIndex && rf.getLastLogTerm() == args.LastLogTerm) {
			rf.voteFor = args.CandidateID
			reply.VoteFor = args.CandidateID
			//DPrintf("VOTEFOR:%v\n",reply.VoteFor)
		}
		rf.heartbeatCh <- true
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
	if ok {
		if reply.VoteFor == rf.me {
			rf.voteCh <- true
			return ok
		}

		if reply.Term > args.Term {
			rf.role = FOLLOWER
			rf.votes = 0
			rf.voteFor = -1
			return ok
		}
		DPrintf("[SendRequestVote] %v    %v sent request vote to %v,result votefor %v\n", time.Now().UnixNano(), rf.me, server, reply.VoteFor)
	}
	return ok
}

func (rf *Raft) voteMyself() {
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.votes = 1

	rva := RequestVoteArgs{
		CandidateID:rf.me,
		Term:rf.currentTerm,
		LastLogIndex:rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	for i := 0; i < len(rf.peers); i++ {
		//DPrintf("SEND REQUEST:%v to %v\n",rf.me,i)
		if i != rf.me {
			go func(index int, rvargs RequestVoteArgs) {
				rvp := &RequestVoteReply{}
				rf.sendRequestVote(index, rvargs, rvp)
			}(i, rva)
		}

	}
}


type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) GetPerisistSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.logs[0].Index
	lastIndex := rf.getLastLogIndex()

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []Log

	newLogEntries = append(newLogEntries, Log{Index: index, Term: rf.logs[index-baseIndex].Term})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.logs[i-baseIndex])
	}

	rf.logs = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].Index)
	e.Encode(newLogEntries[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []Log) []Log {

	var newLogEntries []Log
	newLogEntries = append(newLogEntries, Log{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for index := len(log) - 1; index >= 0; index-- {
		if log[index].Index == lastIncludedIndex && log[index].Term == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}

	return newLogEntries
}


func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs,reply *InstallSnapshotReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.heartbeatCh <- true
	rf.role = FOLLOWER
	rf.votes = 0
	rf.currentTerm = rf.currentTerm

	rf.persister.SaveSnapshot(args.Data)

	rf.logs = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.logs)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

	rf.lastAppliedIndex = args.LastIncludedIndex
	rf.lastCommitIndex = args.LastIncludedIndex

	rf.persist()
	go func() {
		rf.applyCh <- msg
	}()
}

func (rf *Raft) sendInstallSnapshot(server int,args InstallSnapshotArgs,reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.votes = 0
			rf.voteFor = -1
			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	return ok
}



/////

func (rf *Raft)getLastLogTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}
func (rf *Raft) getLastLogIndex() int {
	return rf.logs[len(rf.logs) - 1].Index
}

func (rf *Raft) updateCommit(){

	lastIndex := rf.getLastLogIndex()
	baseIndex := rf.logs[0].Index
	DPrintf("UPDATE COMMIT lastcommit:%v,last:%v\n",rf.lastCommitIndex,lastIndex)
	if lastIndex > rf.lastCommitIndex {
		N := rf.lastCommitIndex
		for i := rf.lastCommitIndex + 1; i <= lastIndex; i++ {
			nums := 1

			for j := range rf.peers {
				//DPrintf("%v match :%v\n",j,rf.matchIndex[j] )
				if rf.matchIndex[j] >= i {
					nums ++
				}
			}
			if nums * 2 > len(rf.peers) && rf.logs[i - baseIndex].Term == rf.currentTerm {
				N = i
			}
		}
		DPrintf("N = %v,lastApplied:%v,lastCommit:%v,lastIndex:%v\n",N,rf.lastAppliedIndex,rf.lastCommitIndex,lastIndex)
		if N > rf.lastCommitIndex {
			rf.mu.Lock()
			rf.lastCommitIndex = N
			rf.mu.Unlock()
			rf.commit()
		}

	}
}
func (rf *Raft) broadcastEntries() {
	//1.commit
	//2.sendAppendEntries
	baseIndex := rf.logs[0].Index

	for i := 0; i < len(rf.peers); i++ {
		if( i != rf.me && rf.role == LEADER ){

			if rf.nextIndex[i] > baseIndex {
				preIndex := rf.nextIndex[i] - 1
				//DPrintf("%v send appednentries to :%v,:%v preindex:%v now:%v\n", rf.me, i, rf.nextIndex[i], preIndex, time.Now().UnixNano())
				preTerm := rf.logs[preIndex - baseIndex].Term

				aea :=
					AppendEntriesArgs{
						Term : rf.currentTerm,
						LeaderID:rf.me,
						PreLogIndex: preIndex,
						PreLogTerm:preTerm,
						Entries:rf.logs[preIndex + 1 - baseIndex:],
						LeaderCommitIndex: rf.lastCommitIndex,

					}

				go func(index int, aeargs AppendEntriesArgs) {
					aer := &AppendEntriesReply{}
					rf.sendAppendEntriesRPC(index, aeargs, aer)
				}(i, aea)
			}else{
				var args InstallSnapshotArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.logs[0].Index
				args.LastIncludedTerm = rf.logs[0].Term
				args.Data = rf.persister.snapshot
				go func(server int,args InstallSnapshotArgs) {
					reply := &InstallSnapshotReply{}
					rf.sendInstallSnapshot(server, args, reply)
				}(i,args)
			}
		}
	}
}

func (rf *Raft) commit() {
	commitIndex := rf.lastCommitIndex
	baseIndex := rf.logs[0].Index
	DPrintf("%v COMMIT \n",rf.me)
	for i := rf.lastAppliedIndex + 1; i <= commitIndex; i++ {
		msg := ApplyMsg{Index:i, Command:rf.logs[i - baseIndex].Command}
		rf.applyCh <- msg
		rf.lastAppliedIndex = i
	}
	DPrintf("%v COMMIT  %v,commitIndex:%v\n",rf.me,rf.lastAppliedIndex,rf.lastCommitIndex)
}
func (rf *Raft) resetTimer() {
	var timeout time.Duration
	rand.Seed(time.Now().UnixNano())
	switch rf.role {
	case FOLLOWER:
		timeout = (time.Millisecond * time.Duration(50))
	case CANDIDATES:
		timeout = (time.Millisecond * time.Duration(500 + rand.Int63() % 333))
	case LEADER:
		timeout = (time.Millisecond * time.Duration(20))
	}

	rf.timeoutTimer.Reset(timeout)
	//r:=rf.timeoutTimer.Reset(timeout)
	//DPrintf("[RESET]%v reset timeout to %v,result:%v\n",rf.me,timeout,r)
}

func (rf *Raft) changeRole(role int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
	switch  rf.role {
	case FOLLOWER:
		rf.role = FOLLOWER
		rf.votes = 0
		rf.voteFor = -1
		rf.resetTimer()
	case CANDIDATES:
		rf.voteMyself()
		rf.resetTimer()
	case LEADER:
		rf.role = LEADER
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
		DPrintf("%v\n",rf.nextIndex)
		rf.resetTimer()
	}
}
func (rf *Raft) loop() {
	for {
		switch rf.role {
		case FOLLOWER:

				select {
				case <-rf.heartbeatCh:
				//DPrintf("[HEARBEAT] :%v Receive hearbeat:%v\n",rf.me,time.Now().UnixNano())
					rf.resetTimer()
				case timeout := <-rf.timeoutTimer.C:
					DPrintf("[FollowerTIMEOUT] :%v CHANGE TO CANDIDATES:%v\n", rf.me, timeout.UnixNano())
					rf.role = CANDIDATES
					rf.voteMyself()
					rf.resetTimer()
				case <-rf.voteCh:
				}
		case CANDIDATES:
				select {
				case <-rf.heartbeatCh:
					rf.changeRole(FOLLOWER)
				case timeout := <-rf.timeoutTimer.C:
					DPrintf("[CandidatesTIMEOUT] %v CHANGE TO CANDIDATES:%v\n", rf.me, timeout.UnixNano())
					rf.changeRole(CANDIDATES)
				case <-rf.voteCh:
					rf.votes++
					DPrintf("[Vote] %v Receive VOTE ,Votes :%v,len:%v\n", rf.me, rf.votes,len(rf.peers))
					if rf.votes * 2 > len(rf.peers) {
						DPrintf("[CHANGE ROLE] %v change to leader\n", rf.me)
						rf.changeRole(LEADER)
						rf.broadcastEntries()
					}
				}

		case LEADER:
				select {
				case <-rf.timeoutTimer.C:
					rf.broadcastEntries()
					rf.resetTimer()
				case <-rf.voteCh:
				}
		}
	}
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
	term := rf.currentTerm
	isLeader := rf.role == LEADER
	rf.mu.Lock()
	if isLeader {
		DPrintf("[command] %v Reveive command : %v\n", rf.me, command)
		index = rf.getLastLogIndex() + 1
		rf.logs = append(rf.logs, Log{Term:term, Index:index, Command:command})
		rf.persist()
	}
	rf.mu.Unlock()

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
	rf.currentTerm = 0
	rf.logs = append(rf.logs, Log{Term:0})
	rf.voteFor = -1
	rf.role = FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.heartbeatCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, len(peers) - 1)
	rf.timeoutTimer = time.NewTimer(time.Millisecond * time.Duration(100 + rand.Int63n(100)))
	rf.applyCh = applyCh

	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go rf.loop()

	// initialize from state persisted before a crash

	return rf
}
