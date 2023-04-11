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

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int // first index is 1
}

type AppendEntriesRPC struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesRPCReply struct {
	Term          int
	Success       bool
	Inconsistency bool
	XTerm         int
	XIndex        int
	XLen          int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	Mu        sync.Mutex          // Lock to protect shared access to this peer's state
	Peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	Me        int                 // this peer's index into peers[]
	Dead      int32               // set by Kill()
	State     string              // leader, follower, candidate

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm int
	VotedFor    int // candidate id that received vote in the current term
	Log         []LogEntry

	// volatile state on all servers
	CommitIndex int // index of highest log entry known to be committed initial = 0 incerase monotonically
	LastApplied int // index of highest log entry applied to state machine initial = 0 incerase monotonically

	// snapshot
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapshotSeg       []byte
	SnapshotTerm      int
	SnapshotIndex     int

	// volatile on leaders
	NextIndex  []int //NextIndex[serverID] = nextTerm
	MatchIndex []int

	// channels
	ApplyCh chan ApplyMsg

	// timeout field
	ExpirationTime time.Time
	// votes
	SumVotes int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	// var term int
	// var isleader bool
	// Your code here (2A).
	return rf.CurrentTerm, rf.State == "leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	fmt.Printf("%v's log len in persist is %v and my status is %v and persisterSize is %v \n-------\n", rf.Me, len(rf.Log), rf.State, rf.Persister.RaftStateSize())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil ||
		e.Encode(rf.LastIncludedIndex) != nil ||
		e.Encode(rf.LastIncludedTerm) != nil {
		fmt.Printf("Encode error in line 139! \n")
		return
	}
	raftstate := w.Bytes()
	rf.Persister.Save(raftstate, rf.SnapshotSeg) // write the snapshot to persistant state
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm, VotedFor, LastIncludedIndex, LastIncludedTerm int
	var Logs []LogEntry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Logs) != nil ||
		d.Decode(&LastIncludedIndex) != nil ||
		d.Decode(&LastIncludedTerm) != nil {
		fmt.Printf("error in decode in line 164 \n")
		return // TODO what should I do here?
	} else {
		rf.Log = Logs
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.LastIncludedIndex = LastIncludedIndex
		rf.LastIncludedTerm = LastIncludedTerm
	}
}

// ------------------------------- Install Snapshot ------------------------------------------------- //

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.Mu.Lock()
	// fmt.Printf("index is %v\n", index)
	baseIndex := rf.GetBaseIndex() // should be Last included index of snapshot
	lastIndex, _ := rf.GetLastIndexAndTerm()

	// if lastindex < baseIndex or input Index > LastIndex then we fucked up
	// this might not matter
	if index > lastIndex || baseIndex > lastIndex {
		fmt.Printf("you fucked up hard \n")
		rf.Mu.Unlock()
		return
	}

	if index <= baseIndex {
		fmt.Printf("%v got index %v less than base Index %v \n", rf.Me, index, baseIndex)
		rf.Mu.Unlock()
		return
	}

	// fmt.Printf("-----------\nsnapshot index %v, baseIndex %v, lastindex %v, log %+v\n------------\n", index, baseIndex, lastIndex, rf.Log)

	// modify log
	rf.TruncateLog(index, baseIndex)
	// after truncating the term, assign fields
	rf.SnapshotSeg = snapshot // persister.readSnapshot() state of persister is same as long as I don't write to it again
	rf.LastIncludedIndex = rf.Log[0].Index
	rf.LastIncludedTerm = rf.Log[0].Term
	rf.persist()
	rf.Mu.Unlock()

}

func (rf *Raft) GetLastIndexAndTerm() (index int, term int) {
	return rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term
}

func (rf *Raft) GetBaseIndex() int {
	return rf.Log[0].Index
}
func (rf *Raft) TruncateLog(index int, baseIndex int) {
	// helper function that snapshot calls
	// truncates log through and including that index
	oldLen := len(rf.Log)
	relPos := index - baseIndex
	logEntry := LogEntry{Command: "INIT", Index: index, Term: rf.Log[relPos].Term}
	newLog := []LogEntry{}
	newLog = append(newLog, logEntry)
	newLog = append(newLog, rf.Log[relPos+1:]...)
	rf.Log = newLog

	fmt.Printf("%v's log truncated! oldLengh was %v and now is %v \n", rf.Me, oldLen, len(rf.Log))
}

// make structs for RPC and return
type InstallSnapshotRPC struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotResponse struct {
	Term int
}

func (rf *Raft) MakeSnapshotArgs() (bool, InstallSnapshotRPC) {
	// called when lock is held
	if rf.State != "leader" {
		// fmt.Printf("trying to make sna pshot when I am no longer the leader")
		return false, InstallSnapshotRPC{}
	}
	args := InstallSnapshotRPC{}
	args.Term = rf.CurrentTerm
	args.LeaderID = rf.Me
	args.LastIncludedIndex = rf.LastIncludedIndex
	args.LastIncludedTerm = rf.LastIncludedTerm
	args.Data = rf.SnapshotSeg // TODO this might need to be changed. Potentially stale

	return true, args
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotRPC, reply *InstallSnapshotResponse) {
	ok := rf.Peers[server].Call("Raft.InstallSnapshot", args, reply)

	if !ok || rf.killed() || args.Term != rf.CurrentTerm || rf.State != "leader" {
		return
	}
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	if reply.Term > rf.CurrentTerm {
		rf.UpdateTerms(reply.Term)
	}
	// TODO double check this else statement
	// Then we set nextindex to be lastincludedindex + 1
	rf.NextIndex[server] = rf.GetMax(args.LastIncludedIndex+1, rf.NextIndex[server])
	rf.MatchIndex[server] = rf.GetMax(args.LastIncludedIndex, rf.MatchIndex[server])
}

// InstallSnapshot handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotRPC, reply *InstallSnapshotResponse) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	defer rf.persist()

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		fmt.Printf("%v returned in pos 1 in installSnapshot\n", rf.Me)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.UpdateTerms(args.Term)
	}

	rf.SetElectionTimeout()
	reply.Term = rf.CurrentTerm

	// HOW IS THIS DISCARDED?

	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		fmt.Printf("%v returned in pos 2 in installSnapshot\n", rf.Me)
		return
	}

	// 4. Reply and wait for more data chunks if done is false
	// WHAT SHOULD I DO HERE?

	// 5. Save snapshot file, discard any existing or partial snapshot
	// with a smaller index
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.CommitIndex = args.LastIncludedIndex
	rf.SnapshotSeg = args.Data

	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply

	newLog := []LogEntry{}
	dummyEntry := LogEntry{Command: "INIT", Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}
	newLog = append(newLog, dummyEntry)
	baseIndex := rf.GetBaseIndex()

	// Just look at whether log at args.LastincludedLog's term and index match
	// if so, throw away everything before, toehrwise, throw away everything after
	// fmt.Printf("$$$ %v's log %v, lastIncludedIndex %v $$$ \n", rf.Me, rf.Log, args.LastIncludedIndex)
	if args.LastIncludedIndex-baseIndex <= len(rf.Log)-1 {
		// if it is in range. Else, we can immediately discard
		if rf.Log[args.LastIncludedIndex-baseIndex].Index == args.LastIncludedIndex && rf.Log[args.LastIncludedIndex-baseIndex].Term == args.LastIncludedTerm {
			newLog = append(newLog, rf.Log[args.LastIncludedIndex-baseIndex+1:]...)
			rf.Log = newLog
			return
		}
	}

	// 7. Discard the entire log
	rf.Log = newLog // No inconsistency, log now only contains dummy entry
	fmt.Printf("%v log is now %+v in installSnapshot\n", rf.Me, rf.Log)
	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
}

// ------------------------------- RequestVote ------------------------------------------------- //

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	// fmt.Printf("%v locked in line 207 interm %v \n", rf.Me, rf.CurrentTerm)
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	// defer rf.persist()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	fmt.Printf("%v got election request from %v in term %v\n", rf.Me, args.CandidateID, rf.CurrentTerm)

	if args.Term < rf.CurrentTerm {
		fmt.Printf("request vote im %v args term %v < current term %v\n", rf.Me, args.Term, rf.CurrentTerm)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.UpdateTerms(args.Term)
	}

	// grant vote if
	// votedFor is nul or candidateID, and
	// candidate's log is at least as up to date as receiver's log
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		// election restriction implementation
		// only grants vote unless candidate log is more up to date than my own
		// if logs have last entries with different terms, the log with later term is more up to date
		lastLogIndex, lastLogTerm := rf.GetLastIndexAndTerm()

		if rf.CanGrantVote(args.LastLogTerm, args.LastLogIndex, lastLogIndex, lastLogTerm) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateID
			fmt.Printf("%v granted vote to %v\n", rf.Me, args.CandidateID)
			// reset election timeout
			rf.persist()
			rf.SetElectionTimeout()
		}

	}
}
func (rf *Raft) CanGrantVote(ct int, ci int, li int, lt int) bool {
	return ct > lt || (ci >= li && ct == lt)
}

func (rf *Raft) PerpetualHeartBeat(term int) {
	// helpfer funtion that calls CallHeartBeat
	// sends heartbeat as long as current term is the same as input term and i am the leader
	// fmt.Printf("locked in perpetual heartbeat line 216 \n")
	rf.Mu.Lock()

	for term == rf.CurrentTerm && rf.State == "leader" && !rf.killed() {
		// fmt.Printf("unlocked in perpetual heartbeat line 220")
		rf.Mu.Unlock()

		rf.CallHeartBeat()

		// set time out and then sleep
		time.Sleep(100 * time.Millisecond)

		rf.Mu.Lock()

	}
	rf.Mu.Unlock()
}

func (rf *Raft) CallHeartBeat() {

	// leader sends heartbeat with empty fields as heartbeat messages
	rf.Mu.Lock()

	if rf.State != "leader" {
		rf.Mu.Unlock()
		return
	}

	state := rf.State
	rf.Mu.Unlock()

	for server := range rf.Peers {
		if rf.Me != server && state == "leader" {
			rf.SendAppendEntriesOrSnapshot(server)
		}
	}
}

func (rf *Raft) SendAppendEntriesOrSnapshot(server int) {
	rf.Mu.Lock()
	// callend when lock not held
	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	bolAppend, inputAppendEntries := rf.makeAppendEntriesArgs(server)
	replyAppendEntries := AppendEntriesRPCReply{}
	boolSnap, inputInstallSnapshots := rf.MakeSnapshotArgs()
	replyInstallSnapshots := InstallSnapshotResponse{}

	// fmt.Printf("%v is sending append entries to %v my nextIndex is %v and entries is %v \n", rf.Me, server, rf.NextIndex, input)
	rf.Mu.Unlock()
	if bolAppend {
		// fmt.Printf("sending append entries! \n")
		go rf.SendAppendEntries(server, &inputAppendEntries, &replyAppendEntries) // TODO trigger aggrement to be sent to log
	} else {
		if boolSnap {
			// fmt.Printf("sending install snapshots! \n")
			go rf.SendInstallSnapshot(server, &inputInstallSnapshots, &replyInstallSnapshots)
		} else {
			// fmt.Printf("you fucked up here 2 \n")
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {

	ok := rf.Peers[server].Call("Raft.RequestVote", args, reply)

	if !ok || rf.killed() {
		return
	}

	rf.Mu.Lock()

	// update term
	if reply.Term > rf.CurrentTerm {
		rf.UpdateTerms(reply.Term)
		rf.Mu.Unlock()
		return
	}

	if args.Term == rf.CurrentTerm && reply.VoteGranted && rf.State == "candidate" {

		rf.SumVotes += 1
		fmt.Printf("%v received one vote in term %v \n", rf.Me, rf.CurrentTerm)

		if rf.SumVotes > len(rf.Peers)/2 && args.Term <= rf.CurrentTerm && rf.State != "leader" {
			fmt.Printf("I am the leader %v in term %v \n", rf.Me, rf.CurrentTerm)
			// populate nextIndex and matchIndex
			lastLogIndex, _ := rf.GetLastIndexAndTerm()
			rf.NextIndex = []int{}
			rf.MatchIndex = []int{}
			for i := 0; i < len(rf.Peers); i++ {
				rf.MatchIndex = append(rf.MatchIndex, 0)
				rf.NextIndex = append(rf.NextIndex, lastLogIndex+1)
			}

			rf.State = "leader"
			term := rf.CurrentTerm
			rf.persist()
			rf.Mu.Unlock()
			go rf.PerpetualHeartBeat(term)
		} else {
			rf.Mu.Unlock()
		}
	} else {
		rf.Mu.Unlock()
	}
}

func (rf *Raft) sendLeaderElection(currentTerm int) {
	// function to periodically send requestVote rpcs when it hasnt heard from another peer for a while

	// TODO will deadlock occur with this locking scheme?
	// fmt.Printf("locked in leader election line 393 \n")

	rf.Mu.Lock()

	if rf.State != "candidate" || rf.killed() || currentTerm < rf.CurrentTerm || time.Now().After(rf.ExpirationTime) {
		// only candidates can send leader election notices
		rf.Mu.Unlock()
		return
	}

	rf.SumVotes = 1
	lastLogIndex, lastLogTerm := rf.GetLastIndexAndTerm()
	input := RequestVoteArgs{}
	input.CandidateID = rf.Me
	input.Term = rf.CurrentTerm
	input.LastLogIndex = lastLogIndex
	input.LastLogTerm = lastLogTerm
	rf.Mu.Unlock()

	for server := range rf.Peers {

		if rf.Me != server {
			reply := RequestVoteReply{}
			fmt.Printf("%v sending request Vote to %v in term %v \n", rf.Me, server, rf.CurrentTerm)
			go rf.sendRequestVote(server, &input, &reply)
		}
	}
}

// ------------------------------- AppendEntries ------------------------------------------------- //

func (rf *Raft) makeAppendEntriesArgs(server int) (bool, AppendEntriesRPC) {
	// helper function that fills in fields for appendEntries RPC all
	// returns false if base index greater than nextIndex
	// PrevLogIndex := rf.GetMax(rf.NextIndex[server]-1, 0)
	PrevLogIndex := rf.GetMax(rf.NextIndex[server]-1, 0)
	baseIndex := rf.GetBaseIndex()
	lastLogIndex, _ := rf.GetLastIndexAndTerm()

	// fmt.Printf("prevLogIdx %v basIndex %v lastLogIndex %v nextIndex %v. My Log is %v\n", PrevLogIndex, baseIndex, lastLogIndex, rf.NextIndex[server], rf.Log)

	if baseIndex > rf.NextIndex[server] {
		return false, AppendEntriesRPC{}
	}

	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex

	appendEntriesMsg := AppendEntriesRPC{}
	appendEntriesMsg.LeaderCommit = rf.CommitIndex
	appendEntriesMsg.LeaderID = rf.Me
	appendEntriesMsg.PrevLogIndex = PrevLogIndex
	appendEntriesMsg.Term = rf.CurrentTerm

	if appendEntriesMsg.PrevLogIndex >= baseIndex {
		appendEntriesMsg.PrevLogTerm = rf.Log[appendEntriesMsg.PrevLogIndex-baseIndex].Term
		// default is 0?
	}
	if lastLogIndex >= rf.NextIndex[server] { // Check this logic in OH
		entriesSeg := rf.Log[rf.NextIndex[server]-baseIndex:]
		entries := make([]LogEntry, len(entriesSeg))
		copy(entries, entriesSeg)
		appendEntriesMsg.Entries = entries
		// default is heartbeat, empty emtries
	}

	return true, appendEntriesMsg
}

// helper function that discovers inconsitencies in log entries
// returns index of inconsitency
// if no inconsistencies, returns -1

func (rf *Raft) DiscoverConflicts(myLogs []LogEntry, argsLogs []LogEntry) bool {

	for index, my_entry := range myLogs {
		if index < len(argsLogs)-1 {
			if my_entry.Term != argsLogs[index].Term {
				return true
			}
		}
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesRPC, reply *AppendEntriesRPCReply) {

	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	// resets election timeout so other servers don't step forward
	// as leader when one has already been elected
	rf.SetElectionTimeout()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	// case heartBeat
	if args.Term > rf.CurrentTerm {
		rf.UpdateTerms(args.Term)
	}
	// Initialize
	reply.Success = false
	reply.Term = rf.CurrentTerm
	reply.Inconsistency = false

	// optimization: fast rollback
	reply.XIndex = -1
	reply.XTerm = -1
	reply.XLen = -1

	lastLogIndex, _ := rf.GetLastIndexAndTerm()

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm {
		reply.Inconsistency = true
		return
	}

	if args.PrevLogIndex > lastLogIndex {
		// If the previous log index is greater, then I immediately set the retry index to be the last index

		reply.XLen = lastLogIndex + 1
		// fmt.Printf("%v in case 1.5. Term: %v, xlen %v, xInd %v, xTerm %v, inconsistency %v \n", rf.Me, rf.CurrentTerm, reply.XLen, reply.XIndex, reply.XTerm, reply.Inconsistency)
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

	baseIndex := rf.GetBaseIndex()
	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.Log[args.PrevLogIndex-baseIndex].Term { // there might be off by 1
		reply.XTerm = rf.Log[args.PrevLogIndex-baseIndex].Term

		// find the retry index
		for i := len(rf.Log) - 1; i >= 0; i-- {
			if rf.Log[args.PrevLogIndex-baseIndex].Term == rf.Log[i].Term {
				reply.XIndex = rf.Log[i].Index // DOUBLE TRIPLE CHECK
				// break
			}
		}
		return
	}

	// 3. if existing entry conflicts with new one (same index but different terms)
	// delete existing entry and all that follows it

	if args.PrevLogIndex >= baseIndex-1 {

		entriesToLookAt := rf.Log[args.PrevLogIndex-baseIndex+1:]

		iterLength := rf.GetMin(len(entriesToLookAt), len(args.Entries))

		// conflictIndex := -1
		// var conflictIndex int

		for i := 0; i < iterLength; i++ {

			if args.Entries[i].Term != entriesToLookAt[i].Term {
				// we want to clear the term inconsistency
				// we want to keep the log until our starting index plus i

				rf.Log = rf.Log[:args.PrevLogIndex-baseIndex+i+1]
				rf.persist()
				break
			}
		}

		// if conflict index is in my entries then i want to add everything after

		for i := 0; i < len(args.Entries); i++ {
			if len(rf.Log) == args.PrevLogIndex-baseIndex+i+1 {
				rf.Log = append(rf.Log, args.Entries[i])
				rf.persist()
			}
		}

	}

	reply.Success = true

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = rf.GetMin(args.LeaderCommit, lastLogIndex)
	}

}

func (rf *Raft) Contains(log []LogEntry, entry LogEntry) bool {
	for _, v := range log {
		if v == entry {
			return true
		}
	}
	return false
}

func (rf *Raft) GetMax(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) GetMin(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesRPC, reply *AppendEntriesRPCReply) {
	ok := rf.Peers[server].Call("Raft.AppendEntries", args, reply)

	rf.Mu.Lock()
	term := rf.CurrentTerm
	rf.Mu.Unlock()

	if !ok || rf.killed() || reply.Term < term || args.Term != term {
		return
	}

	// update term
	if reply.Term > term {
		rf.Mu.Lock()
		rf.UpdateTerms(reply.Term)
		rf.Mu.Unlock()
		return
	}

	//• If successful: update nextIndex and matchIndex for follower (§5.3)
	rf.Mu.Lock()
	// fmt.Printf("leader %v received follower respond in %v \n", rf.Me, server)
	if reply.Success {

		// check for stale entries
		if args.PrevLogIndex < rf.NextIndex[server]-1 {
			rf.Mu.Unlock()
			// fmt.Printf("returned in loc1 \n")
			return
		}

		// matchindex is for each server, index of highest log entry
		// known to be replicated on server
		// which means it is the length of the entries plus args' previndex

		rf.MatchIndex[server] = len(args.Entries) + args.PrevLogIndex
		rf.NextIndex[server] = rf.MatchIndex[server] + 1

		rf.UpdateCommitIdx()
		rf.Mu.Unlock()

	} else { // unsuccessful reply
		// optimization
		if !reply.Inconsistency {
			if reply.XLen == -1 {

				//   Case 1: leader doesn't have XTerm:
				xTermFound := false
				for i := 0; i < len(rf.Log); i++ {
					//   Case 2: leader has XTerm:
					if reply.XTerm == rf.Log[i].Term {
						rf.NextIndex[server] = i + 1
						xTermFound = true
						// break
					}
				}
				if !xTermFound {
					// case 1
					if reply.XIndex != -1 {
						rf.NextIndex[server] = reply.XIndex
					}
				}

			} else if reply.XLen != -1 && reply.XIndex == -1 && reply.XTerm == -1 {
				//   Case 3: follower's log is too short:
				rf.NextIndex[server] = reply.XLen
			}

			// • If AppendEntries fails because of log  inconsistency:
			// decrement nextIndex and immediately retry (§5.3)
			bol, input := rf.makeAppendEntriesArgs(server)
			reply := AppendEntriesRPCReply{}
			rf.Mu.Unlock()
			if bol {
				rf.Peers[server].Call("Raft.AppendEntries", &input, &reply)
			}
		} else {
			rf.Mu.Unlock()
		}
	}
}

func (rf *Raft) UpdateCommitIdx() {

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	baseIndex := rf.GetBaseIndex()
	lastLogIndex, _ := rf.GetLastIndexAndTerm()
	if rf.State == "leader" {
		for N := rf.CommitIndex + 1; N < lastLogIndex+1; N++ {
			if rf.Log[N-baseIndex].Term == rf.CurrentTerm {
				count := 1
				for server := range rf.Peers {
					if server != rf.Me && rf.MatchIndex[server] >= N {
						count += 1
					}
				}

				if count > len(rf.Peers)/2 {
					rf.CommitIndex = N
				}
			}
		}
	}
}

func (rf *Raft) UpdateTerms(newTerm int) {

	// helper function that updates current term, sets votedfor = -1, and sumvotes = -1
	// and resets to follower
	// used for rpc call to detect term inconsistency
	rf.CurrentTerm = newTerm
	rf.VotedFor = -1
	rf.SumVotes = 0
	rf.State = "follower"

	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// TODO need to initialize indices

	rf.Mu.Lock()
	isLeader := rf.State == "leader"
	lastLogIndex, _ := rf.GetLastIndexAndTerm()
	index := lastLogIndex + 1
	term := rf.CurrentTerm

	// append command to local log
	if isLeader {

		newEntry := LogEntry{}
		newEntry.Command = command
		newEntry.Index = index
		newEntry.Term = term
		rf.Log = append(rf.Log, newEntry)
		rf.persist()
		// fmt.Printf("Leader %v received new log entry %v in term %v and now log is %v\n", rf.Me, newEntry, rf.CurrentTerm, rf.Log)

		rf.Mu.Unlock()
		// leader sends out appendEntries
		for server := range rf.Peers {
			if server != rf.Me {
				rf.SendAppendEntriesOrSnapshot(server)
			}
		}
	} else {
		rf.Mu.Unlock()
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.Dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.Dead)
	return z == 1
}

func (rf *Raft) SetElectionTimeout() {
	// helper function that resets election timeout
	// should be called in ticker and requestVote

	min := 400
	max := 600
	random_timeout := rand.Intn(max-min) + min
	timeFrame := time.Duration(random_timeout) * time.Millisecond
	rf.ExpirationTime = time.Now().Add(timeFrame)
}

func (rf *Raft) UpdateLog() {
	// 2B
	// If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine (§5.3)

	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.Mu.Lock()

		if rf.GetBaseIndex() <= rf.LastApplied {
			for i := rf.LastApplied + 1; i <= rf.CommitIndex && (rf.GetBaseIndex() <= rf.LastApplied); i++ {
				rf.LastApplied++
				baseIndex := rf.GetBaseIndex()
				applyTerm := rf.Log[i-baseIndex]
				applyMsg := ApplyMsg{}

				applyMsg.CommandValid = true
				applyMsg.Command = applyTerm.Command
				applyMsg.CommandIndex = i
				applyMsg.SnapshotValid = false
				rf.Mu.Unlock()
				rf.ApplyCh <- applyMsg
				rf.Mu.Lock()
			}
		} else {
			applyMsg := ApplyMsg{}
			rf.LastApplied = rf.LastIncludedIndex
			applyMsg.SnapshotValid = true
			applyMsg.CommandValid = false
			applyMsg.Snapshot = rf.SnapshotSeg
			applyMsg.SnapshotTerm = rf.LastIncludedTerm
			applyMsg.SnapshotIndex = rf.LastIncludedIndex
			rf.Mu.Unlock()
			rf.ApplyCh <- applyMsg
			rf.Mu.Lock()
		}
		rf.Mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		time.Sleep(100 * time.Millisecond)
		// check if time of last heartbeat is within expiration time
		rf.Mu.Lock()
		expTime := rf.ExpirationTime
		state := rf.State
		rf.Mu.Unlock()

		if time.Now().After(expTime) && state != "leader" { // if timer has expired
			// fmt.Printf("%v in term %v in ticker \n", rf.Me, rf.CurrentTerm)
			rf.Mu.Lock()
			rf.State = "candidate"
			rf.CurrentTerm += 1
			rf.VotedFor = rf.Me
			rf.SumVotes = 1
			rf.persist()

			// reset election timeout
			rf.SetElectionTimeout()
			rf.Mu.Unlock()
			// fmt.Printf("%v sending election request in term %v \n", rf.Me, rf.CurrentTerm)
			rf.sendLeaderElection(rf.CurrentTerm)

		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.Peers = peers
	rf.Persister = persister
	rf.Me = me
	rf.Mu.Lock()

	// Your initialization code here (2A, 2B, 2C).
	// create backround gorountine that kick off leader election periodically
	// by sending out requestvote rpcs when it hasnt heard back in a while
	rf.CurrentTerm = 1
	rf.VotedFor = -1
	rf.Log = []LogEntry{}

	// make dummy log entry
	dummyEntry := LogEntry{}
	dummyEntry.Command = "INIT"
	dummyEntry.Index = 0
	dummyEntry.Term = 0
	rf.Log = append(rf.Log, dummyEntry)

	// snapShot
	rf.LastIncludedIndex = -1
	rf.LastIncludedTerm = -1

	// initialize rf fields
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = []int{}
	rf.MatchIndex = []int{}
	rf.State = "follower"
	rf.ExpirationTime = time.Now()
	rf.ApplyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// after a crash
	rf.SnapshotSeg = persister.ReadSnapshot()
	if len(rf.SnapshotSeg) != 0 {
		rf.CommitIndex = rf.LastIncludedIndex
	}

	rf.Mu.Unlock()

	// start ticker and log update goroutines to start elections
	go rf.ticker()
	go rf.UpdateLog()

	return rf
}
