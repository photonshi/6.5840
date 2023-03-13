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
	Snapshotchunk     []byte // received from snapshot

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil {
		fmt.Printf("Encode error in line 139! \n")
		return
	}
	raftstate := w.Bytes()
	rf.Persister.Save(raftstate, rf.Snapshotchunk)

	// states that should be persisted include
	// currentTerm, votedFor, and rf.Log

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
	var CurrentTerm, VotedFor int
	var Logs []LogEntry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Logs) != nil {
		fmt.Printf("error in decode in line 164 \n")
		return // TODO what should I do here?
	} else {
		rf.Log = Logs
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.TruncateLog(index)
	rf.Snapshotchunk = snapshot
}

func (rf *Raft) TruncateLog(index int) {
	// helper function that snapshot calls
	// truncates log through and including that index

	// if index > rf.LastIncludedIndex {
	// 	fmt.Print("you've entered the twilight zone \n")
	// 	// non_stale entries
	// 	// rf.LastIncludedTerm = rf.Log[index-rf.LastIncludedIndex-1].Term
	// 	// rf.Log = rf.Log[index-rf.LastIncludedIndex:]
	// 	// rf.LastIncludedIndex = index
	// } else if index > len(rf.Log) {
	// 	fmt.Print("you've entered the twilight zone vol 2 \n")
	// } else {
	// lastIncluded index and term? QUESTION FOR OH
	fmt.Printf("%v's log before: %v, index is %v \n", rf.Me, rf.Log, index)
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = rf.Log[index].Term
	rf.Log = rf.Log[index+1:]
	fmt.Printf("%v's log after: %v, index is %v, llT is %v, llI is %v \n", rf.Me, rf.Log, index, rf.LastIncludedTerm, rf.LastIncludedIndex)
	// }

}

func (rf *Raft) LastLogTermAndIndex() (int, int) {
	// helper function that returns last log index and term
	// This is because now, item's position in log is no longer reflective of their actual index
	// mutex must be held
	var index, term int
	if len(rf.Log) > 0 {
		index, term = rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term
	} else {
		index, term = rf.LastIncludedIndex, rf.LastIncludedTerm
	}
	// index, term = rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term
	return index, term
}

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

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

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
		// lastLogIndex := len(rf.Log) - 1
		// lastLogTerm := rf.Log[lastLogIndex].Term

		lastLogIndex, lastLogTerm := rf.LastLogTermAndIndex()

		if rf.CanGrantVote(args.LastLogTerm, args.LastLogIndex, lastLogIndex, lastLogTerm) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateID

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
			rf.Mu.Lock()

			// If last log index ≥ nextIndex for a follower: send
			// AppendEntries RPC with log entries starting at nextIndex
			input := rf.makeAppendEntriesArgs(server)
			reply := AppendEntriesRPCReply{}

			// fmt.Printf("%v is sending append entries to %v my nextIndex is %v and entries is %v \n", rf.Me, server, rf.NextIndex, input)
			rf.Mu.Unlock()

			go rf.SendAppendEntries(server, &input, &reply) // TODO trigger aggrement to be sent to log
		}

	}
}

func (rf *Raft) makeAppendEntriesArgs(server int) AppendEntriesRPC {

	// helper function that fills in fields for appendEntries RPC all
	lastLogIndex, _ := rf.LastLogTermAndIndex()
	PrevLogIndex := rf.GetMax(rf.NextIndex[server]-1, 0)
	inputEntries := make([]LogEntry, len(entries))
	// if last log index >= indexIndex for a follower, senw entries starting at next Index
	if rf.NextIndex[server] < lastLogIndex {
		entries := rf.Log[rf.NextIndex[server]-1-rf.LastIncludedIndex:]
		copy(inputEntries, entries)
	}
	PrevLogTerm := rf.LastIncludedTerm
	if PrevLogIndex > rf.LastIncludedIndex {
		PrevLogTerm = rf.Log[PrevLogIndex-rf.LastIncludedIndex-1].Term
	}
	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex

	appendEntriesMsg := AppendEntriesRPC{}

	// newEntries := make([]LogEntry, len(rf.Log[PrevLogIndex+1:])) // TODO check this logic

	// copy(newEntries, rf.Log[PrevLogIndex+1:])
	appendEntriesMsg.Entries = inputEntries
	appendEntriesMsg.LeaderCommit = rf.CommitIndex
	appendEntriesMsg.LeaderID = rf.Me
	appendEntriesMsg.Term = rf.CurrentTerm
	appendEntriesMsg.PrevLogIndex = PrevLogIndex
	appendEntriesMsg.PrevLogTerm = PrevLogTerm
	return appendEntriesMsg
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

	// Print before any modifications
	fmt.Printf("before mod %v 's term: %v, args term: %v, current log %v, args entries %v, agrs prevIndex %v, args prevLogTerm %v \n",
		rf.Me,
		rf.CurrentTerm,
		args.Term,
		rf.Log,
		args.Entries,
		args.PrevLogIndex,
		args.PrevLogTerm)

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

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm {
		// fmt.Printf("%v received expired request. My current term is %v, args term is %v \n", rf.Me, rf.CurrentTerm, args.Term)
		reply.Inconsistency = true
		return
	}

	// 2D TODO: check whether this assumption is correct
	lastLogIndex, _ := rf.LastLogTermAndIndex()
	var lastLogTerm int
	diffLength := args.PrevLogIndex - lastLogIndex - 1 // might get off by 1 error
	if lastLogIndex == args.PrevLogIndex {
		lastLogTerm = args.PrevLogTerm
	} else {
		// this means args.Prevlogindex > prevlogindex
		// might get off by 1 error
		lastLogTerm = rf.Log[diffLength].Term
	}

	if args.PrevLogIndex > lastLogIndex {
		// If the previous log index is greater, then I immediately set the retry index to be the last index

		reply.XLen = lastLogIndex + 1 // TODO might get an off by 1 error
		fmt.Printf("%v in case 1.5. Term: %v, xlen %v, xInd %v, xTerm %v, inconsistency %v \n", rf.Me, rf.CurrentTerm, reply.XLen, reply.XIndex, reply.XTerm, reply.Inconsistency)
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// var prevLogTerm int
	// if lastincluded index + 1 = prevLogindex, then prevlogterm = lastincluded term
	// otherwise,
	if args.PrevLogIndex >= 0 {
		if lastLogTerm != args.PrevLogTerm {
			reply.XTerm = lastLogTerm

			// find the retry index
			for i := diffLength; i >= 0; i-- {
				if lastLogTerm == rf.Log[i].Term {
					reply.XIndex = rf.Log[i].Index
				}
			}
			return
		}
	}

	// 3. if existing entry conflicts with new one (same index but different terms)
	// delete existing entry and all that follows it

	if len(args.Entries) > 0 {

		entriesToLookAt := rf.Log[args.PrevLogIndex-rf.LastIncludedIndex:]

		iterLength := rf.GetMin(len(entriesToLookAt), len(args.Entries))

		for i := 0; i < iterLength; i++ {

			if args.Entries[i].Term != entriesToLookAt[i].Term {
				// we want to clear the term inconsistency
				// we want to keep the log until our starting index plus i
				// fmt.Printf("%v: conflict found. log was %v \n", rf.Me, rf.Log)
				rf.Log = rf.Log[:args.PrevLogIndex+i-rf.LastIncludedIndex]
				rf.persist()
			}
		}
		// TODO duplicate detection

		// fmt.Printf("%v 's conflict index is %v and entriesToLook at is %v, iterlength is %v in line 441 \n", rf.Me, conflictIndex, entriesToLookAt, iterLength)
		// if conflict index is in my entries then i want to add everything after
		for i := 0; i < len(args.Entries); i++ {
			if len(rf.Log) == args.PrevLogIndex+i+-rf.LastIncludedIndex {
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

	// fmt.Printf(" \n --- after Mod %v 's term: %v, args term: %v, current log %v, args entries %v, agrs prevIndex %v, args prevLogTerm %v, term at prevIndex %v --- \n",
	// 	rf.Me,
	// 	rf.CurrentTerm,
	// 	args.Term,
	// 	rf.Log,
	// 	args.Entries,
	// 	args.PrevLogIndex,
	// 	args.PrevLogTerm,
	// 	rf.Log[args.PrevLogIndex])

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
	fmt.Printf("leader %v received follower respond in %v \n", rf.Me, server)
	if reply.Success {

		// check for stale entries
		if args.PrevLogIndex < rf.NextIndex[server]-1 {
			rf.Mu.Unlock()
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
		// fmt.Printf("%v is attempting to resend append entries. CurrentTerm is %v. State is %v\n", rf.Me, rf.CurrentTerm, rf.State)
		// ask in OH: how do do indexing
		// optimization
		if !reply.Inconsistency {
			if reply.XLen == -1 {

				//   Case 1: leader doesn't have XTerm:
				xTermFound := false
				// for i := len(rf.Log) - 1; i >= 0; i-- {
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
			input := rf.makeAppendEntriesArgs(server)
			reply := AppendEntriesRPCReply{}
			rf.Mu.Unlock()

			// fmt.Printf("%v is sending append entries again in else case to %v and input is %v \n", rf.Me, server, input)
			rf.Peers[server].Call("Raft.AppendEntries", &input, &reply)
		} else {
			rf.Mu.Unlock()
		}
	}
}

func (rf *Raft) UpdateCommitIdx() {

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	lastLogIndex, _ := rf.LastLogTermAndIndex()
	// TODO figure out how the offsets work
	if rf.State == "leader" {
		for N := rf.CommitIndex + 1; N < len(rf.Log); N++ {
			count := 1
			for server := range rf.Peers {
				if server != rf.Me && rf.MatchIndex[server] >= N && rf.Log[N].Term == rf.CurrentTerm {
					count += 1
				}
			}
			if count > len(rf.Peers)/2 {
				rf.CommitIndex = N
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
		// fmt.Printf("%v received one vote in term %v \n", rf.Me, rf.CurrentTerm)

		if rf.SumVotes > len(rf.Peers)/2 && args.Term <= rf.CurrentTerm && rf.State != "leader" {
			fmt.Printf("I am the leader %v \n", rf.Me)
			// populate nextIndex and matchIndex

			rf.NextIndex = make([]int, len(rf.Peers))
			rf.MatchIndex = make([]int, len(rf.Peers))
			lastLogIndex, _ := rf.LastLogTermAndIndex()
			for i := 0; i < len(rf.Peers); i++ {
				rf.NextIndex[i] = lastLogIndex + 1
				rf.MatchIndex[i] = 0
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
	// defer rf.Mu.Lock()

	if rf.State != "candidate" || rf.killed() || currentTerm < rf.CurrentTerm || time.Now().After(rf.ExpirationTime) {
		// only candidates can send leader election notices
		rf.Mu.Unlock()
		return
	}

	rf.SumVotes = 1
	input := RequestVoteArgs{}

	input.CandidateID = rf.Me
	input.Term = rf.CurrentTerm
	lastLogTerm, lastLogIndex := rf.LastLogTermAndIndex()
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
	// defer rf.Mu.Unlock()

	isLeader := rf.State == "leader"
	index := len(rf.Log)
	term := rf.CurrentTerm

	// append command to local log
	if isLeader {

		newEntry := LogEntry{}
		newEntry.Command = command
		newEntry.Index = index
		newEntry.Term = term
		rf.Log = append(rf.Log, newEntry)
		rf.persist()
		// fmt.Printf("Leader %v received new log entry %v and now log is %v\n", rf.Me, newEntry, rf.Log)

		rf.Mu.Unlock()
		// leader sends out appendEntries
		for server := range rf.Peers {
			if server != rf.Me {
				rf.Mu.Lock()
				args := rf.makeAppendEntriesArgs(server)
				reply := AppendEntriesRPCReply{}
				rf.Mu.Unlock()
				go rf.SendAppendEntries(server, &args, &reply)
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

		n := rf.CommitIndex - rf.LastApplied
		// fmt.Printf("cmd = %v, app = %v, len = %v\n", rf.CommitIndex, rf.LastApplied, len(rf.Log))
		for i := 0; i < n; i++ {
			rf.LastApplied++

			// fmt.Printf("my %v last applied is %v and log is %v \n", rf.Me, rf.LastApplied, rf.Log)
			// TODO relinquish lock before sending to channel
			applyTerm := rf.Log[rf.LastApplied]

			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = applyTerm.Command
			applyMsg.CommandIndex = rf.LastApplied // double check

			fmt.Printf("\n %v is sending to channel %v at termv %v -------------- \n", rf.Me, applyMsg, rf.CurrentTerm)
			rf.ApplyCh <- applyMsg
		}
		rf.Mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds

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

	// initialize rf fields
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.LastIncludedIndex = -1
	rf.LastIncludedTerm = -1
	rf.NextIndex = []int{}
	rf.MatchIndex = []int{}
	rf.State = "follower"
	rf.ExpirationTime = time.Now()
	rf.ApplyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.CommitIndex = rf.LastIncludedIndex
	rf.LastApplied = rf.LastIncludedIndex

	rf.Mu.Unlock()

	// start ticker and log update goroutines to start elections
	go rf.ticker()
	go rf.UpdateLog()

	return rf
}
