----------------------------- MODULE ObjectKVCell -----------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* objectKV Cell v0 reference model.                                      *)
(*                                                                         *)
(* This is an architectural state machine, not an implementation of Raft,  *)
(* RocksDB, an object store, or a transaction resolver. It specifies the    *)
(* contracts those mechanisms must refine:                                 *)
(*                                                                         *)
(*   concurrent requests                                                    *)
(*       -> generation-fenced ordering                                      *)
(*       -> RAM staging                                                     *)
(*       -> quorum-stable txLog                                             *)
(*       -> asynchronous immutable object closure                           *)
(*       -> safe txLog pop                                                   *)
(*       -> disposable RAM / NVMe / Rocks serving images                     *)
(*                                                                         *)
(* The Fault* constants are deliberate negative-control switches. The       *)
(* reference configuration sets all of them to FALSE.                       *)
(***************************************************************************)

CONSTANTS
    Nodes,
    Txns,
    MaxVersion,
    MaxGeneration,
    Quorum,
    MaxMediaFailures,
    FaultAckBeforeStableQuorum,
    FaultSkipConflictValidation,
    FaultIgnoreGenerationFence,
    FaultPublishIncompleteClosure,
    FaultPopWithoutProtectedObject,
    FaultServeWithoutRecovery

ASSUME /\ Nodes # {}
       /\ Txns # {}
       /\ MaxVersion \in Nat \ {0}
       /\ MaxGeneration \in Nat \ {0}
       /\ Quorum \in 1..Cardinality(Nodes)
       /\ (2 * Quorum) > Cardinality(Nodes)
       /\ MaxMediaFailures \in 0..(Cardinality(Nodes) - 1)
       /\ FaultAckBeforeStableQuorum \in BOOLEAN
       /\ FaultSkipConflictValidation \in BOOLEAN
       /\ FaultIgnoreGenerationFence \in BOOLEAN
       /\ FaultPublishIncompleteClosure \in BOOLEAN
       /\ FaultPopWithoutProtectedObject \in BOOLEAN
       /\ FaultServeWithoutRecovery \in BOOLEAN

TxnStates == {"idle", "pending", "sequenced", "committed", "rejected"}
ReplyStates == {"none", "buffered", "unknown", "committed"}
ServingTiers == {"none", "ram", "nvme", "rocks"}
Versions == 0..MaxVersion
NodeSymmetry == Permutations(Nodes)

VARIABLES
    activeGeneration,
    nextVersion,
    commitVersion,
    txnState,
    txnGeneration,
    txnVersion,
    readVersion,
    conflicted,
    ramCopies,
    stableCopies,
    nodeEpoch,
    failedMedia,
    retainedTxLog,
    reply,
    quorumAtCommit,
    objectBuiltThrough,
    pendingObjectFrontier,
    pendingObjectGeneration,
    activeObjectFrontier,
    txLogFloor,
    servingTier,
    servingGeneration,
    servingThrough,
    servingReady,
    unsafeReadObserved,
    staleCommitObserved,
    conflictCommitObserved,
    earlyCommitObserved,
    unsafePopObserved,
    incompletePublicationObserved

vars == <<
    activeGeneration,
    nextVersion,
    commitVersion,
    txnState,
    txnGeneration,
    txnVersion,
    readVersion,
    conflicted,
    ramCopies,
    stableCopies,
    nodeEpoch,
    failedMedia,
    retainedTxLog,
    reply,
    quorumAtCommit,
    objectBuiltThrough,
    pendingObjectFrontier,
    pendingObjectGeneration,
    activeObjectFrontier,
    txLogFloor,
    servingTier,
    servingGeneration,
    servingThrough,
    servingReady,
    unsafeReadObserved,
    staleCommitObserved,
    conflictCommitObserved,
    earlyCommitObserved,
    unsafePopObserved,
    incompletePublicationObserved
>>

ProtectedObjectThrough ==
    IF pendingObjectFrontier > activeObjectFrontier
    THEN pendingObjectFrontier
    ELSE activeObjectFrontier

ObjectProtects(v) ==
    /\ v > 0
    /\ v <= objectBuiltThrough
    /\ v <= ProtectedObjectThrough

VersionRecoverable(v) ==
    \/ ObjectProtects(v)
    \/ /\ v \in retainedTxLog
       /\ stableCopies[v] # {}

CommittedVersions ==
    {txnVersion[t] : t \in {candidate \in Txns:
        txnState[candidate] = "committed"}}

EarlierSequenced(t) ==
    \E other \in Txns:
        /\ txnState[other] = "sequenced"
        /\ txnVersion[other] < txnVersion[t]

CanBuildObjectThrough(v) ==
    \A committed \in CommittedVersions:
        committed <= v => VersionRecoverable(committed)

CanServeThrough(v) ==
    \A committed \in CommittedVersions:
        committed <= v => VersionRecoverable(committed)

Init ==
    /\ activeGeneration = 1
    /\ nextVersion = 1
    /\ commitVersion = 0
    /\ txnState = [t \in Txns |-> "idle"]
    /\ txnGeneration = [t \in Txns |-> 0]
    /\ txnVersion = [t \in Txns |-> 0]
    /\ readVersion = [t \in Txns |-> 0]
    /\ conflicted = {}
    /\ ramCopies = [v \in Versions |-> {}]
    /\ stableCopies = [v \in Versions |-> {}]
    /\ nodeEpoch = [n \in Nodes |-> 1]
    /\ failedMedia = {}
    /\ retainedTxLog = {}
    /\ reply = [t \in Txns |-> "none"]
    /\ quorumAtCommit = [t \in Txns |-> FALSE]
    /\ objectBuiltThrough = 0
    /\ pendingObjectFrontier = 0
    /\ pendingObjectGeneration = 0
    /\ activeObjectFrontier = 0
    /\ txLogFloor = 0
    /\ servingTier = [n \in Nodes |-> "none"]
    /\ servingGeneration = [n \in Nodes |-> 0]
    /\ servingThrough = [n \in Nodes |-> 0]
    /\ servingReady = [n \in Nodes |-> FALSE]
    /\ unsafeReadObserved = FALSE
    /\ staleCommitObserved = FALSE
    /\ conflictCommitObserved = FALSE
    /\ earlyCommitObserved = FALSE
    /\ unsafePopObserved = FALSE
    /\ incompletePublicationObserved = FALSE

Begin(t) ==
    /\ txnState[t] = "idle"
    /\ txnState' = [txnState EXCEPT ![t] = "pending"]
    /\ txnGeneration' = [txnGeneration EXCEPT ![t] = activeGeneration]
    /\ readVersion' = [readVersion EXCEPT ![t] = commitVersion]
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnVersion, conflicted,
        ramCopies, stableCopies, nodeEpoch, failedMedia, retainedTxLog, reply,
        quorumAtCommit, objectBuiltThrough, pendingObjectFrontier,
        pendingObjectGeneration, activeObjectFrontier, txLogFloor,
        servingTier, servingGeneration, servingThrough, servingReady,
        unsafeReadObserved, staleCommitObserved, conflictCommitObserved,
        earlyCommitObserved, unsafePopObserved, incompletePublicationObserved
       >>

SequenceTxn(t) ==
    /\ txnState[t] = "pending"
    /\ nextVersion <= MaxVersion
    /\ txnState' = [txnState EXCEPT ![t] = "sequenced"]
    /\ txnVersion' = [txnVersion EXCEPT ![t] = nextVersion]
    /\ nextVersion' = nextVersion + 1
    /\ UNCHANGED <<
        activeGeneration, commitVersion, txnGeneration, readVersion, conflicted,
        ramCopies, stableCopies, nodeEpoch, failedMedia, retainedTxLog, reply,
        quorumAtCommit, objectBuiltThrough, pendingObjectFrontier,
        pendingObjectGeneration, activeObjectFrontier, txLogFloor,
        servingTier, servingGeneration, servingThrough, servingReady,
        unsafeReadObserved, staleCommitObserved, conflictCommitObserved,
        earlyCommitObserved, unsafePopObserved, incompletePublicationObserved
       >>

StageInRam(t, n) ==
    /\ txnState[t] = "sequenced"
    /\ nodeEpoch[n] = txnGeneration[t]
    /\ ramCopies' = [ramCopies EXCEPT ![txnVersion[t]] = @ \cup {n}]
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, stableCopies, nodeEpoch,
        failedMedia, retainedTxLog, reply, quorumAtCommit, objectBuiltThrough,
        pendingObjectFrontier, pendingObjectGeneration, activeObjectFrontier,
        txLogFloor, servingTier, servingGeneration, servingThrough,
        servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

PersistOnStableMedia(t, n) ==
    /\ txnState[t] = "sequenced"
    /\ n \in ramCopies[txnVersion[t]]
    /\ n \notin failedMedia
    /\ stableCopies' = [stableCopies EXCEPT ![txnVersion[t]] = @ \cup {n}]
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, nodeEpoch, failedMedia,
        retainedTxLog, reply, quorumAtCommit, objectBuiltThrough,
        pendingObjectFrontier, pendingObjectGeneration, activeObjectFrontier,
        txLogFloor, servingTier, servingGeneration, servingThrough,
        servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

ReturnBuffered(t) ==
    /\ txnState[t] = "sequenced"
    /\ Cardinality(ramCopies[txnVersion[t]]) >= Quorum
    /\ reply' = [reply EXCEPT ![t] = "buffered"]
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, retainedTxLog, quorumAtCommit,
        objectBuiltThrough, pendingObjectFrontier, pendingObjectGeneration,
        activeObjectFrontier, txLogFloor, servingTier, servingGeneration,
        servingThrough, servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

CommitTxn(t) ==
    LET hasStableQuorum == Cardinality(stableCopies[txnVersion[t]]) >= Quorum
        generationMatches == txnGeneration[t] = activeGeneration
        conflictFree == t \notin conflicted
        durabilityAllowed == hasStableQuorum \/ FaultAckBeforeStableQuorum
        generationAllowed == generationMatches \/ FaultIgnoreGenerationFence
        conflictAllowed == conflictFree \/ FaultSkipConflictValidation
    IN
    /\ txnState[t] = "sequenced"
    /\ ~EarlierSequenced(t)
    /\ durabilityAllowed
    /\ generationAllowed
    /\ conflictAllowed
    /\ txnState' = [txnState EXCEPT ![t] = "committed"]
    /\ commitVersion' = IF txnVersion[t] > commitVersion
                        THEN txnVersion[t]
                        ELSE commitVersion
    /\ retainedTxLog' = retainedTxLog \cup {txnVersion[t]}
    /\ reply' = [reply EXCEPT ![t] = "unknown"]
    /\ quorumAtCommit' = [quorumAtCommit EXCEPT ![t] = hasStableQuorum]
    /\ conflicted' = conflicted \cup
        {other \in Txns:
            other # t /\ txnState[other] \in {"pending", "sequenced"}}
    /\ staleCommitObserved' = staleCommitObserved \/ ~generationMatches
    /\ conflictCommitObserved' = conflictCommitObserved \/ ~conflictFree
    /\ earlyCommitObserved' = earlyCommitObserved \/ ~hasStableQuorum
    /\ UNCHANGED <<
        activeGeneration, nextVersion, txnGeneration, txnVersion, readVersion,
        ramCopies, stableCopies, nodeEpoch, failedMedia, objectBuiltThrough,
        pendingObjectFrontier, pendingObjectGeneration, activeObjectFrontier,
        txLogFloor, servingTier, servingGeneration, servingThrough,
        servingReady, unsafeReadObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

RejectConflict(t) ==
    /\ txnState[t] = "sequenced"
    /\ t \in conflicted
    /\ txnState' = [txnState EXCEPT ![t] = "rejected"]
    /\ reply' = [reply EXCEPT ![t] = "none"]
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, retainedTxLog, quorumAtCommit,
        objectBuiltThrough, pendingObjectFrontier, pendingObjectGeneration,
        activeObjectFrontier, txLogFloor, servingTier, servingGeneration,
        servingThrough, servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

DeliverCommitted(t) ==
    /\ txnState[t] = "committed"
    /\ reply[t] # "committed"
    /\ reply' = [reply EXCEPT ![t] = "committed"]
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, retainedTxLog, quorumAtCommit,
        objectBuiltThrough, pendingObjectFrontier, pendingObjectGeneration,
        activeObjectFrontier, txLogFloor, servingTier, servingGeneration,
        servingThrough, servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

BuildObjectClosure(v) ==
    /\ v \in 1..commitVersion
    /\ v > objectBuiltThrough
    /\ CanBuildObjectThrough(v)
    /\ objectBuiltThrough' = v
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, retainedTxLog, reply, quorumAtCommit,
        pendingObjectFrontier, pendingObjectGeneration, activeObjectFrontier,
        txLogFloor, servingTier, servingGeneration, servingThrough,
        servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

PrepareObjectFrontier(v) ==
    LET closureComplete == v <= objectBuiltThrough
    IN
    /\ pendingObjectFrontier = 0
    /\ v \in 1..commitVersion
    /\ v > activeObjectFrontier
    /\ closureComplete \/ FaultPublishIncompleteClosure
    /\ pendingObjectFrontier' = v
    /\ pendingObjectGeneration' = activeGeneration
    /\ incompletePublicationObserved' =
        incompletePublicationObserved \/ ~closureComplete
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, retainedTxLog, reply, quorumAtCommit,
        objectBuiltThrough, activeObjectFrontier, txLogFloor, servingTier,
        servingGeneration, servingThrough, servingReady, unsafeReadObserved,
        staleCommitObserved, conflictCommitObserved, earlyCommitObserved,
        unsafePopObserved
       >>

PopTxLogThroughPending ==
    LET protected ==
        /\ pendingObjectFrontier > txLogFloor
        /\ pendingObjectFrontier <= objectBuiltThrough
        /\ pendingObjectGeneration = activeGeneration
    IN
    /\ pendingObjectFrontier > 0
    /\ protected \/ FaultPopWithoutProtectedObject
    /\ txLogFloor' = pendingObjectFrontier
    /\ retainedTxLog' =
        {v \in retainedTxLog: v > pendingObjectFrontier}
    /\ unsafePopObserved' = unsafePopObserved \/ ~protected
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, reply, quorumAtCommit, objectBuiltThrough,
        pendingObjectFrontier, pendingObjectGeneration, activeObjectFrontier,
        servingTier, servingGeneration, servingThrough, servingReady,
        unsafeReadObserved, staleCommitObserved, conflictCommitObserved,
        earlyCommitObserved, incompletePublicationObserved
       >>

ActivateObjectFrontier ==
    /\ pendingObjectFrontier > 0
    /\ txLogFloor >= pendingObjectFrontier
    /\ activeObjectFrontier' = pendingObjectFrontier
    /\ pendingObjectFrontier' = 0
    /\ pendingObjectGeneration' = 0
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, retainedTxLog, reply, quorumAtCommit,
        objectBuiltThrough, txLogFloor, servingTier, servingGeneration,
        servingThrough, servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

AdvanceGeneration ==
    /\ activeGeneration < MaxGeneration
    /\ activeGeneration' = activeGeneration + 1
    /\ UNCHANGED <<
        nextVersion, commitVersion, txnState, txnGeneration, txnVersion,
        readVersion, conflicted, ramCopies, stableCopies, nodeEpoch,
        failedMedia, retainedTxLog, reply, quorumAtCommit,
        objectBuiltThrough, pendingObjectFrontier, pendingObjectGeneration,
        activeObjectFrontier, txLogFloor, servingTier, servingGeneration,
        servingThrough, servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

InstallGeneration(n) ==
    /\ nodeEpoch[n] < activeGeneration
    /\ nodeEpoch' = [nodeEpoch EXCEPT ![n] = activeGeneration]
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        failedMedia, retainedTxLog, reply, quorumAtCommit,
        objectBuiltThrough, pendingObjectFrontier, pendingObjectGeneration,
        activeObjectFrontier, txLogFloor, servingTier, servingGeneration,
        servingThrough, servingReady, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

LoseRam(n) ==
    /\ \E v \in Versions: n \in ramCopies[v]
    /\ ramCopies' = [v \in Versions |-> ramCopies[v] \ {n}]
    /\ servingReady' =
        IF servingTier[n] = "ram"
        THEN [servingReady EXCEPT ![n] = FALSE]
        ELSE servingReady
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, stableCopies, nodeEpoch,
        failedMedia, retainedTxLog, reply, quorumAtCommit,
        objectBuiltThrough, pendingObjectFrontier, pendingObjectGeneration,
        activeObjectFrontier, txLogFloor, servingTier, servingGeneration,
        servingThrough, unsafeReadObserved, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

LoseStableMedium(n) ==
    /\ n \notin failedMedia
    /\ Cardinality(failedMedia) < MaxMediaFailures
    /\ failedMedia' = failedMedia \cup {n}
    /\ stableCopies' = [v \in Versions |-> stableCopies[v] \ {n}]
    /\ servingReady' =
        IF servingTier[n] \in {"nvme", "rocks"}
        THEN [servingReady EXCEPT ![n] = FALSE]
        ELSE servingReady
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, nodeEpoch,
        retainedTxLog, reply, quorumAtCommit, objectBuiltThrough,
        pendingObjectFrontier, pendingObjectGeneration, activeObjectFrontier,
        txLogFloor, servingTier, servingGeneration, servingThrough,
        unsafeReadObserved, staleCommitObserved, conflictCommitObserved,
        earlyCommitObserved, unsafePopObserved, incompletePublicationObserved
       >>

HydrateServingImage(n, tier, v) ==
    LET reconstructable == CanServeThrough(v)
    IN
    /\ tier \in ServingTiers \ {"none"}
    /\ v \in 0..commitVersion
    /\ reconstructable \/ FaultServeWithoutRecovery
    /\ servingTier' = [servingTier EXCEPT ![n] = tier]
    /\ servingGeneration' = [servingGeneration EXCEPT ![n] = activeGeneration]
    /\ servingThrough' = [servingThrough EXCEPT ![n] = v]
    /\ servingReady' = [servingReady EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, retainedTxLog, reply, quorumAtCommit,
        objectBuiltThrough, pendingObjectFrontier, pendingObjectGeneration,
        activeObjectFrontier, txLogFloor, unsafeReadObserved,
        staleCommitObserved, conflictCommitObserved, earlyCommitObserved,
        unsafePopObserved, incompletePublicationObserved
       >>

ServeRead(n) ==
    LET safe ==
        /\ servingGeneration[n] = activeGeneration
        /\ CanServeThrough(servingThrough[n])
    IN
    /\ servingReady[n]
    /\ safe \/ FaultServeWithoutRecovery
    /\ unsafeReadObserved' = unsafeReadObserved \/ ~safe
    /\ UNCHANGED <<
        activeGeneration, nextVersion, commitVersion, txnState, txnGeneration,
        txnVersion, readVersion, conflicted, ramCopies, stableCopies,
        nodeEpoch, failedMedia, retainedTxLog, reply, quorumAtCommit,
        objectBuiltThrough, pendingObjectFrontier, pendingObjectGeneration,
        activeObjectFrontier, txLogFloor, servingTier, servingGeneration,
        servingThrough, servingReady, staleCommitObserved,
        conflictCommitObserved, earlyCommitObserved, unsafePopObserved,
        incompletePublicationObserved
       >>

Next ==
    \/ \E t \in Txns: Begin(t)
    \/ \E t \in Txns: SequenceTxn(t)
    \/ \E t \in Txns, n \in Nodes: StageInRam(t, n)
    \/ \E t \in Txns, n \in Nodes: PersistOnStableMedia(t, n)
    \/ \E t \in Txns: ReturnBuffered(t)
    \/ \E t \in Txns: CommitTxn(t)
    \/ \E t \in Txns: RejectConflict(t)
    \/ \E t \in Txns: DeliverCommitted(t)
    \/ \E v \in Versions: BuildObjectClosure(v)
    \/ \E v \in Versions: PrepareObjectFrontier(v)
    \/ PopTxLogThroughPending
    \/ ActivateObjectFrontier
    \/ AdvanceGeneration
    \/ \E n \in Nodes: InstallGeneration(n)
    \/ \E n \in Nodes: LoseRam(n)
    \/ \E n \in Nodes: LoseStableMedium(n)
    \/ \E n \in Nodes, tier \in ServingTiers, v \in Versions:
        HydrateServingImage(n, tier, v)
    \/ \E n \in Nodes: ServeRead(n)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ activeGeneration \in 1..MaxGeneration
    /\ nextVersion \in 1..(MaxVersion + 1)
    /\ commitVersion \in Versions
    /\ txnState \in [Txns -> TxnStates]
    /\ txnGeneration \in [Txns -> 0..MaxGeneration]
    /\ txnVersion \in [Txns -> Versions]
    /\ readVersion \in [Txns -> Versions]
    /\ conflicted \subseteq Txns
    /\ ramCopies \in [Versions -> SUBSET Nodes]
    /\ stableCopies \in [Versions -> SUBSET Nodes]
    /\ nodeEpoch \in [Nodes -> 1..MaxGeneration]
    /\ failedMedia \subseteq Nodes
    /\ retainedTxLog \subseteq Versions \ {0}
    /\ reply \in [Txns -> ReplyStates]
    /\ quorumAtCommit \in [Txns -> BOOLEAN]
    /\ objectBuiltThrough \in Versions
    /\ pendingObjectFrontier \in Versions
    /\ pendingObjectGeneration \in 0..MaxGeneration
    /\ activeObjectFrontier \in Versions
    /\ txLogFloor \in Versions
    /\ servingTier \in [Nodes -> ServingTiers]
    /\ servingGeneration \in [Nodes -> 0..MaxGeneration]
    /\ servingThrough \in [Nodes -> Versions]
    /\ servingReady \in [Nodes -> BOOLEAN]
    /\ unsafeReadObserved \in BOOLEAN
    /\ staleCommitObserved \in BOOLEAN
    /\ conflictCommitObserved \in BOOLEAN
    /\ earlyCommitObserved \in BOOLEAN
    /\ unsafePopObserved \in BOOLEAN
    /\ incompletePublicationObserved \in BOOLEAN

ObjectFrontiersAreComplete ==
    /\ activeObjectFrontier <= objectBuiltThrough
    /\ pendingObjectFrontier <= objectBuiltThrough

SafeTxLogPop ==
    /\ txLogFloor <= objectBuiltThrough
    /\ txLogFloor <= ProtectedObjectThrough
    /\ ~unsafePopObserved

CommittedStateIsRecoverable ==
    \A committed \in CommittedVersions: VersionRecoverable(committed)

RetainedSuffixIsExact ==
    \A committed \in CommittedVersions:
        committed > txLogFloor => committed \in retainedTxLog

RepliesTellTheTruth ==
    /\ \A t \in Txns:
        reply[t] = "committed" =>
            /\ txnState[t] = "committed"
            /\ quorumAtCommit[t]
            /\ VersionRecoverable(txnVersion[t])
    /\ \A t \in Txns:
        reply[t] = "buffered" => txnState[t] # "committed"
    /\ ~earlyCommitObserved

VersionsAreUnique ==
    \A a, b \in Txns:
        /\ txnVersion[a] > 0
        /\ txnVersion[a] = txnVersion[b]
        => a = b

GenerationsAreFenced == ~staleCommitObserved
ConflictsAreValidated == ~conflictCommitObserved
ServingReadsAreSafe == ~unsafeReadObserved
PublicationsAreComplete == ~incompletePublicationObserved

Safety ==
    /\ TypeOK
    /\ activeObjectFrontier <= commitVersion
    /\ pendingObjectFrontier <= commitVersion
    /\ ObjectFrontiersAreComplete
    /\ SafeTxLogPop
    /\ CommittedStateIsRecoverable
    /\ RetainedSuffixIsExact
    /\ RepliesTellTheTruth
    /\ VersionsAreUnique
    /\ GenerationsAreFenced
    /\ ConflictsAreValidated
    /\ ServingReadsAreSafe
    /\ PublicationsAreComplete

(***************************************************************************)
(* The concurrency configuration keeps the same transaction and quorum     *)
(* actions but prunes objectification, media loss, and serving-image state. *)
(* This makes two-request conflict and generation exploration tractable     *)
(* without weakening the integrated one-request cell model.                 *)
(***************************************************************************)
ConcurrencyConstraint ==
    /\ objectBuiltThrough = 0
    /\ pendingObjectFrontier = 0
    /\ activeObjectFrontier = 0
    /\ txLogFloor = 0
    /\ failedMedia = {}
    /\ \A n \in Nodes:
        /\ servingTier[n] = "none"
        /\ ~servingReady[n]

OperationalConstraint ==
    /\ Cardinality({n \in Nodes: servingReady[n]}) <= 1
    /\ \A n \in Nodes:
        ~servingReady[n] =>
            /\ servingTier[n] = "none"
            /\ servingGeneration[n] = 0
            /\ servingThrough[n] = 0

=============================================================================
