use crate::chess::ChessState;
use crate::delta::{encode_move, record_label};
use okv_app_history::{
    decode_checkpoint, decode_history_segment, encode_checkpoint, encode_history_segment,
    GameHistoryManifestV1, HistoryObjectRef, HistorySegmentRef, ReducerId,
};
use okv_playground_support::{MemoryObjectHistory, ObjectBlob};
use okv_publication::ObjectKind;
use std::collections::BTreeSet;
use std::time::Instant;

const REDUCER_SCHEMA_VERSION: u8 = 1;

pub fn run_object_golden() -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_object_golden_async())
}

pub fn run_branch_gc_golden() -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_branch_gc_golden_async())
}

async fn run_object_golden_async() -> Result<(), String> {
    let moves = ["e2e4", "e7e5", "c2c4"];
    let expected = reduce(ChessState::default(), &moves)?;
    let checkpoint = checkpoint_blob("chess-g5")?;
    let segment = segment_blob("chess-g5", "tail", 1, &moves)?;
    let manifest = manifest_blob(
        "chess-g5",
        checkpoint.reference.clone(),
        vec![segment_reference(&segment, 1, 3)],
        3,
        None,
        None,
        expected.fingerprint(),
    )?;
    let mut history = MemoryObjectHistory::new();
    let started = Instant::now();
    let prepared = history
        .prepare_and_upload(
            "chess-g5-main",
            "chess/main",
            None,
            &manifest,
            &[checkpoint, segment],
        )
        .await?;
    if reconstruct_flat(&mut history, &manifest.reference).await? != expected {
        return Err("Chess G5 recursive verification diverged".to_owned());
    }
    history.publish(&prepared)?;
    let cold_reopen = reconstruct_flat(&mut history, &manifest.reference).await?;
    let root_exact = history.root("chess/main") == Some(&prepared.manifest);
    if cold_reopen != expected || !root_exact {
        return Err("Chess G5 cold reopen diverged".to_owned());
    }
    println!(
        "{{\"workload\":\"chess-object-history-v1\",\"rung\":\"G5\",\"status\":\"VERIFIED\",\"scope\":\"memory-object-store-pure-publication-authority\",\"gcs_status\":\"PROPOSED\",\"replicated_publication_authority\":false,\"moves\":3,\"object_puts\":{},\"object_gets\":{},\"put_bytes\":{},\"get_bytes\":{},\"publication_millis\":{},\"recursive_closure_verified_before_publish\":true,\"cold_reopen_exact\":true,\"root_exact\":true,\"fingerprint\":\"{}\"}}",
        history.put_count(),
        history.get_count(),
        history.put_bytes(),
        history.get_bytes(),
        started.elapsed().as_millis(),
        cold_reopen.fingerprint()
    );
    Ok(())
}

async fn run_branch_gc_golden_async() -> Result<(), String> {
    let prefix_moves = ["e2e4", "e7e5"];
    let main_suffix_moves = ["g1f3", "b8c6"];
    let branch_moves = ["c2c4"];
    let fork_state = reduce(ChessState::default(), &prefix_moves)?;
    let main_state = reduce(fork_state.clone(), &main_suffix_moves)?;
    let branch_state = reduce(fork_state, &branch_moves)?;
    if main_state == branch_state {
        return Err("Chess G6 branch did not diverge".to_owned());
    }
    let checkpoint = checkpoint_blob("chess-g6")?;
    let prefix = segment_blob("chess-g6", "prefix", 1, &prefix_moves)?;
    let main_suffix = segment_blob("chess-g6", "main-suffix", 3, &main_suffix_moves)?;
    let main_manifest = manifest_blob(
        "chess-g6-main",
        checkpoint.reference.clone(),
        vec![
            segment_reference(&prefix, 1, 2),
            segment_reference(&main_suffix, 3, 4),
        ],
        4,
        None,
        None,
        main_state.fingerprint(),
    )?;
    let mut history = MemoryObjectHistory::new();
    let main_prepared = history
        .prepare_and_upload(
            "chess-g6-main",
            "chess/main",
            None,
            &main_manifest,
            &[checkpoint.clone(), prefix.clone(), main_suffix.clone()],
        )
        .await?;
    if reconstruct_flat(&mut history, &main_manifest.reference).await? != main_state {
        return Err("Chess G6 main verification diverged".to_owned());
    }
    history.publish(&main_prepared)?;
    history.pin_from_root("chess/main", &main_prepared.manifest, "chess-branch-build")?;

    let branch_suffix = segment_blob("chess-g6", "branch-suffix", 3, &branch_moves)?;
    let branch_manifest = manifest_blob(
        "chess-g6-branch",
        checkpoint.reference.clone(),
        vec![segment_reference(&branch_suffix, 3, 3)],
        3,
        Some(main_manifest.reference.clone()),
        Some(2),
        branch_state.fingerprint(),
    )?;
    let puts_before_branch = history.put_count();
    let branch_prepared = history
        .prepare_and_upload(
            "chess-g6-branch",
            "chess/line-1",
            None,
            &branch_manifest,
            std::slice::from_ref(&branch_suffix),
        )
        .await?;
    if reconstruct_branch(&mut history, &branch_manifest.reference).await? != branch_state {
        return Err("Chess G6 branch verification diverged".to_owned());
    }
    history.publish(&branch_prepared)?;
    history.unpin("chess-branch-build", &main_prepared.manifest)?;
    let branch_new_puts = history.put_count().saturating_sub(puts_before_branch);
    if branch_new_puts != 2 {
        return Err("Chess G6 branch copied inherited objects".to_owned());
    }
    history.remove_root("chess/line-1", &branch_prepared.manifest)?;
    let reachable = BTreeSet::from([
        main_manifest.reference.key.clone(),
        checkpoint.reference.key.clone(),
        prefix.reference.key.clone(),
        main_suffix.reference.key.clone(),
    ]);
    let deleted = history
        .sweep_unreachable(&reachable, "chess-g6-sweep")
        .await?;
    let main_after_gc = reconstruct_flat(&mut history, &main_manifest.reference).await?;
    if deleted.len() != 2 || main_after_gc != main_state {
        return Err("Chess G6 GC violated reachable history".to_owned());
    }
    println!(
        "{{\"workload\":\"chess-object-branch-gc-v1\",\"rung\":\"G6\",\"status\":\"VERIFIED\",\"scope\":\"memory-object-store-pure-publication-authority\",\"replicated_publication_authority\":false,\"main_moves\":4,\"fork_position\":2,\"branch_moves\":1,\"branch_new_puts\":{},\"copied_prefix_puts\":0,\"deleted_branch_objects\":{},\"surviving_main_objects\":{},\"main_after_gc_exact\":true,\"branch_before_gc_exact\":true,\"pin_from_root_used\":true,\"exact_root_removal_used\":true,\"fingerprint\":\"{}\"}}",
        branch_new_puts,
        deleted.len(),
        reachable.len(),
        main_after_gc.fingerprint()
    );
    Ok(())
}

async fn reconstruct_flat(
    history: &mut MemoryObjectHistory,
    manifest_ref: &HistoryObjectRef,
) -> Result<ChessState, String> {
    let manifest = GameHistoryManifestV1::decode(&history.read(manifest_ref).await?)?;
    let checkpoint = decode_checkpoint(
        ReducerId::Chess,
        REDUCER_SCHEMA_VERSION,
        manifest.checkpoint_position,
        &history.read(&manifest.checkpoint).await?,
    )
    .map_err(|error| error.to_string())?;
    let mut state = ChessState::decode(&checkpoint.state)?;
    for segment in &manifest.segments {
        let records = decode_history_segment(
            ReducerId::Chess,
            REDUCER_SCHEMA_VERSION,
            segment.first_position,
            &history.read(&segment.object).await?,
        )
        .map_err(|error| error.to_string())?;
        for record in records {
            state = state.apply_move(&record_label(&record.payload)?)?.0;
        }
    }
    if state.fingerprint() != manifest.expected_fingerprint {
        return Err("Chess object manifest fingerprint mismatch".to_owned());
    }
    Ok(state)
}

async fn reconstruct_branch(
    history: &mut MemoryObjectHistory,
    child_ref: &HistoryObjectRef,
) -> Result<ChessState, String> {
    let child = GameHistoryManifestV1::decode(&history.read(child_ref).await?)?;
    let parent_ref = child
        .parent_manifest
        .as_ref()
        .ok_or_else(|| "branch manifest has no parent".to_owned())?;
    let fork_position = child
        .fork_position
        .ok_or_else(|| "branch manifest has no fork position".to_owned())?;
    let parent = GameHistoryManifestV1::decode(&history.read(parent_ref).await?)?;
    let checkpoint = decode_checkpoint(
        ReducerId::Chess,
        REDUCER_SCHEMA_VERSION,
        parent.checkpoint_position,
        &history.read(&parent.checkpoint).await?,
    )
    .map_err(|error| error.to_string())?;
    let mut state = ChessState::decode(&checkpoint.state)?;
    for segment in &parent.segments {
        let records = decode_history_segment(
            ReducerId::Chess,
            REDUCER_SCHEMA_VERSION,
            segment.first_position,
            &history.read(&segment.object).await?,
        )
        .map_err(|error| error.to_string())?;
        for record in records {
            if record.position <= fork_position {
                state = state.apply_move(&record_label(&record.payload)?)?.0;
            }
        }
    }
    for segment in &child.segments {
        let records = decode_history_segment(
            ReducerId::Chess,
            REDUCER_SCHEMA_VERSION,
            segment.first_position,
            &history.read(&segment.object).await?,
        )
        .map_err(|error| error.to_string())?;
        for record in records {
            state = state.apply_move(&record_label(&record.payload)?)?.0;
        }
    }
    if state.fingerprint() != child.expected_fingerprint {
        return Err("Chess branch manifest fingerprint mismatch".to_owned());
    }
    Ok(state)
}

fn checkpoint_blob(namespace: &str) -> Result<ObjectBlob, String> {
    let bytes = encode_checkpoint(
        ReducerId::Chess,
        REDUCER_SCHEMA_VERSION,
        0,
        &ChessState::default().encode(),
    )
    .map_err(|error| error.to_string())?;
    Ok(ObjectBlob::content_addressed(
        namespace,
        "checkpoint",
        ObjectKind::Data,
        bytes,
    ))
}

fn segment_blob(
    namespace: &str,
    label: &str,
    first_position: u64,
    moves: &[&str],
) -> Result<ObjectBlob, String> {
    let mut state = ChessState::default();
    if first_position > 1 {
        let prefix_length = usize::try_from(first_position - 1)
            .map_err(|_| "Chess history position exceeds usize".to_owned())?;
        for notation in ["e2e4", "e7e5"].iter().take(prefix_length) {
            state = state.apply_move(notation)?.0;
        }
    }
    let mut payloads = Vec::with_capacity(moves.len());
    for notation in moves {
        let (next, applied) = state.apply_move(notation)?;
        payloads.push(encode_move(applied.from, applied.to)?.to_vec());
        state = next;
    }
    let bytes = encode_history_segment(
        ReducerId::Chess,
        REDUCER_SCHEMA_VERSION,
        first_position,
        &payloads,
    )
    .map_err(|error| error.to_string())?;
    Ok(ObjectBlob::content_addressed(
        namespace,
        label,
        ObjectKind::Data,
        bytes,
    ))
}

#[allow(clippy::too_many_arguments)]
fn manifest_blob(
    namespace: &str,
    checkpoint: HistoryObjectRef,
    segments: Vec<HistorySegmentRef>,
    covered_through: u64,
    parent_manifest: Option<HistoryObjectRef>,
    fork_position: Option<u64>,
    expected_fingerprint: String,
) -> Result<ObjectBlob, String> {
    let manifest = GameHistoryManifestV1 {
        format_version: 1,
        reducer: ReducerId::Chess,
        reducer_schema: REDUCER_SCHEMA_VERSION,
        checkpoint,
        checkpoint_position: 0,
        segments,
        covered_through,
        parent_manifest,
        fork_position,
        expected_fingerprint,
    };
    Ok(ObjectBlob::content_addressed(
        namespace,
        "manifest",
        ObjectKind::Manifest,
        manifest.encode()?,
    ))
}

fn segment_reference(object: &ObjectBlob, first: u64, last: u64) -> HistorySegmentRef {
    HistorySegmentRef {
        object: object.reference.clone(),
        first_position: first,
        last_position: last,
    }
}

fn reduce(mut state: ChessState, moves: &[&str]) -> Result<ChessState, String> {
    for notation in moves {
        state = state.apply_move(notation)?.0;
    }
    Ok(state)
}
