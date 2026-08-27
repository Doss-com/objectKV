use crate::delta::{decode_action, encode_action};
use crate::game::{Action, GameState};
use crate::trace;
use okv_app_history::{
    decode_checkpoint, decode_history_segment, encode_checkpoint, encode_history_segment,
    GameHistoryManifestV1, HistoryObjectRef, HistorySegmentRef, ReducerId,
};
use okv_playground_support::{MemoryObjectHistory, ObjectBlob};
use okv_publication::ObjectKind;
use std::collections::BTreeSet;
use std::time::Instant;

const REDUCER_SCHEMA_VERSION: u8 = 1;

pub fn run_object_golden(actions: usize) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_object_golden_async(actions))
}

pub fn run_branch_gc_golden(actions: usize) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_branch_gc_golden_async(actions))
}

async fn run_object_golden_async(actions: usize) -> Result<(), String> {
    if actions < 2 {
        return Err("Tetris G5 requires at least two actions".to_owned());
    }
    let action_trace = trace::deterministic_actions(actions);
    let trace_sha256 = trace::trace_sha256(&action_trace);
    let expected = reduce(GameState::default(), &action_trace);
    let checkpoint = checkpoint_blob("tetris-g5", &GameState::default(), 0)?;
    let segment = segment_blob("tetris-g5", "tail", 1, &action_trace)?;
    let manifest = manifest_blob(
        "tetris-g5",
        checkpoint.reference.clone(),
        vec![segment_reference(&segment, 1, actions as u64)],
        actions as u64,
        None,
        None,
        expected.fingerprint(),
    )?;
    let mut history = MemoryObjectHistory::new();
    let started = Instant::now();
    let prepared = history
        .prepare_and_upload(
            "tetris-g5-main",
            "tetris/main",
            None,
            &manifest,
            &[checkpoint, segment],
        )
        .await?;
    let verified_before_publish = reconstruct_flat(&mut history, &manifest.reference).await?;
    if verified_before_publish != expected {
        return Err("Tetris G5 recursive verification diverged".to_owned());
    }
    history.publish(&prepared)?;
    let cold_reopen = reconstruct_flat(&mut history, &manifest.reference).await?;
    let root_exact = history.root("tetris/main") == Some(&prepared.manifest);
    if cold_reopen != expected || !root_exact {
        return Err("Tetris G5 cold reopen diverged".to_owned());
    }
    println!(
        "{{\"workload\":\"tetris-object-history-v1\",\"rung\":\"G5\",\"status\":\"VERIFIED\",\"scope\":\"memory-object-store-pure-publication-authority\",\"gcs_status\":\"PROPOSED\",\"replicated_publication_authority\":false,\"trace_sha256\":\"{}\",\"actions\":{},\"object_puts\":{},\"object_gets\":{},\"put_bytes\":{},\"get_bytes\":{},\"publication_millis\":{},\"recursive_closure_verified_before_publish\":true,\"cold_reopen_exact\":true,\"root_exact\":true,\"fingerprint\":\"{}\"}}",
        trace_sha256,
        actions,
        history.put_count(),
        history.get_count(),
        history.put_bytes(),
        history.get_bytes(),
        started.elapsed().as_millis(),
        cold_reopen.fingerprint()
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_branch_gc_golden_async(actions: usize) -> Result<(), String> {
    if actions < 4 || actions % 2 != 0 {
        return Err("Tetris G6 requires an even action count of at least four".to_owned());
    }
    let fork_position = actions / 2;
    let action_trace = trace::deterministic_actions(actions);
    let trace_sha256 = trace::trace_sha256(&action_trace);
    let fork_state = reduce(GameState::default(), &action_trace[..fork_position]);
    let main_state = reduce(fork_state.clone(), &action_trace[fork_position..]);
    let branch_actions = divergent_actions(fork_state.clone(), 64);
    let branch_state = reduce(fork_state, &branch_actions);
    if branch_state == main_state {
        return Err("Tetris G6 branch did not diverge".to_owned());
    }

    let checkpoint = checkpoint_blob("tetris-g6", &GameState::default(), 0)?;
    let prefix = segment_blob("tetris-g6", "prefix", 1, &action_trace[..fork_position])?;
    let main_suffix = segment_blob(
        "tetris-g6",
        "main-suffix",
        fork_position as u64 + 1,
        &action_trace[fork_position..],
    )?;
    let main_manifest = manifest_blob(
        "tetris-g6-main",
        checkpoint.reference.clone(),
        vec![
            segment_reference(&prefix, 1, fork_position as u64),
            segment_reference(&main_suffix, fork_position as u64 + 1, actions as u64),
        ],
        actions as u64,
        None,
        None,
        main_state.fingerprint(),
    )?;

    let mut history = MemoryObjectHistory::new();
    let main_prepared = history
        .prepare_and_upload(
            "tetris-g6-main",
            "tetris/main",
            None,
            &main_manifest,
            &[checkpoint.clone(), prefix.clone(), main_suffix.clone()],
        )
        .await?;
    if reconstruct_flat(&mut history, &main_manifest.reference).await? != main_state {
        return Err("Tetris G6 main verification diverged".to_owned());
    }
    history.publish(&main_prepared)?;
    history.pin_from_root(
        "tetris/main",
        &main_prepared.manifest,
        "tetris-branch-build",
    )?;

    let branch_suffix = segment_blob(
        "tetris-g6",
        "branch-suffix",
        fork_position as u64 + 1,
        &branch_actions,
    )?;
    let branch_manifest = manifest_blob(
        "tetris-g6-branch",
        checkpoint.reference.clone(),
        vec![segment_reference(
            &branch_suffix,
            fork_position as u64 + 1,
            fork_position as u64 + branch_actions.len() as u64,
        )],
        fork_position as u64 + branch_actions.len() as u64,
        Some(main_manifest.reference.clone()),
        Some(fork_position as u64),
        branch_state.fingerprint(),
    )?;
    let puts_before_branch = history.put_count();
    let branch_prepared = history
        .prepare_and_upload(
            "tetris-g6-branch",
            "tetris/branch-1",
            None,
            &branch_manifest,
            std::slice::from_ref(&branch_suffix),
        )
        .await?;
    let branch_verified = reconstruct_branch(&mut history, &branch_manifest.reference).await?;
    if branch_verified != branch_state {
        return Err("Tetris G6 branch verification diverged".to_owned());
    }
    history.publish(&branch_prepared)?;
    history.unpin("tetris-branch-build", &main_prepared.manifest)?;
    let branch_new_puts = history.put_count().saturating_sub(puts_before_branch);
    if branch_new_puts != 2 {
        return Err("Tetris G6 branch copied inherited objects".to_owned());
    }

    history.remove_root("tetris/branch-1", &branch_prepared.manifest)?;
    let reachable = BTreeSet::from([
        main_manifest.reference.key.clone(),
        checkpoint.reference.key.clone(),
        prefix.reference.key.clone(),
        main_suffix.reference.key.clone(),
    ]);
    let deleted = history
        .sweep_unreachable(&reachable, "tetris-g6-sweep")
        .await?;
    let main_after_gc = reconstruct_flat(&mut history, &main_manifest.reference).await?;
    if deleted.len() != 2 || main_after_gc != main_state {
        return Err("Tetris G6 GC violated reachable history".to_owned());
    }
    println!(
        "{{\"workload\":\"tetris-object-branch-gc-v1\",\"rung\":\"G6\",\"status\":\"VERIFIED\",\"scope\":\"memory-object-store-pure-publication-authority\",\"replicated_publication_authority\":false,\"trace_sha256\":\"{}\",\"main_actions\":{},\"fork_position\":{},\"branch_actions\":{},\"branch_new_puts\":{},\"copied_prefix_puts\":0,\"deleted_branch_objects\":{},\"surviving_main_objects\":{},\"main_after_gc_exact\":true,\"branch_before_gc_exact\":true,\"pin_from_root_used\":true,\"exact_root_removal_used\":true,\"fingerprint\":\"{}\"}}",
        trace_sha256,
        actions,
        fork_position,
        branch_actions.len(),
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
) -> Result<GameState, String> {
    let manifest_bytes = history.read(manifest_ref).await?;
    let manifest = GameHistoryManifestV1::decode(&manifest_bytes)?;
    let checkpoint_bytes = history.read(&manifest.checkpoint).await?;
    let checkpoint = decode_checkpoint(
        ReducerId::Tetris,
        REDUCER_SCHEMA_VERSION,
        manifest.checkpoint_position,
        &checkpoint_bytes,
    )
    .map_err(|error| error.to_string())?;
    let mut state = GameState::decode(&checkpoint.state)?;
    for segment in &manifest.segments {
        let bytes = history.read(&segment.object).await?;
        for record in decode_history_segment(
            ReducerId::Tetris,
            REDUCER_SCHEMA_VERSION,
            segment.first_position,
            &bytes,
        )
        .map_err(|error| error.to_string())?
        {
            state = state.apply(decode_action(&record.payload)?);
        }
    }
    if state.fingerprint() != manifest.expected_fingerprint {
        return Err("Tetris object manifest fingerprint mismatch".to_owned());
    }
    Ok(state)
}

async fn reconstruct_branch(
    history: &mut MemoryObjectHistory,
    child_ref: &HistoryObjectRef,
) -> Result<GameState, String> {
    let child_bytes = history.read(child_ref).await?;
    let child = GameHistoryManifestV1::decode(&child_bytes)?;
    let parent_ref = child
        .parent_manifest
        .as_ref()
        .ok_or_else(|| "branch manifest has no parent".to_owned())?;
    let fork_position = child
        .fork_position
        .ok_or_else(|| "branch manifest has no fork position".to_owned())?;
    let parent_bytes = history.read(parent_ref).await?;
    let parent = GameHistoryManifestV1::decode(&parent_bytes)?;
    let checkpoint_bytes = history.read(&parent.checkpoint).await?;
    let checkpoint = decode_checkpoint(
        ReducerId::Tetris,
        REDUCER_SCHEMA_VERSION,
        parent.checkpoint_position,
        &checkpoint_bytes,
    )
    .map_err(|error| error.to_string())?;
    let mut state = GameState::decode(&checkpoint.state)?;
    for segment in &parent.segments {
        let bytes = history.read(&segment.object).await?;
        for record in decode_history_segment(
            ReducerId::Tetris,
            REDUCER_SCHEMA_VERSION,
            segment.first_position,
            &bytes,
        )
        .map_err(|error| error.to_string())?
        {
            if record.position <= fork_position {
                state = state.apply(decode_action(&record.payload)?);
            }
        }
    }
    for segment in &child.segments {
        let bytes = history.read(&segment.object).await?;
        for record in decode_history_segment(
            ReducerId::Tetris,
            REDUCER_SCHEMA_VERSION,
            segment.first_position,
            &bytes,
        )
        .map_err(|error| error.to_string())?
        {
            state = state.apply(decode_action(&record.payload)?);
        }
    }
    if state.fingerprint() != child.expected_fingerprint {
        return Err("Tetris branch manifest fingerprint mismatch".to_owned());
    }
    Ok(state)
}

fn checkpoint_blob(
    namespace: &str,
    state: &GameState,
    position: u64,
) -> Result<ObjectBlob, String> {
    let bytes = encode_checkpoint(
        ReducerId::Tetris,
        REDUCER_SCHEMA_VERSION,
        position,
        &state.encode(),
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
    actions: &[Action],
) -> Result<ObjectBlob, String> {
    let payloads = actions
        .iter()
        .map(|action| encode_action(*action).to_vec())
        .collect::<Vec<_>>();
    let bytes = encode_history_segment(
        ReducerId::Tetris,
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
        reducer: ReducerId::Tetris,
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

fn reduce(mut state: GameState, actions: &[Action]) -> GameState {
    for action in actions {
        state = state.apply(*action);
    }
    state
}

fn divergent_actions(mut state: GameState, count: usize) -> Vec<Action> {
    let cycle = [Action::Right, Action::Tick, Action::Drop, Action::Rotate];
    (0..count)
        .map(|index| {
            let action = if state.game_over {
                Action::Reset
            } else {
                cycle[index % cycle.len()]
            };
            state = state.apply(action);
            action
        })
        .collect()
}
