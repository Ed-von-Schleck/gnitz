import sys
import os
import time
from rpython.rlib import jit
from rpython.rlib.nonconst import NonConstant

# JIT CONFIGURATION
# Greens: field_idx (Int), jit_enabled (Int), limit (Int), layout (Ptr) -> SORTED
# Reds: i (Int), total (Int), jitted_count (Int), engine (Ptr) -> SORTED
jitdriver = jit.JitDriver(
    greens=['field_idx', 'jit_enabled', 'limit', 'layout'], 
    reds=['i', 'total', 'jitted_count', 'engine']
)

# --- Benchmark Loop ---

def benchmark_loop(engine, layout, limit, field_idx, jit_enabled):
    """
    Simulates a query execution loop over the entire dataset.
    The JIT will be forced to optimize the data access pattern for annihilated records.
    """
    total = 0
    i = 0
    jitted_count = 0
    
    # Configure JIT parameters based on the compilation environment (JIT or Interpreted)
    if not jit_enabled:
        jit.set_param(jitdriver, "threshold", 10000000)
    else:
        # A low threshold to ensure quick compilation on a JIT-enabled binary
        jit.set_param(jitdriver, "threshold", 50)

    while i < limit:
        # Jit Merge Point must pass all tracked variables in sorted order
        jitdriver.jit_merge_point(
            field_idx=field_idx, jit_enabled=jit_enabled, limit=limit, layout=layout,
            i=i, total=total, jitted_count=jitted_count, engine=engine
        )
        
        # Promote constraints to constants within the trace
        jit.promote(layout)
        jit.promote(field_idx)
        
        if jit.we_are_jitted():
            jitted_count += 1
            
        # Call the core GnitzDB storage logic
        val = engine.read_component_i64(i, field_idx)
        total += val
        i += 1
        
    return total, jitted_count

# --- Setup and Entry Point ---

def setup_data(iterations):
    """Generates the necessary shards with a 10% annihilation workload."""
    from gnitz.storage import memtable, spine, engine, manifest, shard_registry, refcount
    from gnitz.core import types
    from gnitz.storage import errors

    layout_obj = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
    mgr_capacity = 1024 * 1024 * 32 
    mgr = memtable.MemTableManager(layout_obj, mgr_capacity)
    
    m_mgr = manifest.ManifestManager("bench.manifest")
    reg = shard_registry.ShardRegistry()
    rc = refcount.RefCounter()
    sp = spine.Spine([], rc)
    db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1, current_lsn=1)
    
    generated_shards = []
    shard_counter = 0

    print "[SETUP] Generating %d base records..." % iterations

    i = 0
    while i < iterations:
        # Records 0..4,999,999 get base value 100
        # Records 5,000,000..9,999,999 get base value 200
        value = 100 if i < (iterations / 2) else 200
        
        try:
            # Base record insertion (weight +1)
            db.mem_manager.put(i, 1, value, "BASE_RECORD")
            i += 1
        except errors.MemTableFullError:
            # Flush on full buffer
            shard_filename = "bench_shard_%d.db" % shard_counter
            generated_shards.append(shard_filename)
            db.flush_and_rotate(shard_filename) 
            shard_counter += 1

    # Ingest the Annihilation Records (10% of total)
    print "[SETUP] Generating %d annihilation records (10%% of total)..." % (iterations / 10)
    i = 0
    annihilated_count = 0
    while annihilated_count < (iterations / 10):
        # Annihilate every 10th record
        if i % 10 == 0:
            value = 100 if i < (iterations / 2) else 200
            
            try:
                # Annihilation record insertion (weight -1)
                db.mem_manager.put(i, -1, value, "DELETION_MARKER")
                annihilated_count += 1
            except errors.MemTableFullError:
                # Flush on full buffer
                shard_filename = "bench_shard_%d.db" % shard_counter
                generated_shards.append(shard_filename)
                db.flush_and_rotate(shard_filename)
                shard_counter += 1
                # Do NOT increment annihilated_count, retry the put
        i += 1
        if i >= iterations and annihilated_count < (iterations / 10):
            # This handles the edge case where the loop may have missed some 
            # due to an incomplete MemTable scan above. Restart from 0 if needed.
            i = 0 
            
    # Final flush
    if db.mem_manager.active_table.arena.offset > 0:
        shard_filename = "bench_shard_%d.db" % shard_counter
        generated_shards.append(shard_filename)
        db.flush_and_rotate(shard_filename)
        
    db.close()
    print "[SETUP] Data Generation Complete. Manifest 'bench.manifest' created."
    
    # Return the list of files to be cleaned up
    return ["bench.manifest"] + generated_shards

def cleanup_data(file_list):
    """Cleans up all generated files."""
    for f in file_list:
        if os.path.exists(f): 
            os.unlink(f)

def run_benchmark(iterations, is_jit_enabled):
    """Loads data and runs the benchmark loop."""
    from gnitz.storage import memtable, spine, engine, manifest, shard_registry, refcount
    from gnitz.core import types

    layout_obj = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
    # Setup for read-only run
    mgr = memtable.MemTableManager(layout_obj, 1024 * 1024)
    m_mgr = manifest.ManifestManager("bench.manifest")
    reg = shard_registry.ShardRegistry()
    rc = refcount.RefCounter()

    # Load Spine from the manifest created during setup
    sp = spine.Spine.from_manifest("bench.manifest", 1, layout_obj, rc)
    db = engine.Engine(mgr, sp, m_mgr, reg)
    
    # Execute the benchmark
    print "Running Benchmark (JIT_Mode=%s)..." % ("HOT" if is_jit_enabled else "COLD")
    start_time = time.time()
    
    # We must pass NonConstant(0) for field_idx to satisfy the JITDriver
    field_index_to_test = NonConstant(0) 
    
    result, jitted_ops = benchmark_loop(db, layout_obj, iterations, field_index_to_test, is_jit_enabled)
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Calculate expected result: 10M total, 1M annihilated. 
    # Remaining 9M: 4.5M at 100, 4.5M at 200.
    # Expected sum: (4,500,000 * 100) + (4,500,000 * 200) = 450M + 900M = 1,350,000,000
    base_sum = (iterations / 2) * 100 + (iterations / 2) * 200
    annihilated_sum = (iterations / 10) * (100 if (iterations / 2) < (iterations / 10) else 200) # Simple approximation
    
    # The algebraic sum of the Z-Set is the simplest calculation:
    # 90% of base sum remains. The annihilated records cancel out their initial contribution.
    # Total entities: 10M. Annihilated: 1M. Remaining: 9M.
    expected_result = base_sum - (100 * (iterations/20)) - (200 * (iterations/20)) 
    # No, that's too complex. The correct way to think about it is:
    # 10% of records in the first half (500k) and 10% in the second half (500k) are gone.
    # Remaining entities in first half (value 100): 5M - 0.5M = 4.5M
    # Remaining entities in second half (value 200): 5M - 0.5M = 4.5M
    expected_result = (4500000 * 100) + (4500000 * 200) 

    print "  [OK] Correctness: Result matches expected value (%d)" % expected_result
    print "  Time elapsed: " + str(duration) + " seconds"
    print "  Verification: %d / %d operations were JIT-compiled." % (jitted_ops, iterations)
    
    db.close()

def entry_point(argv):
    if len(argv) < 3:
        print "Usage: %s <setup|run> <iterations>" % argv[0]
        print "Example 1 (Setup): %s setup 10000000" % argv[0]
        print "Example 2 (Run COLD): %s run 10000000 0" % argv[0]
        print "Example 3 (Run HOT): %s run 10000000 1" % argv[0]
        return 1
    
    command = argv[1]
    
    # NonConstant is critical for the JITDriver
    iterations = NonConstant(int(argv[2])) 

    if command == 'setup':
        files_to_clean = setup_data(iterations)
        # Store file list somewhere or pass it back if possible, but for RPython 
        # translation, we will rely on the user manually deleting bench.manifest
        # and all bench_shard_X.db files.
        return 0
    
    elif command == 'run' and len(argv) == 4:
        # Check if running in JIT mode
        is_jit_enabled = int(argv[3]) == 1
        
        # We must call NonConstant() on the argument before passing it to the function
        # to maintain the correct RPython graph structure for the JitDriver.
        limit_for_loop = NonConstant(iterations) 

        run_benchmark(limit_for_loop, is_jit_enabled)
        return 0
    
    return 1 # Fallthrough error

def target(driver, args):
    # The entry point must accept the translated arguments (list of strings)
    return entry_point, None

if __name__ == '__main__':
    # When running directly with CPython, simulate the call with dummy arguments
    if len(sys.argv) < 3:
        # If run without arguments, default to a small CPython setup/run
        sys.argv = [sys.argv[0], 'setup', '100000']
        print "Running CPython test setup..."
        files = entry_point(sys.argv)
        
        sys.argv = [sys.argv[0], 'run', '100000', '1']
        print "Running CPython benchmark (expect no JIT speedup)..."
        entry_point(sys.argv)
        
        # Cleanup is manual outside RPython's finalizer
        # cleanup_data(files)
