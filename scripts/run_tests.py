import sys
import subprocess
import json
import random
from concurrent.futures import ProcessPoolExecutor, as_completed

def get_test_executable():
    res = subprocess.run(
        ["cargo", "test", "--test", "randomized", "--no-run", "--message-format=json"],
        capture_output=True,
        text=True,
        check=True
    )
    for line in res.stdout.splitlines():
        try:
            data = json.loads(line)
            if data.get("reason") == "compiler-artifact":
                target = data.get("target", {})
                if target.get("name") == "randomized":
                    executable = data.get("executable")
                    if executable:
                        return executable
        except Exception:
            pass
    return None

def run_single_test(executable, seed):
    res = subprocess.run(
        [executable, "randomized_protocol_chaos_test", "--nocapture"],
        env={"SEED": str(seed), "RUST_BACKTRACE": "1"},
        capture_output=True,
        text=True
    )
    if res.returncode != 0:
        return False, seed, res.stdout + res.stderr
    return True, seed, ""

def main():
    if len(sys.argv) > 1:
        try:
            num_tests = int(sys.argv[1])
        except ValueError:
            print(f"Error: Invalid number of tests: {sys.argv[1]}", file=sys.stderr)
            sys.exit(1)
    else:
        num_tests = 100

    print(f"Building/locating randomized test executable...")
    executable = get_test_executable()
    if not executable:
        print("Error: Could not find randomized test executable.", file=sys.stderr)
        sys.exit(1)

    print(f"Running {num_tests} randomized tests in parallel using {executable}...")
    
    seeds = [random.randint(0, 2**63 - 1) for _ in range(num_tests)]
    failed_runs = []
    
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(run_single_test, executable, seed): seed for seed in seeds}
        
        completed = 0
        for future in as_completed(futures):
            success, seed, output = future.result()
            completed += 1
            if not success:
                failed_runs.append((seed, output))
                print(f"[{completed}/{num_tests}] Seed {seed} FAILED!")
            else:
                if completed % 100 == 0 or completed == num_tests:
                    print(f"[{completed}/{num_tests}] completed...")

    print("\n=== TEST RESULTS ===")
    print(f"Total runs: {num_tests}")
    print(f"Successes:  {num_tests - len(failed_runs)}")
    print(f"Failures:   {len(failed_runs)}")

    if failed_runs:
        print("\n=== FAILED SEEDS ===")
        for seed, output in failed_runs:
            print(f"\nSeed: {seed}")
            print("-" * 40)
            print(output)
            print("-" * 40)
        sys.exit(1)
    else:
        print("\nAll tests passed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
