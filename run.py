import argparse
import sys
import logging
import importlib
import multiprocessing

# Configure basic logging for the runner script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Target function for multiprocessing
def run_worker_process(component_name):
    """Imports and runs the main function of the specified worker module."""
    try:
        module_path = f"modules.{component_name}.worker"
        logger.info(f"Process {multiprocessing.current_process().pid}: Importing and running {module_path}.main()")
        worker_module = importlib.import_module(module_path)
        if hasattr(worker_module, 'main') and callable(worker_module.main):
            worker_module.main()
        else:
            logger.error(f"Process {multiprocessing.current_process().pid}: Worker module {module_path} lacks a main function.")
    except Exception as e:
        logger.error(f"Process {multiprocessing.current_process().pid}: Error running worker {module_path}: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Run a specific worker component of the CodeSearcher application.")
    parser.add_argument(
        "component",
        choices=["discover_repos", "process_repos"],
        help="The worker component to run."
    )
    # Optional argument to control concurrency for process_repos
    parser.add_argument(
        "-n", "--num-processes",
        type=int,
        default=30,
        help="Number of processes to spawn for 'process_repos' worker (default: 30)"
    )

    args = parser.parse_args()

    component_name = args.component
    num_processes = args.num_processes

    if component_name == "process_repos":
        logger.info(f"Spawning {num_processes} processes for the '{component_name}' worker...")
        processes = []
        for i in range(num_processes):
            process = multiprocessing.Process(target=run_worker_process, args=(component_name,), name=f"RepoProcessor-{i}")
            processes.append(process)
            process.start()
            logger.info(f"Started process {process.pid} (Name: {process.name})")

        logger.info("Waiting for all worker processes to complete...")
        for process in processes:
            process.join() # Wait for each process to finish
            logger.info(f"Process {process.pid} (Name: {process.name}) finished with exit code {process.exitcode}")

        logger.info(f"All {num_processes} '{component_name}' worker processes have completed.")

    else:
        # For other components like discover_repos, run directly in the main process
        logger.info(f"Attempting to start the '{component_name}' worker in the main process...")
        try:
            module_path = f"modules.{component_name}.worker"
            worker_module = importlib.import_module(module_path)
            if hasattr(worker_module, 'main') and callable(worker_module.main):
                worker_module.main()
            else:
                logger.error(f"The worker module '{module_path}' does not have a callable 'main' function.")
                sys.exit(1)
        except ImportError as e:
            logger.error(f"Failed to import the worker module '{module_path}': {e}")
            logger.error("Ensure the component exists and the application structure is correct.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"An unexpected error occurred while running the '{component_name}' worker: {e}", exc_info=True)
            sys.exit(1)

if __name__ == "__main__":
    # Required for multiprocessing on Windows to prevent infinite recursion
    multiprocessing.freeze_support()
    main() 