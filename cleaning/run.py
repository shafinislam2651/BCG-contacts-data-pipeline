#!/usr/bin/env python3
"""
Data Pipeline Runner
Runs the complete contact data processing pipeline in sequence:
1. fill_missing_contacts.py - Fill missing data from source files
2. clean_contacts.py - Clean and deduplicate contact data
3. validate_fields.py - Validate field formats and data quality
"""

import subprocess
import sys
import os
import time
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def run_script(script_name, description):
    """Run a Python script and handle errors"""
    script_path = Path(__file__).parent / script_name
    
    if not script_path.exists():
        logging.error(f"❌ Script not found: {script_path}")
        return False
    
    logging.info(f"🚀 Starting: {description}")
    logging.info(f"📄 Running: {script_path}")
    
    start_time = time.time()
    
    try:
        # Run the script using the same Python interpreter
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent  # Run from project root
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Print the output from the script in real-time style
        if result.stdout:
            # Split output into lines and print with indentation
            for line in result.stdout.strip().split('\n'):
                if line.strip():  # Only print non-empty lines
                    print(f"    {line}")
        
        if result.returncode == 0:
            logging.info(f"✅ Completed: {description} (took {duration:.1f}s)")
            return True
        else:
            logging.error(f"❌ Failed: {description}")
            if result.stderr:
                logging.error("Error details:")
                for line in result.stderr.strip().split('\n'):
                    if line.strip():
                        logging.error(f"    {line}")
            return False
            
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        logging.error(f"❌ Exception in {description} (after {duration:.1f}s): {e}")
        return False

def main():
    """Run the complete data processing pipeline"""
    logging.info("=" * 60)
    logging.info("🔄 STARTING DATA PROCESSING PIPELINE")
    logging.info("=" * 60)
    
    pipeline_start = time.time()
    
    # Define the pipeline steps
    steps = [
        ("fill_missing_contacts.py", "Fill missing contact data from source files"),
        ("clean_contacts.py", "Clean and deduplicate contact data"),
        ("validate_fields.py", "Validate field formats and data quality")
    ]
    
    # Run each step in sequence
    for i, (script, description) in enumerate(steps, 1):
        logging.info(f"\n📋 STEP {i}/{len(steps)}: {description}")
        logging.info("-" * 50)
        
        success = run_script(script, description)
        
        if not success:
            logging.error(f"💥 Pipeline failed at step {i}: {description}")
            logging.error("🛑 Stopping pipeline execution")
            sys.exit(1)
        
        # Add a small delay between steps
        if i < len(steps):
            logging.info("⏳ Preparing for next step...")
            time.sleep(2)
    
    # Pipeline completed successfully
    pipeline_end = time.time()
    total_duration = pipeline_end - pipeline_start
    
    logging.info("\n" + "=" * 60)
    logging.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    logging.info(f"⏱️  Total execution time: {total_duration:.1f} seconds")
    logging.info("=" * 60)
    
    # Show output files and summary
    output_dir = Path(__file__).parent.parent / "output"
    if output_dir.exists():
        logging.info("\n📁 Output files created:")
        for file in output_dir.glob("*"):
            if file.is_file():
                size_mb = file.stat().st_size / (1024 * 1024)
                logging.info(f"   📄 {file.name} ({size_mb:.1f} MB)")
        
        # Show validation summary if errors file exists
        validation_file = output_dir / "validation_errors.json"
        if validation_file.exists():
            try:
                import json
                with open(validation_file, 'r') as f:
                    validation_errors = json.load(f)
                
                if validation_errors:
                    logging.info(f"\n📊 Validation Summary:")
                    
                    # Count error types
                    error_counts = {}
                    for error in validation_errors:
                        for err_msg in error['errors']:
                            error_counts[err_msg] = error_counts.get(err_msg, 0) + 1
                    
                    # Show top error types
                    for error_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                        logging.info(f"   • {error_type}: {count} records")
                    
                    if len(error_counts) > 5:
                        logging.info(f"   • ... and {len(error_counts) - 5} other error types")
                        
                    logging.info(f"   📈 Total validation issues: {len(validation_errors)} records")
                else:
                    logging.info(f"\n📊 Validation Summary: ✅ All records passed validation!")
                    
            except Exception as e:
                logging.warning(f"Could not read validation summary: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"\n💥 Unexpected error: {e}")
        sys.exit(1)