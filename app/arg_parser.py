import argparse
import sys
from typing import Dict, Any


def validate_arguments(args: argparse.Namespace) -> None:
    if args.county and not args.state:
        print("ERROR: If you specify a county (--county), you must also specify a state (--state)")
        print("Example: --county 'Travis' --state 'Texas'")
        sys.exit(1)
    
    if args.months < 1 or args.months > 12:
        print(f"ERROR: Months must be between 1 and 12, got: {args.months}")
        sys.exit(1)
    
    if args.years < 1 or args.years > 3:
        print(f"ERROR: Years must be between 1 and 3, got: {args.years}")
        sys.exit(1)


def parse_arguments() -> Dict[str, Any]:
    parser = argparse.ArgumentParser(description="Build flood dataset with optional filtering")
    parser.add_argument("--county", type=str, default=None, help="Name of county to filter")
    parser.add_argument("--state", type=str, default=None, help="Name of state to filter")
    parser.add_argument("--months", type=int, default=12, help="Number of months per year to process (1-12)")
    parser.add_argument("--years", type=int, default=3, help="Number of years to process (1-3)")
    
    args = parser.parse_args()
    validate_arguments(args)
    
    return {
        'county': args.county,
        'state': args.state,
        'months': args.months,
        'years': args.years
    }


def print_filter_settings(target_county: str, target_state: str, month_limit: int, years: int) -> None:
    print("=== Filter Settings ===")
    if target_county:
        print(f"Target County: {target_county}")
    else:
        print("Target County: None (no filter)")
        
    if target_state:
        print(f"Target State: {target_state}")
    else:
        print("Target State: None (no filter)")
        
    print(f"Month Limit: {month_limit} months per year")
    print(f"Years to Process: {years} years")
    print("=====================")
