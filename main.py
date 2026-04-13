"""
main.py
=======
Entry point for the SQL Data Analyst Agent.

Usage:
    python main.py --question "What are the top 5 products by total revenue?"
    python main.py --question "What is the average salary by department?"
    python main.py --question "Which region had the most orders in 2024?"
    python main.py --demo    # Runs 3 built-in demo questions

Prerequisites:
    1. Copy .env.example to .env and fill in your GROQ_API_KEY.
    2. python databases/seed_data.py   (run once to create the DBs)
    3. pip install -r requirements.txt
"""

import argparse
import os
import sys
from colorama import Fore, Style, init as colorama_init

# Ensure imports work regardless of where main.py is invoked from
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agent import SQLAnalystAgent

colorama_init(autoreset=True)

DEMO_QUESTIONS = [
    "What are the top 5 products by total revenue?",
    "What is the average salary by department?",
    "Which region had the highest number of completed orders?",
]

LOGO = f"""{Fore.CYAN}
  ___  ___  _      ___  ___  ___  _     ___  ___  ___
 / __|| _ \\| |    /   \\|   \\  _ \\| |   | __|/ __||_ _|
 \\__ \\|  _/| |__ |  O  | |) |   /| |_  | _|| (_ | | |
 |___/|_|  |____| \\___/|___/ |_|\\|____||___|\\___||___|

           SQL Data Analyst Agent  —  Project 3
 ─────────────────────────────────────────────────────
  Concepts: Dynamic Tools · Schema-Aware Prompting
            Iterative Refinement · Semantic Caching
            Multi-DB Routing · NL2SQL Self-Correction
 ─────────────────────────────────────────────────────
{Style.RESET_ALL}"""


def check_prerequisites() -> bool:
    """Validates that .env and the databases exist before running."""
    base = os.path.dirname(os.path.abspath(__file__))

    # Check .env
    if not os.path.exists(os.path.join(base, ".env")):
        print(Fore.RED + "❌  .env file not found.")
        print(Fore.YELLOW + "    Copy .env.example to .env and add your GROQ_API_KEY.")
        return False

    # Check API key
    from dotenv import load_dotenv
    load_dotenv()
    if not os.getenv("GROQ_API_KEY"):
        print(Fore.RED + "❌  GROQ_API_KEY is not set in your .env file.")
        return False

    # Check databases
    sales_db = os.path.join(base, "databases", "sales.db")
    hr_db    = os.path.join(base, "databases", "hr.db")
    if not os.path.exists(sales_db) or not os.path.exists(hr_db):
        print(Fore.RED + "❌  Databases not found.")
        print(Fore.YELLOW + "    Run:  python databases/seed_data.py")
        return False

    return True


def run_question(question: str) -> None:
    """Runs the agent on a single question and prints the report path."""
    print(LOGO)
    print(Fore.WHITE + f"  Question: {Fore.YELLOW}{question}{Style.RESET_ALL}\n")

    if not check_prerequisites():
        sys.exit(1)

    agent       = SQLAnalystAgent()
    report_path = agent.run(question)

    print(f"\n{Fore.GREEN}{'═'*55}")
    print(f"{Fore.GREEN}  ✅  Done! Report saved to:")
    print(f"{Fore.WHITE}      {report_path}")
    print(f"{Fore.GREEN}{'═'*55}{Style.RESET_ALL}\n")

    # Print cache stats at the end
    stats = agent.cache.stats()
    print(Fore.MAGENTA + f"  📦 Cache: {stats['total_entries']} entries | "
          f"{stats['total_hits']} total hits | "
          f"threshold={stats['threshold']}")
    print()


def run_demo() -> None:
    """Runs all three demo questions sequentially to showcase all concepts."""
    print(LOGO)
    print(Fore.CYAN + "  Running DEMO MODE — 3 questions across both databases\n")

    if not check_prerequisites():
        sys.exit(1)

    agent = SQLAnalystAgent()

    for i, question in enumerate(DEMO_QUESTIONS, start=1):
        print(f"\n{Fore.WHITE}{'═'*55}")
        print(f"{Fore.WHITE}  Demo Question {i}/{len(DEMO_QUESTIONS)}:")
        print(f"  {Fore.YELLOW}{question}{Style.RESET_ALL}")
        print(f"{Fore.WHITE}{'═'*55}\n")

        report_path = agent.run(question)

        print(f"\n{Fore.GREEN}  ✅  Report: {report_path}{Style.RESET_ALL}")

    # Final cache stats (should show hits if any questions were similar)
    stats = agent.cache.stats()
    print(f"\n{Fore.MAGENTA}{'─'*55}")
    print(f"  📦 Final Cache Stats:")
    print(f"     Entries: {stats['total_entries']}")
    print(f"     Hits:    {stats['total_hits']}")
    print(f"     Threshold: {stats['threshold']}")
    print(f"{Fore.MAGENTA}{'─'*55}{Style.RESET_ALL}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SQL Data Analyst Agent — Ask questions in natural language.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --question "What are the top 5 products by total revenue?"
  python main.py --question "What is the average salary by department?"
  python main.py --question "Which region had the most orders in 2024?"
  python main.py --demo
        """,
    )
    parser.add_argument(
        "--question", "-q",
        type=str,
        help="Natural language question to analyze",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run 3 built-in demo questions",
    )
    parser.add_argument(
        "--cache-stats",
        action="store_true",
        help="Print semantic cache statistics and exit",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the semantic cache and exit",
    )

    args = parser.parse_args()

    if args.cache_stats:
        from semantic_cache import SemanticCache
        cache = SemanticCache()
        stats = cache.stats()
        print(Fore.MAGENTA + f"\n📦 Semantic Cache Stats:")
        for k, v in stats.items():
            print(f"   {k}: {v}")
        print()
        return

    if args.clear_cache:
        from semantic_cache import SemanticCache
        SemanticCache().clear()
        print(Fore.YELLOW + "🗑️  Semantic cache cleared.\n")
        return

    if args.demo:
        run_demo()
    elif args.question:
        run_question(args.question)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
