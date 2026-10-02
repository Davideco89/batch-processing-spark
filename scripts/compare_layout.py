"""Historical provenance helper; the old single-archive comparison CLI is retired.

Use compare_legacy.py for distributed seven-dataset legacy reconciliation and
validate_parquet.py for the current eight-dataset oracle/publication contract.
Historical reports remain evidence of their original commands, not new runs.
"""

import argparse


def historical_source_uri(recorded, supplied_uri):
    """Physical relocation must not change the provenance recorded in rows."""
    samples = [row for rows in recorded["samples"].values() for row in rows]
    samples += recorded["rejected_samples"]
    observed = {row["source_file"] for row in samples}
    assert observed == {supplied_uri}, (observed, "historical URI metadata")
    return supplied_uri


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.error("The historical single-archive CLI is retired. Use compare_legacy.py for explicit "
                 "read-only legacy seven-dataset reconciliation, then validate_parquet.py for the "
                 "eight published datasets against the extended raw oracle. Legacy SQL exports are not certified.")


if __name__ == "__main__":
    main()
