from __future__ import annotations

import argparse
import os
from multiprocessing import cpu_count

from matchengine.internals.engine import MatchEngine
from matchengine.internals.load import load
import logging
from datetime import date
import shutil


def run_match(run_args):
    """
    Main function which triggers run of engine with args passed in from command line.
    """

    if run_args.age_comparison_date:
        age_comparison_date = date.fromisoformat(run_args.age_comparison_date)
    else:
        age_comparison_date = date.today()
    with MatchEngine(
        plugin_dir=run_args.plugin_dir,
        sample_ids=run_args.samples,
        protocol_nos=run_args.trials,
        match_on_closed=run_args.match_on_closed,
        match_on_deceased=run_args.match_on_deceased,
        num_workers=run_args.workers[0],
        config=run_args.config_path,
        db_name=run_args.db_name,
        ignore_run_log=run_args.ignore_run_log,
        skip_run_log_entry=run_args.skip_run_log_entry,
        trial_match_collection=run_args.trial_match_collection,
        drop=run_args.drop or run_args.drop_and_exit,
        drop_accept=run_args.confirm_drop,
        exit_after_drop=run_args.drop_and_exit,
        age_comparison_date=age_comparison_date,
    ) as me:
        me.get_matches_for_all_trials()
        if not run_args.dry:
            me.update_all_matches(dry_run=run_args.debug_dry)

        if run_args.csv_output:
            me.create_output_csv()


class CustomHelpFormatter(argparse.HelpFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Backport proper width calculation from  python 3.8,
        # slightly increasing max help position
        width = shutil.get_terminal_size().columns
        width -= 2
        self._width = min(width, 120)
        self._max_help_position = min(24, max(self._width - 20, self._indent_increment * 2))

    def _format_args(self, action, default_metavar):
        get_metavar = self._metavar_formatter(action, default_metavar)
        if action.nargs is argparse.ZERO_OR_MORE:
            return '[%s ...]' % get_metavar(1)
        else:
            return super()._format_args(action, default_metavar)

    def _format_action_invocation(self, action):
        if not action.option_strings:
            default = self._get_default_metavar_for_positional(action)
            (metavar,) = self._metavar_formatter(action, default)(1)
            return metavar
        if action.nargs == 0:
            return ', '.join(action.option_strings)
        else:
            default = self._get_default_metavar_for_optional(action)
            args_string = self._format_args(action, default)
            return ', '.join(action.option_strings) + ' ' + args_string

def run_cli():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=None)
    log_level_arg = dict(
        dest='log_level',
        default='DEBUG',
        metavar='LEVEL',
        help='Log level',
    )
    base_dir = os.path.dirname(__file__)
    subp = parser.add_subparsers(help='sub-command help')

    subp_p = subp.add_parser(
        'load',
        help='Populate MongoDB with data for testing purposes.',
        formatter_class=CustomHelpFormatter,
        usage='%(prog)s [OPTION]...',
        description=(
            "Populate MongoDB with data for testing purposes. "
            "All paths can be either a single file containing one or more records or "
            "a directory that will be scanned recursively for such files. "
            "Data can be in CSV, JSON, or YAML format."
        ),
    )
    subp_p.add_argument('--log-level', **log_level_arg)
    subp_p.add_argument('--drop', dest='drop', action='store_true', help='Drop existing data')
    subp_p.add_argument(
        '-t',
        '--trial',
        dest='trial',
        metavar='PATH',
        default=None,
        help=('Path to trial data to load'),
    )
    subp_p.add_argument(
        '-c',
        '--clinical',
        dest='clinical',
        metavar='PATH',
        default=None,
        help=('Path to clinical data to load'),
    )
    subp_p.add_argument(
        '-g',
        '--genomic',
        dest='genomic',
        metavar='PATH',
        default=None,
        help=('Path to genomic data to load'),
    )
    subp_p.add_argument(
        '-p',
        '--prior-treatment',
        dest='prior_treatment',
        metavar='PATH',
        default=None,
        help=('Path to prior treatment data to load'),
    )
    subp_p.add_argument(
        '--db',
        dest='db_name',
        metavar='DB',
        default='',
        required=False,
        help=(
            "Specify a custom db name to load trials and/or patient data into. If no value is passed, "
            "db name will be take from SECRETS_JSON file."
        ),
    )
    subp_p.set_defaults(func=load)

    subp_p = subp.add_parser(
        'match',
        help='Match patients to trials.',
        usage='%(prog)s [OPTION]...',
        formatter_class=CustomHelpFormatter,
    )
    group = subp_p.add_argument_group("matching options")
    group.add_argument(
        "--trials",
        nargs="*",
        type=str,
        default=None,
        metavar=('ID',),
        help='Specific trials to match; defaults to all trials.',
    )
    group.add_argument(
        "--samples",
        nargs="*",
        type=str,
        default=None,
        metavar='ID',
        help='Specific samples to match on; defaults to all samples.',
    )
    group.add_argument(
        "--match-on-closed",
        dest="match_on_closed",
        action="store_true",
        help='Match on all closed trials and all suspended steps, arms and doses. Default is to skip.',
    )
    group.add_argument(
        "--match-on-deceased",
        dest="match_on_deceased",
        action="store_true",
        help='Match on deceased patients. Default is to match only on alive patients.',
    )
    group.add_argument(
        '--today',
        dest='age_comparison_date',
        metavar='YYYY-MM-DD',
        action='store',
        help='Date used to calculate ages for trial matching',
    )

    group = subp_p.add_argument_group("configuration")
    group.add_argument(
        "--config",
        dest="config_path",
        metavar='PATH',
        default=os.path.join(base_dir, "defaults", "config.json"),
        help=("Path to the config file; defaults to %(default)a"),
    )
    group.add_argument(
        "--plugins",
        dest="plugin_dir",
        metavar="DIR",
        default=os.path.join(base_dir, "defaults", "plugins"),
        help="Location of plugin directory; defaults to %(default)a",
    )
    group.add_argument(
        '--db',
        dest='db_name',
        default=None,
        required=False,
        metavar='DB',
        help=(
            "Specify a custom db name to load trials and/or patient data into. If no value is passed, "
            "db name will be take from SECRETS_JSON file."
        ),
    )

    group = subp_p.add_argument_group("run parameters")
    group.add_argument(
        "--force",
        dest="ignore_run_log",
        action="store_true",
        help="Ignore the run log and run on all specified sample IDs/protocol nos.",
    )
    group.add_argument(
        "--skip-run-log-entry",
        dest="skip_run_log_entry",
        action="store_true",
        default=False,
        help="Skip creating any run log entries for this run.",
    )
    group.add_argument(
        "--dry-run",
        dest="dry",
        action="store_true",
        help="Execute a full matching run but do not insert any matches into the DB",
    )
    group.add_argument(
        "--debug-dry",
        dest="debug_dry",
        action="store_true",
        help="View planned changes to database (for debugging only)",
    )
    group.add_argument(
        "--drop",
        dest="drop",
        action="store_true",
        default=False,
        help="Drop trials and samples from args supplier",
    )
    group.add_argument(
        "--drop-and-exit",
        dest="drop_and_exit",
        action="store_true",
        default=False,
        help="Like --drop, but exits directly after",
    )
    group.add_argument(
        "--drop-confirm",
        dest="confirm_drop",
        action="store_true",
        default=False,
        help="Confirm you wish --drop; skips confirmation prompt",
    )
    group.add_argument(
        "--collection",
        dest="trial_match_collection",
        metavar='COLL',
        default="trial_match",
        help="Collection to store trial matches in; defaults to %(default)a",
    )

    group = subp_p.add_argument_group("internals")
    group.add_argument('--log-level', **log_level_arg)
    group.add_argument(
        "--workers", metavar='N', nargs=1, type=int, default=[cpu_count() * 5], help="Number of async workers to use"
    )

    group = subp_p.add_argument_group("outputs")

    group.add_argument(
        '--csv',
        dest="csv_output",
        action="store_true",
        default=False,
        required=False,
        help='Export a csv file of all trial match results',
    )

    subp_p.set_defaults(func=run_match)

    args = parser.parse_args()
    if not args.func:
        parser.error("No command specified")

    logging.basicConfig(
        format='%(asctime)s,%(msecs)03d [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S',
        level=args.log_level.upper(),
    )
    logging.captureWarnings(True)

    args.func(args)
