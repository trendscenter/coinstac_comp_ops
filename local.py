#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: Brad Baker
"""

import ujson as json

# import json
import os
import sys
import copy
import numpy as np
import utils as ut
import mancova_phase_keys as mpk
import gica_phase_keys as gpk
from constants import OUTPUT_TEMPLATE
import os, sys, contextlib


@contextlib.contextmanager
def stdchannel_redirected(stdchannel, dest_filename):
    """
    A context manager to temporarily redirect stdout or stderr
    e.g.:
    with stdchannel_redirected(sys.stderr, os.devnull):
        if compiler.has_function('clock_gettime', libraries=['rt']):
            libraries.append('rt')
    """

    try:
        oldstdchannel = os.dup(stdchannel.fileno())
        dest_file = open(dest_filename, "w")
        os.dup2(dest_file.fileno(), stdchannel.fileno())

        yield
    finally:
        if oldstdchannel is not None:
            os.dup2(oldstdchannel, stdchannel.fileno())
        if dest_file is not None:
            dest_file.close()


LOCAL_MANCOVA_PHASES = mpk.MANCOVA_LOCAL
if __name__ == "__main__":
    parsed_args = json.loads(sys.stdin.read())
    PIPELINE = LOCAL_MANCOVA_PHASES

    phase_key = list(ut.listRecursive(parsed_args, "computation_phase"))
    computation_output = copy.deepcopy(OUTPUT_TEMPLATE)
    if not phase_key:
        ut.log("***************************************", parsed_args["state"])
    ut.log("Starting local phase %s" % phase_key, parsed_args["state"])
    ut.log("With input %s" % str(parsed_args), parsed_args["state"])
    for i, expected_phases in enumerate(PIPELINE):
        ut.log(
            "Expecting phase %s, Got phase %s"
            % (expected_phases.get("recv"), phase_key),
            parsed_args["state"],
        )
        if (
            expected_phases.get("recv") == phase_key
            or expected_phases.get("recv") in phase_key
        ):
            actual_cp = None
            operations = expected_phases.get("do")
            operation_args = expected_phases.get("args")
            operation_kwargs = expected_phases.get("kwargs")
            for operation, args, kwargs in zip(
                operations, operation_args, operation_kwargs
            ):
                if "input" in parsed_args.keys():
                    ut.log(
                        "Operation %s is getting input with keys %s"
                        % (operation.__name__, str(parsed_args["input"].keys())),
                        parsed_args["state"],
                    )
                else:
                    ut.log(
                        "Operation %s is not getting any input!" % operation.__name__,
                        parsed_args["state"],
                    )
                try:
                    ut.log(
                        "Trying operation %s, with args %s, and kwargs %s"
                        % (operation.__name__, str(args), str(kwargs)),
                        parsed_args["state"],
                    )
                    computation_output = operation(parsed_args, *args, **kwargs)
                except NameError as akerr:
                    ut.log("Hit expected error %s" % (str(akerr)), parsed_args["state"])
                    try:
                        ut.log(
                            "Trying operation %s, with kwargs only"
                            % (operation.__name__),
                            parsed_args["state"],
                        )
                        computation_output = operation(parsed_args, **kwargs)
                    except NameError as kerr:
                        ut.log(
                            "Hit expected error %s" % (str(kerr)), parsed_args["state"]
                        )
                        try:
                            ut.log(
                                "Trying operation %s, with args only"
                                % (operation.__name__),
                                parsed_args["state"],
                            )
                            computation_output = operation(parsed_args, *args)
                        except NameError as err:
                            ut.log(
                                "Hit expected error %s" % (str(err)),
                                parsed_args["state"],
                            )
                            ut.log(
                                "Trying operation %s, with no args or kwargs"
                                % (operation.__name__),
                                parsed_args["state"],
                            )
                            computation_output = operation(parsed_args)
                try:
                    parsed_args = copy.deepcopy(computation_output)
                except Exception:
                    parsed_args = computation_output
                ut.log(
                    "Finished with operation %s" % (operation.__name__),
                    parsed_args["state"],
                )
                ut.log(
                    "Operation output has keys %s" % str(parsed_args["output"].keys()),
                    parsed_args["state"],
                )
            if expected_phases.get("send"):
                computation_output["output"]["computation_phase"] = expected_phases.get(
                    "send"
                )
            ut.log(
                "Finished with phase %s" % expected_phases.get("send"),
                parsed_args["state"],
            )
            break
    ut.log(
        "Computation output looks like %s, and output keys %s"
        % (str(computation_output), str(computation_output["output"].keys())),
        parsed_args["state"],
    )
    ut.log(
        "The dump looks like %s" % json.dumps(computation_output), parsed_args["state"]
    )
    sys.stdout.write(json.dumps(computation_output))
