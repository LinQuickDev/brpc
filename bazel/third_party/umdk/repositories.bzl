# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

_UMDK_REMOTE = "https://atomgit.com/openeuler/umdk.git"
_UMDK_COMMIT = "564ee727a55523d4351a8fb3c94292b388ebb924"  # v26.06.0_CAM

_BUILD_FILE = """
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma_headers",
    hdrs = glob([
        "src/urma/lib/urma/bond/include/*.h",
        "src/urma/lib/urma/core/include/*.h",
    ]),
    includes = [
        "src/urma/lib/urma/bond/include",
        "src/urma/lib/urma/core/include",
    ],
)
"""

def _is_false(value):
    return value.lower() in ["0", "false", "no", "off"]

def _first_existing_file(ctx, root, relpaths):
    for relpath in relpaths:
        path = ctx.path(root).get_child(relpath)
        if path.exists:
            return relpath
    return None

def _write_build_file(ctx):
    ctx.file("BUILD.bazel", _BUILD_FILE)

def _use_local_umdk(ctx, urma_root):
    core_include = _first_existing_file(ctx, urma_root, [
        "urma_api.h",
        "ub/umdk/urma/urma_api.h",
        "umdk/urma/urma_api.h",
        "urma/urma_api.h",
        "src/urma/lib/urma/core/include/urma_api.h",
    ])
    if not core_include:
        fail("URMA_ROOT is set to '%s', but no urma_api.h was found under it" %
             urma_root)

    core_include = core_include[:-len("/urma_api.h")] if "/" in core_include else "."
    bond_include = _first_existing_file(ctx, urma_root, [
        "urma_ubagg.h",
        "ub/umdk/urma/urma_ubagg.h",
        "umdk/urma/urma_ubagg.h",
        "urma/urma_ubagg.h",
        "src/urma/lib/urma/bond/include/urma_ubagg.h",
    ])
    if bond_include:
        bond_include = bond_include[:-len("/urma_ubagg.h")] if "/" in bond_include else "."

    ctx.symlink(
        ctx.path(urma_root).get_child(core_include),
        "src/urma/lib/urma/core/include",
    )
    if bond_include:
        ctx.symlink(
            ctx.path(urma_root).get_child(bond_include),
            "src/urma/lib/urma/bond/include",
        )
    else:
        ctx.file("src/urma/lib/urma/bond/include/.keep", "")
    _write_build_file(ctx)

def _run(ctx, args):
    result = ctx.execute(args, quiet = False)
    if result.return_code != 0:
        fail("Failed to run '%s'\nstdout:\n%s\nstderr:\n%s" %
             (" ".join(args), result.stdout, result.stderr))

def _download_umdk(ctx):
    checkout = "umdk_checkout"
    _run(ctx, ["git", "init", checkout])
    _run(ctx, ["git", "-C", checkout, "remote", "add", "origin", _UMDK_REMOTE])
    _run(ctx, ["git", "-C", checkout, "fetch", "--depth", "1", "origin", _UMDK_COMMIT])
    _run(ctx, ["git", "-C", checkout, "checkout", "--detach", "FETCH_HEAD"])
    ctx.symlink(
        ctx.path(checkout).get_child("src/urma/lib/urma/core/include"),
        "src/urma/lib/urma/core/include",
    )
    ctx.symlink(
        ctx.path(checkout).get_child("src/urma/lib/urma/bond/include"),
        "src/urma/lib/urma/bond/include",
    )
    _write_build_file(ctx)

def _umdk_repository_impl(ctx):
    urma_root = ctx.os.environ.get("URMA_ROOT", "")
    if urma_root:
        _use_local_umdk(ctx, urma_root)
        return

    download = ctx.os.environ.get("BRPC_DOWNLOAD_URMA_HEADERS", "true")
    if _is_false(download):
        fail("Failed to find urma_api.h. Set URMA_ROOT to an installed UMDK " +
             "tree or allow downloading with --repo_env=BRPC_DOWNLOAD_URMA_HEADERS=true.")

    _download_umdk(ctx)

umdk_repository = repository_rule(
    implementation = _umdk_repository_impl,
    environ = [
        "BRPC_DOWNLOAD_URMA_HEADERS",
        "URMA_ROOT",
    ],
)
