"""Build hook for the vendored Sigilo C library."""

# Copyright (c) 2026 Erick Andrade Busato
# SPDX-License-Identifier: MIT

from __future__ import annotations

import importlib.util
import shutil
import sys
import warnings
from pathlib import Path

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py

ROOT = Path(__file__).resolve().parent
# Keep the concrete relative path visible for packaging diagnostics and tests:
# atlas/_c/build_lib.py
BUILD_HELPER_PATH = ROOT / "atlas" / "_c" / "build_lib.py"


def _load_build_helper():
    spec = importlib.util.spec_from_file_location("atlas_c_build_lib", BUILD_HELPER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load build helper from {BUILD_HELPER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CMakeBuildExt(build_ext):
    """Build the shared sigilo library before packaging the Python sources."""

    def run(self) -> None:
        helper = _load_build_helper()
        prefer_cmake = shutil.which("cmake") is not None
        result = helper.build_sigilo_library(repo_root=ROOT, prefer_cmake=prefer_cmake)
        if not result.success:
            warnings.warn(
                (
                    f"[atlas] WARNING: C extension build failed ({result.message}). "
                    "atlas.sigilo will use the Python fallback renderer."
                ),
                RuntimeWarning,
                stacklevel=2,
            )
            return
        if not Path(result.library_path).exists():
            warnings.warn(
                "[atlas] WARNING: libatlas_sigilo was reported as built but no artifact was found.",
                RuntimeWarning,
                stacklevel=2,
            )
            return
        self._copy_artifact_into_build_lib(Path(result.library_path))

    def _copy_artifact_into_build_lib(self, artifact_path: Path) -> None:
        target_dir = Path(self.build_lib) / "atlas"
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(artifact_path, target_dir / artifact_path.name)


class AtlasBuildPy(build_py):
    """Ensure the native sigilo library is present in build/lib before wheel assembly."""

    def run(self) -> None:
        self.run_command("build_ext")
        super().run()


setup(
    ext_modules=[Extension("atlas._sigilo_trigger", sources=[])],
    cmdclass={"build_ext": CMakeBuildExt, "build_py": AtlasBuildPy},
)
